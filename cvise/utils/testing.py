from __future__ import annotations
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass
import difflib
from enum import auto, Enum, unique
import filecmp
import logging
import math
from multiprocessing import Manager
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from typing import Any, Callable, Dict, List, Mapping, Union
import concurrent.futures

from cvise.cvise import CVise
from cvise.passes.abstract import AbstractPass, PassResult, ProcessEventNotifier, ProcessEventType
from cvise.utils import keyboard_interrupt_monitor
from cvise.utils.error import AbsolutePathTestCaseError
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.error import InvalidInterestingnessTestError
from cvise.utils.error import InvalidTestCaseError
from cvise.utils.error import PassBugError
from cvise.utils.error import ZeroSizeError
from cvise.utils.folding import FoldingManager, FoldingState
from cvise.utils.readkey import KeyLogger
import pebble
import psutil

MAX_PASS_INCREASEMENT_THRESHOLD = 3


@unique
class PassCheckingOutcome(Enum):
    """Outcome of checking the result of an invocation of a pass."""

    ACCEPT = auto()
    IGNORE = auto()
    STOP = auto()


def rmfolder(name):
    assert 'cvise' in str(name)
    try:
        shutil.rmtree(name)
    except OSError:
        pass


@dataclass
class InitEnvironment:
    """Holds data for executing a Pass new() method in a worker."""

    pass_new: Callable
    test_case: Path
    tmp_dir: Path
    job_timeout: int

    def run(self) -> Any:
        try:
            return self.pass_new(self.test_case, tmp_dir=self.tmp_dir, job_timeout=self.job_timeout)
        except UnicodeDecodeError:
            # most likely the pass is incompatible with non-UTF files - abort it
            logging.debug('Skipping pass due to a unicode issue')
            return None


@dataclass
class AdvanceOnSuccessEnvironment:
    """Holds data for executing a Pass advance_on_success() method in a worker."""

    pass_advance_on_success: Callable
    test_case: Path
    pass_previous_state: Any
    pass_succeeded_state: Any
    job_timeout: int

    def run(self) -> Any:
        return self.pass_advance_on_success(
            self.test_case,
            state=self.pass_previous_state,
            succeeded_state=self.pass_succeeded_state,
            job_timeout=self.job_timeout,
        )


class TestEnvironment:
    """Holds data for running a Pass transform() method and the interestingness test in a worker.

    The transform call is optional - in that case, the interestingness test is simply executed for the unchanged input
    (this is useful for implementing the "sanity check" of the input on the C-Vise startup).
    """

    def __init__(
        self,
        state,
        order,
        test_script,
        folder,
        test_case,
        all_test_cases,
        transform,
        pid_queue=None,
    ):
        self.state = state
        self.folder = folder
        self.base_size = None
        self.test_script = test_script
        self.exitcode = None
        self.result = None
        self.order = order
        self.transform = transform
        self.pid_queue = pid_queue
        self.pwd = os.getcwd()
        self.test_case = test_case
        self.base_size = test_case.stat().st_size
        self.all_test_cases = all_test_cases

        # Copy files to the created folder
        for test_case in all_test_cases:
            (self.folder / test_case.parent).mkdir(parents=True, exist_ok=True)
            shutil.copy2(test_case, self.folder / test_case.parent)

    @property
    def size_improvement(self):
        return self.base_size - self.test_case_path.stat().st_size

    @property
    def test_case_path(self):
        return self.folder / self.test_case

    @property
    def success(self):
        return self.result == PassResult.OK and self.exitcode == 0

    def dump(self, dst):
        for f in self.all_test_cases:
            shutil.copy(self.folder / f, dst)

        shutil.copy(self.test_script, dst)

    def run(self):
        try:
            # transform by state
            (result, self.state) = self.transform(
                str(self.test_case_path), self.state, ProcessEventNotifier(self.pid_queue)
            )
            self.result = result
            if self.result != PassResult.OK:
                return self

            # run test script
            self.exitcode = self.run_test(False)
            return self
        except UnicodeDecodeError:
            # most likely the pass is incompatible with non-UTF files - terminate it
            logging.debug('Skipping pass due to a unicode issue')
            self.result = PassResult.STOP
            return self
        except OSError:
            # this can happen when we clean up temporary files for cancelled processes
            return self
        except Exception as e:
            print('Unexpected TestEnvironment::run failure: ' + str(e))
            traceback.print_exc()
            return self

    def run_test(self, verbose):
        try:
            os.chdir(self.folder)
            # Make the job use our custom temp dir instead of the standard one, so that the standard location doesn't
            # get cluttered with files it might leave undeleted (the process might do this because of an oversight in
            # the interestingness test, or because C-Vise abruptly kills our job without a chance for a proper cleanup).
            with tempfile.TemporaryDirectory(dir=self.folder, prefix='overridetmp') as tmp_override:
                env = override_tmpdir_env(os.environ.copy(), Path(tmp_override))
                stdout, stderr, returncode = ProcessEventNotifier(self.pid_queue).run_process(
                    str(self.test_script), shell=True, env=env
                )
            if verbose and returncode != 0:
                # Drop invalid UTF sequences.
                logging.debug('stdout:\n%s', stdout.decode('utf-8', 'ignore'))
                logging.debug('stderr:\n%s', stderr.decode('utf-8', 'ignore'))
        finally:
            os.chdir(self.pwd)
        return returncode


@unique
class PassStage(Enum):
    BEFORE_INIT = auto()
    IN_INIT = auto()
    ENUMERATING = auto()


@dataclass
class PassContext:
    """Stores runtime data for a currently active pass."""

    pass_: AbstractPass
    stage: PassStage
    # Stores pass-specific files to be used during transform jobs (e.g., hints generated during initialization), and
    # temporary folders for each transform job.
    temporary_root: Union[Path, None]
    # The pass state as returned by the pass new()/advance()/advance_on_success() methods.
    state: Any
    # The state that succeeded in the previous batch of jobs - to be passed as succeeded_state to advance_on_success().
    taken_succeeded_state: Any
    # Currently running transform jobs, as the (order, state) mapping.
    running_transform_order_to_state: Dict[int, Any]
    # When True, the pass is considered dysfunctional and shouldn't be used anymore.
    defunct: bool
    # How many times a job for this pass timed out.
    timeout_count: int

    @staticmethod
    def create(pass_: AbstractPass) -> PassContext:
        pass_name = str(pass_).replace('::', '-')
        root = tempfile.mkdtemp(prefix=f'{TestManager.TEMP_PREFIX}{pass_name}-')
        logging.debug(f'Creating pass root folder: {root}')
        return PassContext(
            pass_=pass_,
            stage=PassStage.BEFORE_INIT,
            temporary_root=Path(root),
            state=None,
            taken_succeeded_state=None,
            running_transform_order_to_state={},
            defunct=False,
            timeout_count=0,
        )

    def can_init_now(self) -> bool:
        """Whether the pass new() method can be scheduled."""
        return not self.defunct and self.stage == PassStage.BEFORE_INIT

    def can_transform_now(self) -> bool:
        """Whether the pass transform() method can be scheduled."""
        return not self.defunct and self.stage == PassStage.ENUMERATING and self.state is not None

    def can_start_job_now(self) -> bool:
        """Whether any of the pass methods can be scheduled."""
        return self.can_init_now() or self.can_transform_now()


@unique
class JobType(Enum):
    INIT = auto()
    TRANSFORM = auto()


@dataclass
class Job:
    type: JobType
    future: Future
    order: int

    # If this job executes a method of a pass, these store pointers to it; None otherwise.
    pass_: Union[AbstractPass, None]
    pass_id: Union[int, None]
    pass_name: str

    start_time: float
    temporary_folder: Union[Path, None]


@dataclass
class SuccessCandidate:
    order: int
    pass_: AbstractPass
    pass_id: int
    pass_state: Any
    tmp_dir: Union[Path, None]
    test_case_path: Path

    @staticmethod
    def create_and_take_file(
        order: int, pass_: AbstractPass, pass_id: int, pass_state: Any, test_case_path: Path
    ) -> SuccessCandidate:
        tmp_dir = Path(tempfile.mkdtemp(prefix=f'{TestManager.TEMP_PREFIX}candidate-'))
        new_test_case_path = tmp_dir / test_case_path.name
        shutil.move(test_case_path, new_test_case_path)
        return SuccessCandidate(
            order=order,
            pass_=pass_,
            pass_id=pass_id,
            pass_state=pass_state,
            tmp_dir=tmp_dir,
            test_case_path=new_test_case_path,
        )

    def release(self) -> None:
        if self.tmp_dir is not None:
            rmfolder(self.tmp_dir)
        self.tmp_dir = None


class TestManager:
    GIVEUP_CONSTANT = 50000
    MAX_TIMEOUTS = 20
    MAX_CRASH_DIRS = 10
    MAX_EXTRA_DIRS = 25000
    TEMP_PREFIX = 'cvise-'
    BUG_DIR_PREFIX = 'cvise_bug_'
    EXTRA_DIR_PREFIX = 'cvise_extra_'
    EVENT_LOOP_TIMEOUT = 1  # seconds

    def __init__(
        self,
        pass_statistic,
        test_script,
        timeout,
        save_temps,
        test_cases,
        parallel_tests,
        no_cache,
        skip_key_off,
        silent_pass_bug,
        die_on_pass_bug,
        print_diff,
        max_improvement,
        no_give_up,
        also_interesting,
        start_with_pass,
        skip_after_n_transforms,
        stopping_threshold,
    ):
        self.test_script = Path(test_script).absolute()
        self.timeout = timeout
        self.save_temps = save_temps
        self.pass_statistic = pass_statistic
        self.test_cases = set()
        self.test_cases_modes = {}
        self.parallel_tests = parallel_tests
        self.no_cache = no_cache
        self.skip_key_off = skip_key_off
        self.silent_pass_bug = silent_pass_bug
        self.die_on_pass_bug = die_on_pass_bug
        self.print_diff = print_diff
        self.max_improvement = max_improvement
        self.no_give_up = no_give_up
        self.also_interesting = also_interesting
        self.start_with_pass = start_with_pass
        self.skip_after_n_transforms = skip_after_n_transforms
        self.stopping_threshold = stopping_threshold

        for test_case in test_cases:
            test_case = Path(test_case)
            self.test_cases_modes[test_case] = test_case.stat().st_mode
            self.check_file_permissions(test_case, [os.F_OK, os.R_OK, os.W_OK], InvalidTestCaseError)
            if test_case.parent.is_absolute():
                raise AbsolutePathTestCaseError(test_case)
            self.test_cases.add(test_case)

        self.orig_total_file_size = self.total_file_size
        self.cache = {}
        self.pass_contexts: List[PassContext] = []
        self.interleaving: bool = False
        if not self.is_valid_test(self.test_script):
            raise InvalidInterestingnessTestError(self.test_script)
        self.jobs: List[Job] = []
        self.order: int = 0
        self.success_candidate: Union[SuccessCandidate, None] = None
        self.folding_manager: Union[FoldingManager, None] = None

        self.use_colordiff = (
            sys.stdout.isatty()
            and subprocess.run(
                'colordiff --version',
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            == 0
        )

    def remove_roots(self):
        if self.save_temps:
            return
        for ctx in self.pass_contexts:
            if not ctx.temporary_root:
                continue
            rmfolder(ctx.temporary_root)
            ctx.temporary_root = None
        if self.success_candidate:
            self.success_candidate.release()
            self.success_candidate = None

    def restore_mode(self):
        for test_case in self.test_cases:
            test_case.chmod(self.test_cases_modes[test_case])

    @classmethod
    def is_valid_test(cls, test_script):
        for mode in {os.F_OK, os.X_OK}:
            if not os.access(test_script, mode):
                return False
        return True

    @property
    def total_file_size(self):
        return self.get_file_size(self.test_cases)

    @property
    def sorted_test_cases(self):
        return sorted(self.test_cases, key=lambda x: x.stat().st_size, reverse=True)

    @staticmethod
    def get_file_size(files):
        return sum(f.stat().st_size for f in files)

    @property
    def total_line_count(self):
        return self.get_line_count(self.test_cases)

    @staticmethod
    def get_line_count(files):
        lines = 0
        for file in files:
            with open(file, 'rb') as f:
                lines += len([line for line in f.readlines() if line and not line.isspace()])
        return lines

    def backup_test_cases(self):
        for f in self.test_cases:
            orig_file = Path(f'{f}.orig')

            if not orig_file.exists():
                # Copy file and preserve attributes
                shutil.copy2(f, orig_file)

    @staticmethod
    def check_file_permissions(path, modes, error):
        for m in modes:
            if not os.access(path, m):
                if error is not None:
                    raise error(path, m)
                else:
                    return False

        return True

    @staticmethod
    def get_extra_dir(prefix, max_number):
        for i in range(0, max_number + 1):
            digits = int(round(math.log10(max_number), 0))
            extra_dir = Path(('{0}{1:0' + str(digits) + 'd}').format(prefix, i))

            if not extra_dir.exists():
                break

        # just bail if we've already created enough of these dirs, no need to
        # clutter things up even more...
        if extra_dir.exists():
            return None

        return extra_dir

    def report_pass_bug(self, job: Job, problem: str):
        """Create pass report bug and return True if the directory is created."""

        if not self.die_on_pass_bug:
            logging.warning(f'{job.pass_} has encountered a non fatal bug: {problem}')

        crash_dir = self.get_extra_dir(self.BUG_DIR_PREFIX, self.MAX_CRASH_DIRS)

        if crash_dir is None:
            return False

        crash_dir.mkdir()
        test_env: TestEnvironment = job.future.result()
        test_env.dump(crash_dir)

        if not self.die_on_pass_bug:
            logging.debug(
                f'Please consider tarring up {crash_dir} and creating an issue at https://github.com/marxin/cvise/issues and we will try to fix the bug.'
            )

        with (crash_dir / 'PASS_BUG_INFO.TXT').open(mode='w') as info_file:
            info_file.write(f'Package: {CVise.Info.PACKAGE_STRING}\n')
            info_file.write(f'Git version: {CVise.Info.GIT_VERSION}\n')
            info_file.write(f'LLVM version: {CVise.Info.LLVM_VERSION}\n')
            info_file.write(f'System: {str(platform.uname())}\n')
            info_file.write(PassBugError.MSG.format(job.pass_, problem, test_env.state, crash_dir))

        if self.die_on_pass_bug:
            raise PassBugError(job.pass_, problem, test_env.state, crash_dir)
        else:
            return True

    @staticmethod
    def diff_files(orig_file, changed_file):
        with open(orig_file, 'rb') as f:
            orig_file_lines = f.readlines()

        with open(changed_file, 'rb') as f:
            changed_file_lines = f.readlines()

        diffed_lines = difflib.diff_bytes(
            difflib.unified_diff, orig_file_lines, changed_file_lines, bytes(orig_file), bytes(changed_file)
        )
        # Drop invalid UTF sequences from the diff, to make it easy to log.
        str_lines = [s.decode('utf-8', 'ignore') for s in diffed_lines]

        return ''.join(str_lines)

    def check_sanity(self):
        logging.debug('perform sanity check... ')

        folder = Path(tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}sanity-'))
        test_env = TestEnvironment(None, 0, self.test_script, folder, list(self.test_cases)[0], self.test_cases, None)
        logging.debug(f'sanity check tmpdir = {test_env.folder}')

        returncode = test_env.run_test(verbose=True)
        if returncode == 0:
            rmfolder(folder)
            logging.debug('sanity check successful')
        else:
            if not self.save_temps:
                rmfolder(folder)
            raise InsaneTestCaseError(self.test_cases, self.test_script)

    @classmethod
    def log_key_event(cls, event):
        logging.info(f'****** {event} ******')

    def kill_pid_queue(self):
        active_pids = set()
        while not self.pid_queue.empty():
            event = self.pid_queue.get()
            if event.type == ProcessEventType.FINISHED:
                active_pids.discard(event.pid)
            else:
                active_pids.add(event.pid)
        for pid in active_pids:
            try:
                process = psutil.Process(pid)
                children = process.children(recursive=True)
                children.append(process)
                for child in children:
                    try:
                        # Terminate the process more reliability: https://github.com/marxin/cvise/issues/145
                        child.kill()
                    except psutil.NoSuchProcess:
                        pass
            except psutil.NoSuchProcess:
                pass

    def release_job(self, job: Job) -> None:
        if not self.save_temps and job.temporary_folder is not None:
            rmfolder(job.temporary_folder)
        self.jobs.remove(job)

    def release_all_jobs(self) -> None:
        while self.jobs:
            self.release_job(self.jobs[0])

    def save_extra_dir(self, test_case_path):
        extra_dir = self.get_extra_dir(self.EXTRA_DIR_PREFIX, self.MAX_EXTRA_DIRS)
        if extra_dir is not None:
            os.mkdir(extra_dir)
            shutil.move(test_case_path, extra_dir)
            logging.info(f'Created extra directory {extra_dir} for you to look at later')

    def process_done_futures(self) -> None:
        jobs_to_remove = []
        for job in self.jobs:
            if not job.future.done():
                continue
            jobs_to_remove.append(job)
            if job.future.exception():
                # starting with Python 3.11: concurrent.futures.TimeoutError == TimeoutError
                if type(job.future.exception()) in (TimeoutError, concurrent.futures.TimeoutError):
                    self.handle_timed_out_job(job)
                    continue
                raise job.future.exception()
            if job.type == JobType.INIT:
                self.handle_finished_init_job(job)
            elif job.type == JobType.TRANSFORM:
                self.handle_finished_transform_job(job)
            else:
                raise ValueError(f'Unexpected job type {job.type}')

        for job in jobs_to_remove:
            self.release_job(job)

    def handle_timed_out_job(self, job: Job) -> None:
        logging.warning('Test timed out for %s.', job.pass_name)
        self.save_extra_dir(job.temporary_folder)
        if job.pass_id is None:
            # The logic of disabling a pass after repeated timeouts isn't applicable to folding jobs.
            return
        ctx = self.pass_contexts[job.pass_id]
        ctx.timeout_count += 1
        ctx.running_transform_order_to_state.pop(job.order)
        if ctx.timeout_count < self.MAX_TIMEOUTS or ctx.defunct:
            return
        logging.warning('Maximum number of timeout were reached for %s: %s', job.pass_name, self.MAX_TIMEOUTS)
        ctx.defunct = True

    def handle_finished_init_job(self, job: Job) -> None:
        ctx = self.pass_contexts[job.pass_id]
        assert ctx.stage == PassStage.IN_INIT
        ctx.stage = PassStage.ENUMERATING
        ctx.state = job.future.result()
        self.pass_statistic.add_initialized(job.pass_, job.start_time)

    def handle_finished_transform_job(self, job: Job) -> None:
        self.pass_statistic.add_executed(job.pass_, job.start_time, self.parallel_tests)
        if job.pass_id is not None:
            self.pass_contexts[job.pass_id].running_transform_order_to_state.pop(job.order)

        outcome = self.check_pass_result(job)
        if outcome == PassCheckingOutcome.STOP:
            self.pass_contexts[job.pass_id].state = None
            return
        if outcome == PassCheckingOutcome.IGNORE:
            self.pass_statistic.add_failure(job.pass_)
            return
        assert outcome == PassCheckingOutcome.ACCEPT
        self.pass_statistic.add_success(job.pass_)
        env: TestEnvironment = job.future.result()
        self.maybe_update_success_candidate(job.order, job.pass_, job.pass_id, env)
        if self.interleaving:
            self.folding_manager.on_transform_job_success(env.state)

    def check_pass_result(self, job: Job):
        test_env: TestEnvironment = job.future.result()
        if test_env.success:
            if self.max_improvement is not None and test_env.size_improvement > self.max_improvement:
                logging.debug(f'Too large improvement: {test_env.size_improvement} B')
                return PassCheckingOutcome.IGNORE
            # Report bug if transform did not change the file
            if filecmp.cmp(self.current_test_case, test_env.test_case_path):
                if not self.silent_pass_bug:
                    if not self.report_pass_bug(job, 'pass failed to modify the variant'):
                        return PassCheckingOutcome.STOP
                return PassCheckingOutcome.IGNORE
            return PassCheckingOutcome.ACCEPT

        if test_env.result == PassResult.OK:
            assert test_env.exitcode
            if self.also_interesting is not None and test_env.exitcode == self.also_interesting:
                self.save_extra_dir(test_env.test_case_path)
        elif test_env.result == PassResult.STOP:
            return PassCheckingOutcome.STOP
        elif test_env.result == PassResult.ERROR:
            if not self.silent_pass_bug:
                self.report_pass_bug(job, 'pass error')
                return PassCheckingOutcome.STOP

        if not self.no_give_up and test_env.order > self.GIVEUP_CONSTANT:
            if not self.giveup_reported:
                self.report_pass_bug(job, 'pass got stuck')
                self.giveup_reported = True
            return PassCheckingOutcome.STOP
        return PassCheckingOutcome.IGNORE

    def maybe_update_success_candidate(
        self, order: int, pass_: Union[AbstractPass, None], pass_id: Union[int, None], env: TestEnvironment
    ) -> None:
        assert env.success
        if self.success_candidate and pass_ is not None:
            # For regular passes, prefer the first-seen candidate (it's likely to be larger); this is different from
            # folding jobs, which grow over time (accumulating all regular successes like a snowball).
            return
        if self.success_candidate:
            # Make sure to clean up old temporary files.
            self.success_candidate.release()
        self.success_candidate = SuccessCandidate.create_and_take_file(
            order=order,
            pass_=pass_,
            pass_id=pass_id,
            pass_state=env.state,
            test_case_path=env.test_case_path,
        )

    @classmethod
    def terminate_all(cls, pool):
        pool.stop()
        pool.join()

    def run_parallel_tests(self) -> None:
        assert not self.jobs
        with pebble.ProcessPool(max_workers=self.parallel_tests) as pool:
            try:
                self.order = 1
                self.next_pass_id = 0
                self.giveup_reported = False
                assert self.success_candidate is None
                if self.interleaving:
                    self.folding_manager = FoldingManager()
                for ctx in self.pass_contexts:
                    # Clean up the information about previously running jobs.
                    ctx.running_transform_order_to_state = {}
                    # Unfinished initializations from the last run will need to be restarted.
                    if ctx.stage == PassStage.IN_INIT:
                        ctx.stage = PassStage.BEFORE_INIT
                while self.jobs or any(c.can_start_job_now() for c in self.pass_contexts):
                    keyboard_interrupt_monitor.maybe_reraise()

                    # schedule new jobs, as long as there are free workers
                    while len(self.jobs) < self.parallel_tests and self.maybe_schedule_job(pool):
                        pass

                    # no more jobs could be scheduled at the moment - wait for some results
                    wait([j.future for j in self.jobs], return_when=FIRST_COMPLETED, timeout=self.EVENT_LOOP_TIMEOUT)
                    self.process_done_futures()

                    # exit if we found successful transformation(s) and don't want to try better ones
                    if self.success_candidate and self.should_proceed_with_success_candidate():
                        break

                self.terminate_all(pool)
            except:
                # Abort running jobs - by default the process pool waits for the ongoing jobs' completion.
                self.terminate_all(pool)
                raise

    def run_passes(self, passes: List[AbstractPass], interleaving: bool):
        assert len(passes) == 1 or interleaving

        if self.start_with_pass:
            current_pass_names = [str(c.pass_) for c in self.pass_contexts]
            if self.start_with_pass in current_pass_names:
                self.start_with_pass = None
            else:
                return

        self.pass_contexts = []
        for pass_ in passes:
            self.pass_contexts.append(PassContext.create(pass_))
        self.interleaving = interleaving
        self.jobs = []
        m = Manager()
        self.pid_queue = m.Queue()
        cache_key = repr([c.pass_ for c in self.pass_contexts])

        pass_titles = ', '.join(repr(c.pass_) for c in self.pass_contexts)
        logging.info(f'===< {pass_titles} >===')

        if self.total_file_size == 0:
            raise ZeroSizeError(self.test_cases)

        if not self.skip_key_off:
            logger = KeyLogger()

        try:
            for test_case in self.sorted_test_cases:
                self.current_test_case = test_case
                starting_test_case_size = test_case.stat().st_size
                success_count = 0

                if self.get_file_size([test_case]) == 0:
                    continue

                if not self.no_cache:
                    with open(test_case, mode='rb+') as tmp_file:
                        test_case_before_pass = tmp_file.read()

                        if cache_key in self.cache and test_case_before_pass in self.cache[cache_key]:
                            tmp_file.seek(0)
                            tmp_file.truncate(0)
                            tmp_file.write(self.cache[cache_key][test_case_before_pass])
                            logging.info(f'cache hit for {test_case}')
                            continue

                self.skip = False
                while any(c.can_start_job_now() for c in self.pass_contexts) and not self.skip:
                    # Ignore more key presses after skip has been detected
                    if not self.skip_key_off and not self.skip:
                        key = logger.pressed_key()
                        if key == 's':
                            self.skip = True
                            self.log_key_event('skipping the rest of this pass')
                        elif key == 'd':
                            self.log_key_event('toggle print diff')
                            self.print_diff = not self.print_diff

                    self.run_parallel_tests()
                    self.kill_pid_queue()

                    is_success = self.success_candidate is not None
                    if is_success:
                        self.process_result()
                        success_count += 1

                    # if the file increases significantly, bail out the current pass
                    test_case_size = self.current_test_case.stat().st_size
                    if test_case_size >= MAX_PASS_INCREASEMENT_THRESHOLD * starting_test_case_size:
                        logging.info(
                            f'skipping the rest of the pass (huge file increasement '
                            f'{MAX_PASS_INCREASEMENT_THRESHOLD * 100}%)'
                        )
                        break

                    self.release_all_jobs()
                    if not is_success:
                        break

                    # skip after N transformations if requested
                    skip_rest = self.skip_after_n_transforms and success_count >= self.skip_after_n_transforms
                    if not self.interleaving:  # max-transforms is only supported for non-interleaving passes
                        assert len(self.pass_contexts) == 1
                        if (
                            self.pass_contexts[0].pass_.max_transforms
                            and success_count >= self.pass_contexts[0].pass_.max_transforms
                        ):
                            skip_rest = True
                    if skip_rest:
                        logging.info(f'skipping after {success_count} successful transformations')
                        break

                # Cache result of this pass
                if not self.no_cache:
                    with open(test_case, mode='rb') as tmp_file:
                        if cache_key not in self.cache:
                            self.cache[cache_key] = {}

                        self.cache[cache_key][test_case_before_pass] = tmp_file.read()

            self.restore_mode()
            self.remove_roots()
        except KeyboardInterrupt:
            logging.info('Exiting now ...')
            # Clean temporary files for all jobs and passes.
            self.release_all_jobs()
            self.remove_roots()
            sys.exit(1)

    def process_result(self) -> None:
        assert self.success_candidate
        new_test_case = self.success_candidate.test_case_path
        if self.print_diff:
            diff_str = self.diff_files(self.current_test_case, new_test_case)
            if self.use_colordiff:
                diff_str = subprocess.check_output('colordiff', shell=True, input=diff_str)
            logging.info(diff_str)

        try:
            shutil.copy(new_test_case, self.current_test_case)
        except FileNotFoundError:
            raise RuntimeError(
                f"Can't find {self.current_test_case} -- did your interestingness test move it?"
            ) from None

        for pass_id, ctx in enumerate(self.pass_contexts):
            # If there's an earlier state whose check hasn't completed - rewind to this state.
            rewind_to = (
                min(ctx.running_transform_order_to_state.keys()) if ctx.running_transform_order_to_state else None
            )
            # The only exception is when the earliest job is the one that succeeded - in that case take the state that
            # its transform() returned.
            if self.success_candidate.pass_id == pass_id and (
                rewind_to is None or self.success_candidate.order <= rewind_to
            ):
                ctx.state = self.success_candidate.pass_state
            elif rewind_to is not None:
                ctx.state = ctx.running_transform_order_to_state[rewind_to]
            ctx.running_transform_order_to_state = {}

            # Also explicitly remember the state that succeeded - advance_on_success() expects it as a separate argument.
            ctx.taken_succeeded_state = (
                self.success_candidate.pass_state if pass_id == self.success_candidate.pass_id else None
            )

            # Next round should reinitialize unfinished passes.
            if ctx.stage == PassStage.ENUMERATING and ctx.state is not None:
                ctx.stage = PassStage.BEFORE_INIT

        pct = 100 - (self.total_file_size * 100.0 / self.orig_total_file_size)
        notes = []
        notes.append(f'{round(pct, 1)}%')
        notes.append(f'{self.total_file_size} bytes')
        if self.total_line_count:
            notes.append(f'{self.total_line_count} lines')
        if len(self.test_cases) > 1:
            notes.append(str(new_test_case.name))
        if len(self.pass_contexts) > 1:
            if isinstance(self.success_candidate.pass_state, FoldingState):
                pass_name = ' + '.join(self.success_candidate.pass_state.statistics.get_passes_ordered_by_delta())
            else:
                pass_name = repr(self.success_candidate.pass_)
            notes.append(f'via {pass_name}')

        self.success_candidate.release()
        self.success_candidate = None

        logging.info('(' + ', '.join(notes) + ')')

    def should_proceed_with_success_candidate(self):
        assert self.success_candidate
        if not self.interleaving:
            return True
        return not self.folding_manager.continue_attempting_folds(
            self.order, self.parallel_tests, len(self.pass_contexts)
        )

    def maybe_schedule_job(self, pool: pebble.ProcessPool) -> bool:
        # The order matters below - higher-priority job types come earlier:
        # 1. Initializing a pass.
        for pass_id, ctx in enumerate(self.pass_contexts):
            if ctx.can_init_now():
                self.schedule_init(pool, pass_id)
                return True
        # 2. Attempting a fold (simultaneous application) of previously discovered successful transformations; only
        # supported in the "interleaving" pass execution mode.
        if self.interleaving:
            folding_state = self.folding_manager.maybe_prepare_folding_job(self.order)
            if folding_state:
                self.schedule_fold(pool, folding_state)
                return True
        # 3. Attempting a transformation using the next heuristic in the round-robin fashion.
        if any(ctx.can_transform_now() for ctx in self.pass_contexts):
            while not self.pass_contexts[self.next_pass_id].can_transform_now():
                self.next_pass_id = (self.next_pass_id + 1) % len(self.pass_contexts)
            self.schedule_transform(pool, self.next_pass_id)
            self.next_pass_id = (self.next_pass_id + 1) % len(self.pass_contexts)
            return True
        return False

    def schedule_init(self, pool: pebble.ProcessPool, pass_id: int) -> None:
        ctx = self.pass_contexts[pass_id]
        assert ctx.can_init_now()

        # Either initialize the pass from scratch, or advance from the previous state.
        if ctx.state is None:
            env = InitEnvironment(
                pass_new=ctx.pass_.new,
                test_case=self.current_test_case,
                tmp_dir=ctx.temporary_root,
                job_timeout=self.timeout,
            )
        else:
            env = AdvanceOnSuccessEnvironment(
                pass_advance_on_success=ctx.pass_.advance_on_success,
                test_case=self.current_test_case,
                pass_previous_state=ctx.state,
                pass_succeeded_state=ctx.taken_succeeded_state,
                job_timeout=self.timeout,
            )
        future = pool.schedule(env.run)
        self.jobs.append(
            Job(
                type=JobType.INIT,
                future=future,
                order=self.order,
                pass_=ctx.pass_,
                pass_id=pass_id,
                pass_name=repr(ctx.pass_),
                start_time=time.monotonic(),
                temporary_folder=None,
            )
        )

        ctx.stage = PassStage.IN_INIT
        self.order += 1

    def schedule_transform(self, pool: pebble.ProcessPool, pass_id: int) -> None:
        ctx = self.pass_contexts[pass_id]
        assert ctx.can_transform_now()
        assert ctx.state is not None

        folder = Path(tempfile.mkdtemp(prefix=self.TEMP_PREFIX, dir=ctx.temporary_root))
        env = TestEnvironment(
            ctx.state,
            self.order,
            self.test_script,
            folder,
            self.current_test_case,
            self.test_cases,
            ctx.pass_.transform,
            self.pid_queue,
        )
        future = pool.schedule(env.run, timeout=self.timeout)
        self.jobs.append(
            Job(
                type=JobType.TRANSFORM,
                future=future,
                order=self.order,
                pass_=ctx.pass_,
                pass_id=pass_id,
                pass_name=repr(ctx.pass_),
                start_time=time.monotonic(),
                temporary_folder=folder,
            )
        )
        assert self.order not in ctx.running_transform_order_to_state
        ctx.running_transform_order_to_state[self.order] = ctx.state

        self.order += 1
        ctx.state = ctx.pass_.advance(self.current_test_case, ctx.state)

    def schedule_fold(self, pool: pebble.ProcessPool, folding_state: FoldingState) -> None:
        assert self.interleaving

        folder = Path(tempfile.mkdtemp(prefix=self.TEMP_PREFIX + 'folding-'))
        env = TestEnvironment(
            folding_state,
            self.order,
            self.test_script,
            folder,
            self.current_test_case,
            self.test_cases,
            FoldingManager.transform,
            self.pid_queue,
        )
        future = pool.schedule(env.run, timeout=self.timeout)
        self.jobs.append(
            Job(
                type=JobType.TRANSFORM,
                future=future,
                order=self.order,
                pass_=None,
                pass_id=None,
                pass_name='Folding',
                start_time=time.monotonic(),
                temporary_folder=folder,
            )
        )

        self.order += 1


def override_tmpdir_env(old_env: Mapping, tmp_override: Path) -> Mapping:
    new_env = dict(old_env)
    for var in ('TMPDIR', 'TEMP', 'TMP'):
        new_env[var] = str(tmp_override)
    return new_env
