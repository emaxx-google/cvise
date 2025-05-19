import collections
from concurrent.futures import FIRST_COMPLETED, wait
import copy
import difflib
from enum import auto, Enum, unique
import filecmp
import logging
import math
import multiprocessing
import os
from pathlib import Path
import platform
import queue
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import traceback
import concurrent.futures
import time

from cvise.cvise import CVise
from cvise.passes.abstract import PassResult, ProcessEventNotifier, ProcessEventType
from cvise.utils.error import AbsolutePathTestCaseError
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.error import InvalidInterestingnessTestError
from cvise.utils.error import InvalidTestCaseError
from cvise.utils.error import PassBugError
from cvise.utils.error import ZeroSizeError
from cvise.utils.misc import is_readable_file, DeltaTimeFormatter
from cvise.utils.readkey import KeyLogger

# Hack to use the vendored version of Pebble to guarantee the performance fix is in (>=5.1.1).
vendor_path = Path(__file__).parent.parent / 'vendor'
sys.path.insert(0, str(vendor_path))
os.environ['PYTHONPATH'] = str(vendor_path) + ':' + os.environ.get('PYTHONPATH', '')
import pebble
assert Path(pebble.__file__).is_relative_to(vendor_path), f'vendor_path={vendor_path} pebble.__file__={pebble.__file__}'
sys.path.pop(0)

# change default Pebble sleep unit for faster response
pebble.common.SLEEP_UNIT = 0.01
MAX_PASS_INCREASEMENT_THRESHOLD = 3


def get_file_size(path):
    inner_paths = [f for f in Path(path).rglob('*') if not f.is_dir()] if os.path.isdir(path) else [path]
    return sum(f.resolve().stat().st_size for f in inner_paths)

def get_file_count(path):
    inner_paths = [f for f in Path(path).rglob('*')] if os.path.isdir(path) else [path]
    return len(inner_paths)

def get_line_count(path):
    lines = 0
    inner_files = [f for f in Path(path).rglob('*') if not f.is_dir()] if os.path.isdir(path) else [path]
    for file in inner_files:
        if is_readable_file(file):
            with open(file) as f:
                lines += len([line for line in f.readlines() if line and not line.isspace()])
    return lines

@unique
class PassCheckingOutcome(Enum):
    """Outcome of checking the result of an invocation of a pass."""

    ACCEPT = auto()
    IGNORE = auto()
    QUIT_LOOP = auto()

@unique
class JobType(Enum):
    RMFOLDER = auto()
    PASS_NEW = auto()
    PASS_TRANSFORM = auto()


def rmtree(name):
    # Not using shutil.rmtree() as it's much slower when there are many
    # files - it calls stat on every file.
    for root, dirs, files in os.walk(name, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))
    os.rmdir(name)

def rmfolder(name):
    assert 'cvise' in str(name)
    try:
        rmtree(name)
    except OSError as e:
        pass

def worker_initializer(start_time, logging_level):
    os.setpgrp()

    logging.getLogger().setLevel(logging_level)
    log_format = '%(delta)s %(levelname)s %(message)s'
    formatter = DeltaTimeFormatter(start_time, log_format)
    root_logger = logging.getLogger()
    syslog = logging.StreamHandler()
    syslog.setFormatter(formatter)
    root_logger.addHandler(syslog)

class RmFolderEnvironment:
    def __init__(self, name):
        self.name = name
    def run(self):
        rmfolder(self.name)

class TestEnvironment:
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
        lazy_input_copying=False
    ):
        self.state = state
        self.folder = folder
        self.base_size = None
        self.new_size = None
        self.test_script = test_script
        self.exitcode = None
        self.result = None
        self.order = order
        self.transform = transform
        self.pid_queue = pid_queue
        self.pwd = os.getcwd()
        self.test_case = test_case
        self.all_test_cases = all_test_cases
        self.lazy_input_copying = lazy_input_copying

    @property
    def size_improvement(self):
        return self.base_size - self.new_size

    @property
    def file_count_improvement(self):
        return self.initial_file_count - self.new_file_count

    @property
    def test_case_path(self):
        return self.folder / self.test_case

    @property
    def success(self):
        return self.result == PassResult.OK and self.exitcode == 0

    def dump(self, dst):
        for f in self.all_test_cases:
            if not (self.folder / f).is_dir():
                shutil.copy(self.folder / f, dst)

        shutil.copy(self.test_script, dst)

    @staticmethod
    def extra_file_paths(test_case):
        return Path(test_case).parent.glob('extra*.dat')

    def run(self):
        start_time = time.monotonic()
        try:
            self.base_size = get_file_size(self.test_case)
            self.initial_file_count = get_file_count(self.test_case)
            if self.lazy_input_copying:
                if self.test_case.is_dir():
                    self.test_case_path.mkdir()
            else:
                self.copy_inputs()

            # transform by state
            args = [str(self.test_case_path), self.state, ProcessEventNotifier(self.pid_queue)]
            if self.lazy_input_copying:
                args.append(self.test_case)
            (result, self.state) = self.transform(*args)
            self.result = result
            if self.result != PassResult.OK:
                return self

            # run test script
            self.exitcode = self.run_test(verbose=True)
            if self.exitcode != 0:
                return self

            self.new_size = get_file_size(self.test_case_path)
            self.new_file_count = get_file_count(self.test_case_path)
            self.new_line_count = get_line_count(self.test_case_path)
            self.duration = time.monotonic() - start_time
            # if isinstance(self.state, list):
            #     assert self.size_improvement == self.predicted_improv, f'size_improvement={self.size_improvement} predicted_improv={self.predicted_improv}'
            return self
        except OSError as e:
            # this can happen when we clean up temporary files for cancelled processes
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'TestEnvironment::run OSError: ' + str(e))
            return self
        except Exception as e:
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'Unexpected TestEnvironment::run failure: ' + str(e))
            traceback.print_exc()
            return self

    def run_test(self, verbose):
        try:
            os.chdir(self.folder)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'TestEnvironment.run_test: "{self.test_script}" in "{self.folder}"')
            need_output = verbose and logging.getLogger().isEnabledFor(logging.DEBUG)
            stdout, stderr, returncode = ProcessEventNotifier(self.pid_queue).run_process(
                str(self.test_script), shell=True, need_output=need_output
            )
            if need_output and returncode != 0:
                logging.debug('stdout:\n' + stdout)
                logging.debug('stderr:\n' + stderr)
        finally:
            os.chdir(self.pwd)
        return returncode

    def copy_inputs(self):
        # Copy files to the created folder
        for test_case in self.all_test_cases:
            (self.folder / test_case.parent).mkdir(parents=True, exist_ok=True)
            dest = self.folder / test_case.parent / os.path.basename(test_case)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'TestEnvironment.run: copy from {test_case} to {dest}')
            if test_case.is_dir():
                shutil.copytree(test_case, dest, symlinks=True)
            else:
                shutil.copy2(test_case, dest)
            for extra in self.extra_file_paths(test_case):
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f'TestEnvironment.run: copy from {extra} to {dest.parent}')
                shutil.copy(extra, dest.parent)

def get_heuristic_names_for_log(state, pass_):
    if isinstance(state, list) and hasattr(state[0], 'get_type'):
        improves = collections.defaultdict(int)
        for s in state:
            improves[s.get_type()] += s.get_improv()
        order = sorted([(v, k) for k, v in improves.items()], reverse=True)
        return ' + '.join(n for _, n in order)
    if hasattr(state, 'get_type'):
        return state.get_type()
    return str(pass_)

def create_multiproc_context_wrapper(context, stopped_worker_pids):
    original_proc_cls = context.Process

    class WrapperProcCls(context.Process):
        def __init__(self, **kwargs):
            self.process = original_proc_cls(**kwargs)
            self.stop_reported = False

        def start(self):
            self.process.start()
            self.actual_pid = self.process.pid

        def join(self, *args):
            self.process.join(*args)
            self.maybe_report_stopped()

        def is_alive(self):
            alive = self.process.is_alive()
            self.maybe_report_stopped()
            return alive

        def __getattr__(self, name):
            return getattr(self.process, name)

        def maybe_report_stopped(self):
            if self.process.exitcode is not None and not self.stop_reported:
                self.stop_reported = True
                stopped_worker_pids.put(self.actual_pid)

    context.Process = WrapperProcCls
    return context

class TestManager:
    GIVEUP_CONSTANT = 50000
    MAX_TIMEOUTS = 0  # TODO: 20
    MAX_CRASH_DIRS = 10
    MAX_EXTRA_DIRS = 25000
    TEMP_PREFIX = 'cvise-'
    BUG_DIR_PREFIX = 'cvise_bug_'

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
        self.setup_timeout = timeout
        self.duration_history = collections.deque(maxlen=max(30, 3 * parallel_tests))
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
        self.tmp_for_best = None
        self.current_pass = None
        self.current_passes = None
        self.stopped_worker_pids = queue.Queue()
        self.multiproc_context = create_multiproc_context_wrapper(
            multiprocessing.get_context('forkserver'), self.stopped_worker_pids)

        for test_case in test_cases:
            test_case = Path(test_case)
            self.test_cases_modes[test_case] = test_case.stat().st_mode
            self.check_file_permissions(test_case, [os.F_OK, os.R_OK, os.W_OK], InvalidTestCaseError)
            if test_case.parent.is_absolute():
                raise AbsolutePathTestCaseError(test_case)
            self.test_cases.add(test_case)

        self.orig_total_file_size = self.total_file_size
        self.cache = {}
        self.roots = []
        self.states = []
        self.init_states = []
        if not self.is_valid_test(self.test_script):
            raise InvalidInterestingnessTestError(self.test_script)

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

        self.line_counts = {f: get_line_count(f) for f in self.test_cases}

    def get_timeout(self):
        if not self.duration_history:
            return self.timeout
        mx = max(self.duration_history)
        timeout = round(math.ceil(5 * mx))
        if len(self.duration_history) < self.duration_history.maxlen:
            timeout = max(timeout, self.timeout)
        return timeout

    def create_root(self, p=None, suffix=''):
        pass_name = str(p or self.current_pass).replace('::', '-').replace(' ', '_')
        root = tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}{pass_name}{suffix}-')
        self.roots.append(Path(root))
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'Creating pass root folder: {root}')

    def recreate_root(self, idx, p, suffix):
        pass_name = str(p).replace('::', '-').replace(' ', '_')
        root = tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}{pass_name}{suffix}-')
        self.roots[idx] = Path(root)
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'Creating pass root folder: {root}')

    def remove_root(self):
        if not self.save_temps:
            for r in self.roots:
                rmfolder(r)
            self.roots = []

    def flush_folder_tombstone(self):
        for path in self.folder_tombstone:
            rmfolder(path)
        self.folder_tombstone = []

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
        return sum(get_file_size(t) for t in self.test_cases)

    @property
    def total_file_count(self):
        return sum(get_file_count(t) for t in self.test_cases)

    @property
    def sorted_test_cases(self):
        return sorted(self.test_cases, key=lambda x: get_file_size(x), reverse=True)

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

    def report_pass_bug(self, test_env, problem):
        """Create pass report bug and return True if the directory is created."""

        if not self.die_on_pass_bug:
            assert self.current_pass
            logging.warning(f'{self.current_pass} has encountered a non fatal bug: {problem}')

        crash_dir = self.get_extra_dir(self.BUG_DIR_PREFIX, self.MAX_CRASH_DIRS)

        if crash_dir is None:
            return False

        crash_dir.mkdir()
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
            info_file.write(PassBugError.MSG.format(self.current_pass, problem, test_env.state, crash_dir))

        if self.die_on_pass_bug:
            raise PassBugError(self.current_pass, problem, test_env.state, crash_dir)
        else:
            return True

    @staticmethod
    def diff_files(orig_file, changed_file):
        with open(orig_file) as f:
            orig_file_lines = f.readlines()

        with open(changed_file) as f:
            changed_file_lines = f.readlines()

        diffed_lines = difflib.unified_diff(orig_file_lines, changed_file_lines, str(orig_file), str(changed_file))

        return ''.join(diffed_lines)

    def check_sanity(self, verbose=False):
        logging.debug('perform sanity check... ')

        folder = Path(tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}sanity-'))
        test_env = TestEnvironment(None, 0, self.test_script, folder, list(self.test_cases)[0], self.test_cases, None)
        logging.debug(f'sanity check tmpdir = {test_env.folder}')

        test_env.copy_inputs()

        returncode = test_env.run_test(verbose)
        if returncode == 0:
            rmfolder(folder)
            logging.debug('sanity check successful')
        else:
            if not self.save_temps:
                rmfolder(folder)
            raise InsaneTestCaseError(self.test_cases, self.test_script)

    def release_folder(self, future):
        name = self.temporary_folders.pop(future)
        if not self.save_temps:
            self.folder_tombstone.append(name)

    def release_folders(self):
        for future in self.futures:
            if future.job_type == JobType.PASS_TRANSFORM:
                self.release_folder(future)
        assert not self.temporary_folders
        if self.tmp_for_best:
            self.folder_tombstone.append(self.tmp_for_best)
            self.tmp_for_best = None

    @classmethod
    def log_key_event(cls, event):
        logging.info(f'****** {event} ******')

    def release_future(self, future):
        self.futures.remove(future)

        if future.job_type == JobType.PASS_TRANSFORM:
            pass_ = self.future_to_pass[future]
            assert pass_
            if not isinstance(pass_, list):
                pass_id = self.current_passes.index(pass_)
                self.last_finished_order[pass_id] = future.order

            self.future_to_pass.pop(future)

            self.release_folder(future)

    def save_extra_dir(self, test_case_path):
        extra_dir = self.get_extra_dir('cvise_extra_', self.MAX_EXTRA_DIRS)
        if extra_dir is not None:
            os.mkdir(extra_dir)
            shutil.move(test_case_path, extra_dir)
            logging.info(f'Created extra directory {extra_dir} for you to look at later')

    def sweep_children_processes(self):
        while True:
            try:
                pid = self.stopped_worker_pids.get(block=False)
            except queue.Empty:
                break
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # The whole group just got dead.

    def process_done_futures(self):
        quit_loop = False
        new_futures = set()
        for future in self.futures:
            # all items after first successfull (or STOP) should be cancelled
            if quit_loop and len(self.current_passes) == 1:
                future.cancel()
                continue

            if future.done():
                if future.job_type == JobType.PASS_TRANSFORM and future.merge_comparison_key is not None:
                    self.running_merges.remove(future.merge_comparison_key)

                if future.exception():
                    # starting with Python 3.11: concurrent.futures.TimeoutError == TimeoutError
                    if type(future.exception()) in (TimeoutError, concurrent.futures.TimeoutError):
                        assert future.job_type != JobType.RMFOLDER
                        if future.job_type == JobType.PASS_NEW:
                            logging.warning(f'Heuristic initialization timed out: pass={self.current_passes[future.pass_id]}')
                            self.states[future.pass_id] = None
                        elif future.job_type == JobType.PASS_TRANSFORM:
                            self.finished_transform_jobs += 1
                            logging.warning(f'Test timed out: pass={self.future_to_pass[future]} state={future.state}')
                        else:
                            assert False, f'Unexpected job type {future.job_type}'
                        self.timeout_count += 1
                        if self.timeout_count < self.MAX_TIMEOUTS and False:  # DISABLED
                            self.save_extra_dir(self.temporary_folders[future])
                            if len(self.current_passes) == 1:
                                quit_loop = True
                                p = self.future_to_pass[future]
                                assert p
                                self.states[self.current_passes.index(p)] = None
                        elif self.timeout_count == self.MAX_TIMEOUTS:
                            logging.warning('Maximum number of timeout were reached: %d' % self.MAX_TIMEOUTS)
                        self.sweep_children_processes()
                        continue
                    else:
                        raise future.exception()

                if future.job_type == JobType.RMFOLDER:
                    pass
                elif future.job_type == JobType.PASS_NEW:
                    state = future.result()
                    pass_id = future.pass_id
                    p = self.current_passes[pass_id]
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.debug(f'Init completed for {p}')
                    self.states[pass_id] = state
                    self.init_states[pass_id] = state
                elif future.job_type == JobType.PASS_TRANSFORM:
                    test_env = future.result()
                    opass = self.current_pass
                    self.current_pass = self.future_to_pass[future]
                    assert self.current_pass
                    if isinstance(test_env.state, list):
                        self.any_merge_completed = True
                    outcome = self.check_pass_result(test_env)
                    self.current_pass = opass
                    if outcome == PassCheckingOutcome.ACCEPT:
                        new_futures.add(future)
                    elif outcome == PassCheckingOutcome.IGNORE:
                        self.finished_transform_jobs += 1
                        if isinstance(test_env.state, list):
                            self.failed_merges.append(test_env.state)
                    elif outcome == PassCheckingOutcome.QUIT_LOOP:
                        self.finished_transform_jobs += 1
                        quit_loop = True
                        p = self.future_to_pass[future]
                        self.states[self.current_passes.index(p)] = None
                else:
                    assert False, f'Unexpected job_type {future.job_type}'

            else:
                new_futures.add(future)

        removed_futures = [f for f in self.futures if f not in new_futures]
        for f in removed_futures:
            self.release_future(f)

        return quit_loop

    def wait_for_first_success(self):
        for future in self.futures:
            if future.job_type == JobType.RMFOLDER:
                continue
            try:
                test_env = future.result()
                self.current_pass = self.future_to_pass[future]
                outcome = self.check_pass_result(test_env)
                if outcome == PassCheckingOutcome.ACCEPT:
                    return test_env
            # starting with Python 3.11: concurrent.futures.TimeoutError == TimeoutError
            except (TimeoutError, concurrent.futures.TimeoutError):
                pass
        return None

    def check_pass_result(self, test_env):
        if test_env.success:
            if self.max_improvement is not None and test_env.size_improvement > self.max_improvement:
                logging.debug(f'Too large improvement: {test_env.size_improvement} B')
                return PassCheckingOutcome.IGNORE
            # Report bug if transform did not change the file
            if test_env.size_improvement == 0 and test_env.file_count_improvement == 0:
                if self.current_test_case.is_dir():
                    if test_env.lazy_input_copying:
                        any_change = False
                        for path in test_env.test_case_path.rglob('*'):
                            if not path.is_symlink() and not path.is_dir():
                                orig_path = self.current_test_case / path.relative_to(test_env.test_case_path)
                                any_change = not orig_path.exists() or not filecmp.cmp(path, orig_path)
                        for path in self.current_test_case.rglob('*'):
                            dest_path = test_env.test_case_path / path.relative_to(self.current_test_case)
                            if not dest_path.exists():
                                any_change = True
                    else:
                        any_change = not filecmp.cmpfiles(self.current_test_case, test_env.test_case_path)
                else:
                    any_change = not filecmp.cmp(self.current_test_case, test_env.test_case_path)
                if not any_change:
                    # Temporarily disable because of rmdir spam.
                    # if not self.silent_pass_bug:
                    #     if not self.report_pass_bug(test_env, f'pass failed to modify the variant; state={test_env.state}'):
                    #         return PassCheckingOutcome.QUIT_LOOP
                    return PassCheckingOutcome.IGNORE
            return PassCheckingOutcome.ACCEPT

        # self.pass_statistic.add_failure(self.current_pass)
        if test_env.result == PassResult.OK and test_env.exitcode is not None:
            if self.also_interesting is not None and test_env.exitcode == self.also_interesting:
                self.save_extra_dir(test_env.test_case_path)
        elif test_env.result == PassResult.STOP:
            return PassCheckingOutcome.QUIT_LOOP
        elif test_env.result == PassResult.ERROR or test_env.result == PassResult.OK and test_env.exitcode is None:
            if not self.silent_pass_bug:
                self.report_pass_bug(test_env, 'pass error')
                return PassCheckingOutcome.QUIT_LOOP

        if not self.no_give_up and test_env.order > self.GIVEUP_CONSTANT:
            if not self.giveup_reported:
                self.report_pass_bug(test_env, 'pass got stuck')
                self.giveup_reported = True
            return PassCheckingOutcome.QUIT_LOOP
        return PassCheckingOutcome.IGNORE

    def terminate_all(self, pool):
        for f in self.futures:
            if f.job_type != JobType.RMFOLDER:
                f.cancel()
        wait(f for f in self.futures if f.job_type == JobType.RMFOLDER)
        pool.stop()
        pool.join()

    def get_pass_id_to_init(self):
        pass_id_to_init = None
        meta_pass_id_to_init = None
        meta_requirements_satisfied = True
        for i, s in enumerate(self.states):
            is_meta_pass = self.current_passes[i].arg and self.current_passes[i].arg.startswith('meta::')
            is_meta_requirement = not is_meta_pass and self.current_passes[i].arg and self.current_passes[i].arg not in ('clang_pcm_lazy_load', 'tree_sitter_delta')
            if is_meta_requirement and s in ('needinit', 'initializing'):
                meta_requirements_satisfied = False
            if s == 'needinit':
                if is_meta_pass and meta_pass_id_to_init is None:
                    meta_pass_id_to_init = i
                if not is_meta_pass and pass_id_to_init is None:
                    pass_id_to_init = i
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'pass_id_to_init={pass_id_to_init} meta_pass_id_to_init={meta_pass_id_to_init} meta_requirements_satisfied={meta_requirements_satisfied} states={self.states}')
        if pass_id_to_init is None and meta_pass_id_to_init is not None and meta_requirements_satisfied:
            return meta_pass_id_to_init
        return pass_id_to_init

    def run_parallel_tests(self, passes):
        assert not self.futures
        assert not self.temporary_folders
        self.future_to_pass = {}
        self.last_finished_order = [None] * len(passes)
        with pebble.ProcessPool(max_workers=self.parallel_tests, context=self.multiproc_context, initializer=worker_initializer, initargs=(self.start_time, logging.getLogger().level,)) as pool:
            try:
                order = 1
                next_pass_to_schedule = 0
                pass_job_index = 0
                self.finished_transform_jobs = 0
                self.timeout_count = 0
                self.giveup_reported = False
                success_cnt = 0
                best_success_env = None
                best_success_pass = None
                best_success_improv = None
                best_success_improv_file_count = None
                best_success_comparison_key = None
                measure_start_time = time.monotonic() if self.last_job_update is None else self.last_job_update
                best_improv_speed = None
                best_file_count_improv_speed = None
                any_merge_started = False
                self.any_merge_completed = False
                job_counter_when_init_finished = None
                init_finished_when = None

                successes = [(s, p, i, ifc) for s, p, i, ifc in self.next_successes_hint if not isinstance(s, list)] + self.successes_hint
                successes.sort(key=lambda i: (-i[2], -i[3]))
                self.successes_hint = successes[:self.parallel_tests]
                self.next_successes_hint = []
                attempted_merges = set()
                self.failed_merges = []
                pulse_counter = 0
                self.running_merges = []

                while any(self.states) or self.futures:
                    # do not create too many states
                    new_transform_possible = any(s not in (None, 'needinit', 'initializing') for s in self.states)
                    if len(self.futures) >= self.parallel_tests or not new_transform_possible and self.get_pass_id_to_init() is None:
                        wait(self.futures, return_when=FIRST_COMPLETED)

                    # Occasionally reclaim system resources - just for the case a timed-out worker wasn't terminated
                    # when the future timeout was handled below.
                    self.sweep_children_processes()

                    quit_loop = self.process_done_futures()
                    assert self.finished_transform_jobs <= order
                    if quit_loop and len(self.current_passes) == 1:
                        if success_cnt > 0:
                            self.current_pass = best_success_pass
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.debug(f'run_parallel_tests: proceeding on quit_loop: best improv={best_success_improv} from pass={self.current_pass} state={best_success_env.state}')
                            self.states[0] = best_success_env.state
                            self.terminate_all(pool)
                            return best_success_env
                        else:
                            success = self.wait_for_first_success()
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.debug(f'run_parallel_tests: wait_for_first_success returned {success.state if success else None}')
                            if success:
                                self.states[0] = success.state
                            self.terminate_all(pool)
                            return success

                    now = time.monotonic()
                    self.last_job_update = now

                    if self.finished_transform_jobs // 1000 != pulse_counter:
                        pulse_counter = self.finished_transform_jobs // 1000
                        logging.info(f'pulse: {pulse_counter * 1000} jobs finished')

                    tmp_futures = copy.copy(self.futures)
                    for future in tmp_futures:
                        if future.done() and not future.exception() and future.job_type == JobType.PASS_TRANSFORM:
                            env = future.result()
                            assert env
                            pass_ = self.future_to_pass[future]
                            assert pass_
                            self.current_pass = pass_
                            if isinstance(env.state, list):
                                self.any_merge_completed = True
                            outcome = self.check_pass_result(env)
                            if outcome == PassCheckingOutcome.ACCEPT:
                                success_cnt += 1
                                self.finished_transform_jobs += 1
                                assert self.finished_transform_jobs <= order
                                improv = env.size_improvement
                                improv_file_count = env.file_count_improvement
                                # assert improv == self.run_test_case_size - get_file_size(env.test_case_path), f'improv={improv} run_test_case_size={self.run_test_case_size} get_file_size(env.test_case_path)={get_file_size(env.test_case_path)} files={list(env.test_case_path.rglob('*'))}'
                                if not isinstance(env.state, list):
                                    logging.info(f'on_success_observed: improv={improv} state={env.state} finished_transform_jobs={self.finished_transform_jobs}')
                                if hasattr(pass_, 'on_success_observed'):
                                    pass_.on_success_observed(env.state, improv)
                                self.next_successes_hint.append((env.state, pass_, improv, improv_file_count))
                                self.duration_history.append(env.duration)
                                comparison_key = get_state_comparison_key(improv, improv_file_count)
                                if best_success_improv is None or comparison_key < best_success_comparison_key:
                                    best_success_env = env
                                    best_success_pass = pass_
                                    best_success_improv = improv
                                    best_success_improv_file_count = improv_file_count
                                    best_success_comparison_key = get_state_comparison_key(best_success_improv, best_success_improv_file_count)
                                    if self.tmp_for_best:
                                        self.folder_tombstone.append(self.tmp_for_best)
                                    self.tmp_for_best = tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}best-')
                                    pa = os.path.join(self.tmp_for_best, os.path.basename(future.result().test_case_path))
                                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                                        logging.debug(f'run_parallel_tests: rename from {future.result().test_case_path} to {pa}')
                                    os.rename(future.result().test_case_path, pa)
                                    best_success_env.test_case = pa
                                    dbg = []
                                    dbg.append(f'{-improv} byte{"s" if abs(improv) != 1 else ""}')
                                    if improv_file_count:
                                        dbg.append(f'{-improv_file_count} file{"s" if improv_file_count > 1 else ""}')
                                    if len(self.current_passes) > 1:
                                        logging.info(f'candidate: {", ".join(dbg)} ({get_heuristic_names_for_log(env.state, pass_)})')
                                self.release_future(future)
                                improv_speed = best_success_improv / (now - measure_start_time)
                                file_count_improv_speed = best_success_improv_file_count / (now - measure_start_time)
                                if logging.getLogger().isEnabledFor(logging.DEBUG):
                                    logging.debug(f'observed success success_cnt={success_cnt} improv={improv} improv_file_count={improv_file_count} is_regular_iteration={env.is_regular_iteration} pass={pass_} state={env.state} order={env.order} finished_jobs={self.finished_transform_jobs} failed_merges={len(self.failed_merges)} improv_speed={improv_speed} file_count_improv_speed={file_count_improv_speed} best_improv_speed={best_improv_speed} best_file_count_improv_speed={best_file_count_improv_speed} comparison_key={comparison_key}')
                            elif outcome == PassCheckingOutcome.IGNORE:
                                if isinstance(env.state, list):
                                    self.failed_merges.append(env.state)
                            self.current_pass = None

                    if self.folder_tombstone:
                        path = self.folder_tombstone.pop(0)
                        env = RmFolderEnvironment(path)
                        future = pool.schedule(env.run)
                        future.job_type = JobType.RMFOLDER
                        self.futures.append(future)
                        continue

                    pass_id_to_init = self.get_pass_id_to_init()
                    if pass_id_to_init is not None:
                        pass_id = pass_id_to_init
                        pass_ = passes[pass_id]
                        if logging.getLogger().isEnabledFor(logging.DEBUG):
                            logging.debug(f'Going to init pass {pass_}')
                        self.states[pass_id] = 'initializing'
                        other_init_states = [s for s in self.init_states if s is not None and hasattr(s, 'has_meta_hints') and s.has_meta_hints]
                        env = SetupEnvironment(pass_, self.current_test_case, self.test_cases, self.save_temps, self.last_state_hint[pass_id], other_init_states=other_init_states)
                        future = pool.schedule(env.run, timeout=self.setup_timeout)
                        future.job_type = JobType.PASS_NEW
                        future.pass_id = pass_id
                        self.futures.append(future)
                        order += 1
                        continue
                    if all(s in ('needinit', 'initializing', None) for s in self.states):
                        continue
                    if job_counter_when_init_finished is None and all(s not in ('needinit', 'initializing') for s in self.states):
                        job_counter_when_init_finished = self.finished_transform_jobs
                        init_finished_when = now

                    state = None
                    is_merge = False
                    is_merge_considered = False
                    if self.finished_transform_jobs % 5 == 0:
                        is_merge_considered = True
                        if len(self.next_successes_hint) >= 2:
                            for attempt in range(10):
                                merge_blocklist = set()
                                for failed_state in self.failed_merges:
                                    for s in random.sample(failed_state, (len(failed_state) + 1) // 2):
                                        merge_blocklist.add(get_state_compact_repr(s))
                                merge_cands = [(sta, pa, imp, imp_fc) for sta, pa, imp, imp_fc in self.next_successes_hint
                                                if hasattr(pa, 'supports_merging') and pa.supports_merging() and not isinstance(sta, list) and
                                                get_state_compact_repr(sta) not in merge_blocklist]
                                merge_train = []
                                for sta, pa, imp, imp_fc in sorted(merge_cands, key=lambda item: get_state_comparison_key(item[2], item[3])):
                                    boardable = True
                                    cand_state = [s for s, p, i, ifc in merge_train] + [sta]
                                    if merge_train and (get_state_compact_repr(cand_state) in self.failed_merges):
                                        boardable = False
                                    if boardable:
                                        merge_train.append((sta, pa, imp, imp_fc))
                                merge_comparison_key = None
                                if len(merge_train) >= 2:
                                    merge_pass = merge_train[0][1]
                                    merged_state = [sta for sta, pa, imp, imp_fc in merge_train]
                                    merge_improv, merge_improv_file_count = merge_pass.predict_improv(self.current_test_case, merged_state)
                                    # logging.info(f'run_parallel_tests: merge_train={merge_train} merge_improv={merge_improv} merge_improv_file_count={merge_improv_file_count} in_attempted={merge_improv in attempted_merges}')
                                    merge_repr = get_state_compact_repr([sta for sta, pa, imp, imp_fc in merge_train])
                                    if (merge_improv > 0 or merge_improv_file_count > 0) and (merge_repr not in attempted_merges):
                                        pass_id = passes.index(merge_train[0][1])
                                        pass_ = passes[pass_id]
                                        merge_comparison_key = get_state_comparison_key(merge_improv, merge_improv_file_count)
                                        if merge_comparison_key < best_success_comparison_key:
                                            state = merged_state
                                            is_merge = True
                                            attempted_merges.add(merge_repr)
                                            should_advance = False
                                            any_merge_started = True
                                            self.running_merges.append(merge_comparison_key)
                                if state or not self.failed_merges:
                                    break

                    if not state and any(s not in ('needinit', 'initializing', None) for s in self.states):
                        pass_job_index += 1
                        while pass_job_index >= passes[next_pass_to_schedule].jobs or self.states[next_pass_to_schedule] in ('needinit', 'initializing', None):
                            pass_job_index = 0
                            if next_pass_to_schedule + 1 < len(passes):
                                next_pass_to_schedule += 1
                            else:
                                next_pass_to_schedule = 0
                        pass_id = next_pass_to_schedule
                        state = self.states[pass_id]
                        should_advance = True
                        pass_ = passes[pass_id]

                    if state:
                        tmp_parent_dir = self.roots[pass_id]
                        folder = Path(tempfile.mkdtemp(f'{self.TEMP_PREFIX}job{order}', dir=tmp_parent_dir))
                        test_case = self.current_test_case
                        transform = pass_.transform
                        lazy_input_copying = hasattr(pass_, 'lazy_input_copying') and pass_.lazy_input_copying()
                        test_env = TestEnvironment(
                            state,
                            order,
                            self.test_script,
                            folder,
                            test_case,
                            self.test_cases,
                            transform,
                            self.pid_queue,
                            lazy_input_copying,
                        )
                        if isinstance(state, list):
                            test_env.predicted_improv = merge_improv
                        test_env.is_regular_iteration = should_advance

                        new_timeout = self.get_timeout()
                        if new_timeout != self.timeout:
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.debug(f'changing timeout: new={new_timeout} old={self.timeout}')
                            self.timeout = new_timeout
                        future = pool.schedule(test_env.run, timeout=self.timeout)
                        future.job_type = JobType.PASS_TRANSFORM
                        future.order = order
                        future.pass_id = pass_id
                        future.state = state
                        future.merge_comparison_key = merge_comparison_key if is_merge else None
                        self.future_to_pass[future] = pass_
                        assert len(self.future_to_pass) <= self.parallel_tests
                        assert future not in self.temporary_folders
                        self.temporary_folders[future] = folder
                        assert len(self.temporary_folders) <= self.parallel_tests
                        self.futures.append(future)
                        assert len(self.futures) <= self.parallel_tests
                        # self.pass_statistic.add_executed(self.current_pass)
                        order += 1

                        if should_advance:
                            old_state = copy.copy(state)
                            state = pass_.advance(self.current_test_case, state)
                            # logging.info(f'advance: from {old_state} to {state} pass={pass_}')
                            self.states[pass_id] = state
                            self.last_state_hint[pass_id] = state
                            if state is None and len(passes) == 1:
                                if success_cnt > 0:
                                    self.current_pass = best_success_pass
                                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                                        logging.debug(f'run_parallel_tests: proceeding on end state with best: improv={best_success_improv} is_regular_iteration={best_success_env.is_regular_iteration} from pass={self.current_pass} state={best_success_env.state}')
                                    self.states[0] = best_success_env.state
                                    self.terminate_all(pool)
                                    return best_success_env
                                else:
                                    success = self.wait_for_first_success()
                                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                                        logging.debug(f'run_parallel_tests: proceeding on end after wait_for_first_success: state={success.state if success else None}')
                                    if success:
                                        self.states[0] = success.state
                                    self.terminate_all(pool)
                                    return success

                    if success_cnt > 0 and is_merge_considered:
                        improv_speed = best_success_improv / (now - measure_start_time)
                        file_count_improv_speed = best_success_improv_file_count / (now - measure_start_time)
                        should_proceed = not self.futures or len(self.current_passes) == 1
                        if not should_proceed and job_counter_when_init_finished is not None and \
                            self.finished_transform_jobs - job_counter_when_init_finished > max(self.parallel_tests, len(self.current_passes)) * 5 and \
                                (not any_merge_started or self.any_merge_completed) and now - init_finished_when >= 10:
                            if best_improv_speed is None or improv_speed > best_improv_speed:
                                if logging.getLogger().isEnabledFor(logging.DEBUG):
                                    logging.debug(f'run_parallel_tests: new best_improv_speed={improv_speed} old={best_improv_speed}')
                                best_improv_speed = improv_speed
                            if best_file_count_improv_speed is None or file_count_improv_speed > best_file_count_improv_speed:
                                best_file_count_improv_speed = file_count_improv_speed
                            if (-improv_speed, -file_count_improv_speed) > multiply_comparison_key((-best_improv_speed, -best_file_count_improv_speed), 0.9):
                                # logging.info(f'self.running_merges={min(self.running_merges)} vs best_success_comparison_key={best_success_comparison_key}')
                                if not self.running_merges:
                                    should_proceed = True
                                elif min(self.running_merges) > multiply_comparison_key(best_success_comparison_key, 1.1):
                                    should_proceed = True
                                # else:
                                #     logging.info(f'PROLONGING RUN because of merge train {min(self.running_merges)} vs best_success_comparison_key={best_success_comparison_key}')

                        if should_proceed:
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.debug(f'run_parallel_tests: proceeding: finished_jobs={self.finished_transform_jobs} failed_merges={len(self.failed_merges)} order={order} improv={best_success_improv} improv_file_count={best_success_improv_file_count} is_regular_iteration={best_success_env.is_regular_iteration} from pass={best_success_pass} state={best_success_env.state} comparison_key={get_state_comparison_key(best_success_improv, best_success_improv_file_count)} improv_speed={improv_speed} file_count_improv_speed={file_count_improv_speed} best_improv_speed={best_improv_speed} best_file_count_improv_speed={best_file_count_improv_speed}')
                            transform_futures = list(f for f in self.futures if f.job_type == JobType.PASS_TRANSFORM)
                            for pass_id, state in dict((fu.pass_id, fu.state)
                                                    for fu in sorted(transform_futures, key=lambda fu: -fu.order)
                                                    if not isinstance(fu.pass_id, list) and
                                                    fu.order > (self.last_finished_order[fu.pass_id] or 0)).items():
                                if logging.getLogger().isEnabledFor(logging.DEBUG):
                                    logging.debug(f'run_parallel_tests: rewinding {passes[pass_id]} from {self.states[pass_id]} to {state}')
                                self.states[pass_id] = state
                            self.terminate_all(pool)
                            return best_success_env

                # we are at the end of enumeration
                self.current_pass = best_success_pass
                self.terminate_all(pool)
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f'TestManager.run_parallel_tests: }}END: end of enumeration')
                return best_success_env

            except Exception as e:
                logging.info('Exception caught, aborting the pool')
                self.terminate_all(pool)
                self.sweep_children_processes()
                raise

    def run_pass(self, pass_):
        if self.start_with_pass:
            if self.start_with_pass == str(pass_):
                self.start_with_pass = None
            else:
                return

        self.current_pass = pass_
        self.current_passes = [pass_]
        self.futures = []
        self.temporary_folders = {}
        self.folder_tombstone = []
        # m = Manager()
        # self.pid_queue = m.Queue()
        self.pid_queue = None
        self.create_root()
        pass_key = repr(self.current_pass)
        self.last_state_hint = [None]
        self.successes_hint = []
        self.next_successes_hint = []
        if not self.tmp_for_best:
            self.tmp_for_best = tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}best-')

        logging.info(f'===< {self.current_pass} >===')

        if self.total_file_size == 0:
            raise ZeroSizeError(self.test_cases)

        # self.pass_statistic.start(self.current_pass)
        if not self.skip_key_off:
            logger = KeyLogger()

        try:
            for test_case in self.sorted_test_cases:
                self.current_test_case = test_case
                starting_test_case_size = get_file_size(test_case)
                self.success_count = 0

                if get_file_size(test_case) == 0:
                    continue

                if not self.no_cache:
                    with open(test_case, mode='rb+') as tmp_file:
                        test_case_before_pass = tmp_file.read()

                        if pass_key in self.cache and test_case_before_pass in self.cache[pass_key]:
                            tmp_file.seek(0)
                            tmp_file.truncate(0)
                            tmp_file.write(self.cache[pass_key][test_case_before_pass])
                            logging.info(f'cache hit for {test_case}')
                            continue

                # create initial state
                self.states = [self.current_pass.new(self.current_test_case, self.check_sanity)]
                self.skip = False
                self.last_job_update = None

                self.timeout_count = 0
                while self.states[0] is not None and not self.skip:
                    # Ignore more key presses after skip has been detected
                    if not self.skip_key_off and not self.skip:
                        key = logger.pressed_key()
                        if key == 's':
                            self.skip = True
                            self.log_key_event('skipping the rest of this pass')
                        elif key == 'd':
                            self.log_key_event('toggle print diff')
                            self.print_diff = not self.print_diff

                    self.run_test_case_size = get_file_size(self.current_test_case)
                    success_env = self.run_parallel_tests(passes=[self.current_pass])
                    self.sweep_children_processes()
                    self.current_pass = pass_

                    if success_env:
                        if self.process_result(success_env):
                            self.success_count += 1

                    # if the file increases significantly, bail out the current pass
                    test_case_size = get_file_size(self.current_test_case)
                    if test_case_size >= MAX_PASS_INCREASEMENT_THRESHOLD * starting_test_case_size:
                        logging.info(
                            f'skipping the rest of the pass (huge file increasement '
                            f'{MAX_PASS_INCREASEMENT_THRESHOLD * 100}%)'
                        )
                        break

                    self.release_folders()
                    self.futures.clear()
                    if not success_env:
                        break

                    # skip after N transformations if requested
                    if (self.skip_after_n_transforms and self.success_count >= self.skip_after_n_transforms) or (
                        self.current_pass.max_transforms and self.success_count >= self.current_pass.max_transforms
                    ):
                        logging.info(f'skipping after {self.success_count} successful transformations')
                        break

                # Cache result of this pass
                if not self.no_cache:
                    with open(test_case, mode='rb') as tmp_file:
                        if pass_key not in self.cache:
                            self.cache[pass_key] = {}

                        self.cache[pass_key][test_case_before_pass] = tmp_file.read()

            self.restore_mode()
            # self.pass_statistic.stop(self.current_pass)
            self.remove_root()
            self.flush_folder_tombstone()
            self.current_passes = None
        except KeyboardInterrupt:
            logging.info('Exiting now ...')
            self.remove_root()
            self.flush_folder_tombstone()
            sys.exit(1)

    def run_concurrent_passes(self, passes):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'run_concurrent_passes: BEGIN{{')

        self.current_pass = None
        self.current_passes = passes
        self.futures = []
        self.temporary_folders = {}
        self.folder_tombstone = []
        # m = Manager()
        # self.pid_queue = m.Queue()
        self.pid_queue = None
        if not self.tmp_for_best:
            self.tmp_for_best = tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}best-')
        for p in passes:
            self.create_root(p)
        # pass_key = repr(self.current_pass)

        # logging.info(f'===< {self.current_pass} >===')

        if self.total_file_size == 0:
            raise ZeroSizeError(self.test_cases)

        # self.pass_statistic.start(self.current_pass)
        if not self.skip_key_off:
            logger = KeyLogger()

        self.last_state_hint = [None] * len(passes)
        self.successes_hint = []
        self.next_successes_hint = []

        try:
            for test_case in self.sorted_test_cases:
                self.current_test_case = test_case
                starting_test_case_size = get_file_size(test_case)
                self.success_count = 0

                if get_file_size(test_case) == 0:
                    continue

                # if not self.no_cache:
                #     with open(test_case, mode='rb+') as tmp_file:
                #         test_case_before_pass = tmp_file.read()

                #         if pass_key in self.cache and test_case_before_pass in self.cache[pass_key]:
                #             tmp_file.seek(0)
                #             tmp_file.truncate(0)
                #             tmp_file.write(self.cache[pass_key][test_case_before_pass])
                #             logging.info(f'cache hit for {test_case}')
                #             continue


                # create initial state
                self.init_all_passes(passes)
                self.skip = False
                self.last_job_update = None

                while any(self.states) and not self.skip:
                    # Ignore more key presses after skip has been detected
                    if not self.skip_key_off and not self.skip:
                        key = logger.pressed_key()
                        if key == 's':
                            self.skip = True
                            self.log_key_event('skipping the rest of this pass')
                        elif key == 'd':
                            self.log_key_event('toggle print diff')
                            self.print_diff = not self.print_diff

                    self.run_test_case_size = get_file_size(self.current_test_case)
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.debug(f'run_test_case_size={self.run_test_case_size}')
                    success_env = self.run_parallel_tests(passes)
                    self.sweep_children_processes()

                    if success_env:
                        if self.process_result(success_env):
                            self.success_count += 1

                    # if the file increases significantly, bail out the current pass
                    test_case_size = get_file_size(self.current_test_case)
                    if test_case_size >= MAX_PASS_INCREASEMENT_THRESHOLD * starting_test_case_size:
                        logging.info(
                            f'skipping the rest of the pass (huge file increasement '
                            f'{MAX_PASS_INCREASEMENT_THRESHOLD * 100}%)'
                        )
                        break

                    self.release_folders()
                    self.futures.clear()
                    if not success_env:
                        break

                    # skip after N transformations if requested
                    # if (self.skip_after_n_transforms and self.success_count >= self.skip_after_n_transforms) or (
                    #     self.current_pass.max_transforms and self.success_count >= self.current_pass.max_transforms
                    # ):
                    #     logging.info(f'skipping after {self.success_count} successful transformations')
                    #     break

                    self.init_all_passes(passes)

                # Cache result of this pass
                # if not self.no_cache:
                #     with open(test_case, mode='rb') as tmp_file:
                #         if pass_key not in self.cache:
                #             self.cache[pass_key] = {}

                #         self.cache[pass_key][test_case_before_pass] = tmp_file.read()

            self.restore_mode()
            # self.pass_statistic.stop(self.current_pass)
            self.remove_root()
            self.current_pass = None
            self.current_passes = None
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'run_concurrent_passes: }}END')
        except KeyboardInterrupt:
            logging.info('Exiting now ...')
            self.remove_root()
            sys.exit(1)

    def init_all_passes(self, passes):
        self.states = ['needinit' for p in passes]
        self.init_states = [None] * len(passes)

    def process_result(self, test_env):
        if self.print_diff:
            diff_str = self.diff_files(self.current_test_case, test_env.test_case_path)
            if self.use_colordiff:
                diff_str = subprocess.check_output('colordiff', shell=True, encoding='utf8', input=diff_str)
            logging.info(diff_str)

        try:
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'process_result: copy from {test_env.test_case_path} to {self.current_test_case}')
            if test_env.test_case_path.is_dir():
                if test_env.lazy_input_copying:
                    any_change = False
                    for path in test_env.test_case_path.rglob('*'):
                        if not path.is_symlink() and not path.is_dir():
                            shutil.copy(path, self.current_test_case / path.relative_to(test_env.test_case_path))
                            any_change = True
                    for path in self.current_test_case.rglob('*'):
                        dest_path = test_env.test_case_path / path.relative_to(self.current_test_case)
                        if not dest_path.exists():
                            if path.is_dir():
                                os.rmdir(path)
                            else:
                                os.unlink(path)
                            any_change = True
                    if not any_change:
                        return False
                else:
                    rmtree(self.current_test_case)
                    shutil.copytree(test_env.test_case_path, self.current_test_case, symlinks=True)
            else:
                shutil.copy(test_env.test_case_path, self.current_test_case)
            self.line_counts[self.current_test_case] = test_env.new_line_count
        except FileNotFoundError:
            raise RuntimeError(
                f"Can't find {self.current_test_case} -- did your interestingness test move it?"
            ) from None
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            assert len(self.line_counts) == len(self.test_cases)
            for f in self.test_cases:
                assert self.line_counts[f] == get_line_count(f)

        if len(self.current_passes) == 1:
            assert self.states[0]
            self.states[0] = self.current_pass.advance_on_success(test_env.test_case_path, self.states[0])
            # self.pass_statistic.add_success(self.current_pass)

        pct = 100 - (self.total_file_size * 100.0 / self.orig_total_file_size)
        notes = []
        notes.append(f'{round(pct, 1)}%')
        notes.append(f'{self.total_file_size} bytes')
        c = self.total_file_count
        notes.append(f'{c} file{"s" if c > 1 else ""}')
        notes.append(f'{sum(self.line_counts.values())} lines')
        if len(self.test_cases) > 1:
            notes.append(str(test_env.test_case))

        logging.info('(' + ', '.join(notes) + ')')
        return True


def get_state_comparison_key(improv, improv_file_count):
    return -improv, -improv_file_count

def multiply_comparison_key(key, factor):
    return key[0] * factor, key[1] * factor

def get_state_compact_repr(state):
    if isinstance(state, list):
        return frozenset(sorted(get_state_compact_repr(s) for s in state))
    return (state.begin(), state.end())

class SetupEnvironment:
    def __init__(self, pass_, test_case, test_cases, save_temps, last_state_hint, other_init_states):
        self.pass_ = pass_
        self.test_case = test_case
        self.test_cases = test_cases
        self.save_temps = save_temps
        self.last_state_hint = last_state_hint
        self.other_init_states = other_init_states

    def run(self):
        return self.pass_.new(self.test_case, last_state_hint=self.last_state_hint, other_init_states=self.other_init_states)
