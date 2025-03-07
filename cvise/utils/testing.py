import collections
from concurrent.futures import FIRST_COMPLETED, wait
import copy
import difflib
from enum import auto, Enum, unique
import filecmp
import logging
import math
from multiprocessing import Manager
import os
from pathlib import Path
import platform
import random
import shutil
import subprocess
import sys
import tempfile
import traceback
import concurrent.futures
import statistics
import threading
import time

from cvise.cvise import CVise
from cvise.passes.abstract import PassResult, ProcessEventNotifier, ProcessEventType, MergedState
from cvise.utils.error import AbsolutePathTestCaseError
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.error import InvalidInterestingnessTestError
from cvise.utils.error import InvalidTestCaseError
from cvise.utils.error import PassBugError
from cvise.utils.error import ZeroSizeError
from cvise.utils.misc import is_readable_file
from cvise.utils.readkey import KeyLogger
import pebble
import psutil

# change default Pebble sleep unit for faster response
pebble.common.SLEEP_UNIT = 0.01
MAX_PASS_INCREASEMENT_THRESHOLD = 3


def get_file_size(path):
    inner_paths = [f for f in Path(path).rglob('*') if not f.is_dir() and not f.is_symlink()] if os.path.isdir(path) else [path]
    return sum(f.stat().st_size for f in inner_paths)

def get_file_count(path):
    inner_paths = [f for f in Path(path).rglob('*') if not f.is_dir() and not f.is_symlink()] if os.path.isdir(path) else [path]
    return len(inner_paths)


@unique
class PassCheckingOutcome(Enum):
    """Outcome of checking the result of an invocation of a pass."""

    ACCEPT = auto()
    IGNORE = auto()
    QUIT_LOOP = auto()


def rmfolder(name):
    assert 'cvise' in str(name)
    try:
        logging.debug(f'rmfolder {name}')
        shutil.rmtree(name)
    except OSError:
        pass

def schedule_rmfolder(name):
    threading.Thread(target=lambda: rmfolder(name)).start()


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
        self.test_case_full_path = test_case
        self.test_case = test_case[0].name if isinstance(test_case, list) else test_case.name
        self.all_test_cases = all_test_cases

    @property
    def size_improvement(self):
        return self.base_size - self.new_size

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

    @staticmethod
    def extra_file_paths(test_case):
        return Path(test_case).parent.glob('extra*.dat')

    def run(self):
        try:
            self.base_size = get_file_size(self.test_case_full_path[0] if isinstance(self.test_case_full_path, list) else self.test_case_full_path)

            # Copy files to the created folder
            for test_case in self.all_test_cases:
                if os.path.basename(test_case) == self.test_case:
                    continue
                (self.folder / test_case.parent).mkdir(parents=True, exist_ok=True)
                dest = self.folder / test_case.parent / os.path.basename(test_case)
                logging.debug(f'TestEnvironment.run: copy from {test_case} to {dest}')
                shutil.copytree(test_case, dest, symlinks=True)
                for extra in self.extra_file_paths(test_case):
                    logging.debug(f'TestEnvironment.run: copy from {extra} to {dest.parent}')
                    shutil.copy(extra, dest.parent)

            src = self.test_case_full_path[0] if isinstance(self.test_case_full_path, list) else self.test_case_full_path
            dest = self.folder / test_case.parent / os.path.basename(test_case)
            logging.debug(f'TestEnvironment.run: copy from {src} to {dest}')
            shutil.copytree(src, dest, symlinks=True)
            for origin in self.test_case_full_path if isinstance(self.test_case_full_path, list) else [self.test_case_full_path]:
                for extra in self.extra_file_paths(origin):
                    logging.debug(f'TestEnvironment.run: copy from {extra} to {dest.parent}')
                    shutil.copy(extra, dest.parent)

            # transform by state
            if isinstance(self.state, MergedState):
                assert self.state.path_pass_state_tuples
                path_to_pass_id = {}
                path_to_states = {}
                for path, pass_id, state in self.state.path_pass_state_tuples:
                    path_to_pass_id[path] = pass_id
                    path_to_states.setdefault(path, [])
                    path_to_states[path].append(state)
                files_dbg = list(Path(self.test_case_path).rglob('*'))
                logging.debug(f'TestEnvironment.run: merged: BEGIN{{: files={files_dbg}')
                new_path_pass_state_tuples = []
                for path, pass_id in path_to_pass_id.items():
                    transform = self.transform[pass_id]
                    (result, upd_state) = transform(
                        str(self.test_case_path), path_to_states[path], ProcessEventNotifier(self.pid_queue))
                    self.result = result
                    if self.result != PassResult.OK:
                        logging.debug(f'TestEnvironment.run: merged: }}END: error')
                        return self
                    new_path_pass_state_tuples.append((path, pass_id, upd_state))
                self.state.path_pass_state_tuples = new_path_pass_state_tuples
                logging.debug(f'TestEnvironment.run: merged: }}END')
            else:
                (result, self.state) = self.transform(
                    str(self.test_case_path), self.state, ProcessEventNotifier(self.pid_queue)
                )
            self.result = result
            if self.result != PassResult.OK:
                return self
            self.new_size = get_file_size(self.test_case_path)

            # run test script
            self.exitcode = self.run_test(verbose=True)

            return self
        except OSError as e:
            # this can happen when we clean up temporary files for cancelled processes
            logging.debug(f'TestEnvironment::run OSError: ' + str(e))
            return self
        except Exception as e:
            logging.debug(f'Unexpected TestEnvironment::run failure: ' + str(e))
            traceback.print_exc()
            return self

    def run_test(self, verbose):
        try:
            os.chdir(self.folder)
            logging.debug(f'TestEnvironment.run_test: "{self.test_script}" in "{self.folder}"')
            stdout, stderr, returncode = ProcessEventNotifier(self.pid_queue).run_process(
                str(self.test_script), shell=True
            )
            if verbose and returncode != 0:
                logging.debug('stdout:\n' + stdout)
                logging.debug('stderr:\n' + stderr)
        finally:
            os.chdir(self.pwd)
        return returncode

def get_available_cores():
    try:
        # try to detect only physical cores, ignore HyperThreading
        # in order to speed up parallel execution
        core_count = psutil.cpu_count(logical=False)
        if not core_count:
            core_count = psutil.cpu_count(logical=True)
        # respect affinity
        try:
            affinity = len(psutil.Process().cpu_affinity())
            assert affinity >= 1
        except AttributeError:
            return core_count

        if core_count:
            core_count = min(core_count, affinity)
        else:
            core_count = affinity
        return core_count
    except NotImplementedError:
        return 1

CORES = get_available_cores()

desired_pace = None
size_pass = None
size_history = collections.deque(maxlen=100*CORES)

def check_pass(current_pass):
    global size_pass
    if size_pass != current_pass:
        size_history.clear()
        size_pass = current_pass

def on_scheduled(current_pass, size):
    check_pass(current_pass)
    size_history.append(size)

def on_succeeded(current_pass, size):
    check_pass(current_pass)
    size_history.append(size)

ME = None

def get_conf_interval(current_pass):
    check_pass(current_pass)
    global ME
    ME = None
    if len(size_history) < 30:
        return None
    szdiff = [size_history[i] - size_history[i+1] for i in range(len(size_history)-1)]
    mean = statistics.mean(szdiff)
    ME = mean
    sigma = statistics.stdev(szdiff)
    K = 4.47
    return mean + K * sigma / math.sqrt(len(szdiff))

def is_pace_good(current_pass, desired):
    if desired is None:
        return True
    check_pass(current_pass)
    if len(size_history) < 10*CORES:
        return True
    conf_r = get_conf_interval(current_pass)
    if conf_r is None:
        return True
    logging.info(f'is_pace_good: len={len(size_history)} conf=0..{round(conf_r,1)} mean={round(ME,1)} desired={round(desired,1)}')
    return desired <= conf_r

class TestManager:
    GIVEUP_CONSTANT = 50000
    MAX_TIMEOUTS = 20
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
        self.strategy = 'size'

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

    def create_root(self, p=None, suffix=''):
        pass_name = str(p or self.current_pass).replace('::', '-').replace(' ', '_')
        root = tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}{pass_name}{suffix}-')
        self.roots.append(Path(root))
        logging.debug(f'Creating pass root folder: {root}')

    def recreate_root(self, idx, p, suffix):
        pass_name = str(p).replace('::', '-').replace(' ', '_')
        root = tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}{pass_name}{suffix}-')
        self.roots[idx] = Path(root)
        logging.debug(f'Creating pass root folder: {root}')

    def remove_root(self):
        if not self.save_temps:
            for r in self.roots:
                rmfolder(r)
            self.roots = []

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

    @property
    def total_line_count(self):
        return self.get_line_count(self.test_cases)

    @staticmethod
    def get_line_count(files):
        lines = 0
        for outer_file in files:
            inner_files = [f for f in Path(outer_file).rglob('*') if not f.is_dir() and not f.is_symlink()] if os.path.isdir(outer_file) else [outer_file]
            for file in inner_files:
                if is_readable_file(file):
                    with open(file) as f:
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
            schedule_rmfolder(name)

    def release_folders(self):
        for future in self.futures:
            self.release_folder(future)
        assert not self.temporary_folders

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

    def release_future(self, future):
        self.futures.remove(future)

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

    def process_done_futures(self):
        quit_loop = False
        new_futures = set()
        for future in self.futures:
            # all items after first successfull (or STOP) should be cancelled
            if quit_loop and len(self.current_passes) == 1:
                future.cancel()
                continue

            if future.done():
                if future.exception():
                    # starting with Python 3.11: concurrent.futures.TimeoutError == TimeoutError
                    if type(future.exception()) in (TimeoutError, concurrent.futures.TimeoutError):
                        logging.warning(f'Test timed out: pass={self.future_to_pass[future]} state={future.state}')
                        self.timeout_count += 1
                        if self.timeout_count < self.MAX_TIMEOUTS or False:  # DISABLED
                            self.save_extra_dir(self.temporary_folders[future])
                            if len(self.current_passes) == 1:
                                quit_loop = True
                                p = self.future_to_pass[future]
                                assert p
                                self.states[self.current_passes.index(p)] = None
                        elif self.timeout_count == self.MAX_TIMEOUTS:
                            logging.warning('Maximum number of timeout were reached: %d' % self.MAX_TIMEOUTS)
                        continue
                    else:
                        raise future.exception()

                test_env = future.result()
                opass = self.current_pass
                self.current_pass = self.future_to_pass[future]
                assert self.current_pass
                outcome = self.check_pass_result(test_env)
                self.current_pass = opass
                if outcome == PassCheckingOutcome.ACCEPT:
                    new_futures.add(future)
                elif outcome == PassCheckingOutcome.IGNORE:
                    pass
                elif outcome == PassCheckingOutcome.QUIT_LOOP:
                    quit_loop = True
                    p = self.future_to_pass[future]
                    self.states[self.current_passes.index(p)] = None
                
                # pass_id = self.current_passes.index(self.future_to_pass[future])
                # if hasattr(test_env.state, 'instances') and self.states[pass_id] and test_env.state.instances != self.states[pass_id].instances:
                #     logging.info(f'Adjusting instances for {self.future_to_pass[future]} from {self.states[pass_id].instances} to {test_env.state.instances}')
                #     # assert test_env.state.instances < self.states[pass_id].instances
                #     self.states[pass_id].instances = test_env.state.instances
                #     if self.states[pass_id].chunk > self.states[pass_id].instances:
                #         self.states[pass_id].chunk = self.states[pass_id].instances
                #     if self.states[pass_id].index >= self.states[pass_id].instances:
                #         self.states[pass_id].index = self.states[pass_id].instances - 1
            
            else:
                new_futures.add(future)

        removed_futures = [f for f in self.futures if f not in new_futures]
        for f in removed_futures:
            self.release_future(f)

        return quit_loop

    def wait_for_first_success(self):
        for future in self.futures:
            try:
                test_env = future.result()
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
            if filecmp.cmp(self.current_test_case, test_env.test_case_path):
                if not self.silent_pass_bug:
                    if not self.report_pass_bug(test_env, 'pass failed to modify the variant'):
                        return PassCheckingOutcome.QUIT_LOOP
                return PassCheckingOutcome.IGNORE
            return PassCheckingOutcome.ACCEPT

        # self.pass_statistic.add_failure(self.current_pass)
        if test_env.result == PassResult.OK:
            assert test_env.exitcode
            if self.also_interesting is not None and test_env.exitcode == self.also_interesting:
                self.save_extra_dir(test_env.test_case_path)
        elif test_env.result == PassResult.STOP:
            return PassCheckingOutcome.QUIT_LOOP
        elif test_env.result == PassResult.ERROR:
            if not self.silent_pass_bug:
                self.report_pass_bug(test_env, 'pass error')
                return PassCheckingOutcome.QUIT_LOOP

        if not self.no_give_up and test_env.order > self.GIVEUP_CONSTANT:
            if not self.giveup_reported:
                self.report_pass_bug(test_env, 'pass got stuck')
                self.giveup_reported = True
            return PassCheckingOutcome.QUIT_LOOP
        return PassCheckingOutcome.IGNORE

    @classmethod
    def terminate_all(cls, pool):
        pool.stop()
        pool.join()

    def get_state_comparison_key(self, state, improv):
        if self.strategy == 'size':
            return -improv
        elif self.strategy == 'topo':
            if isinstance(state, list) or isinstance(state, MergedState):
                inner_states = state if isinstance(state, list) else [i for _, _, i in state.path_pass_state_tuples]
                inner_keys = [self.get_state_comparison_key(i, improv=None) for i in inner_states]
                total_improv_per_depth = [0] * max(len(i) for i in inner_keys)
                for improv_per_depth in inner_keys:
                    for i in range(len(improv_per_depth)):
                        total_improv_per_depth[i] += improv_per_depth[i]
                return total_improv_per_depth
            else:
                improv_per_depth = state.improv_per_depth if hasattr(state, 'improv_per_depth') else []
                return [-i for i in improv_per_depth]

    def run_parallel_tests(self, passes):
        assert not self.futures
        assert not self.temporary_folders
        self.future_to_pass = {}
        self.last_finished_order = [None] * len(passes)
        with pebble.ProcessPool(max_workers=self.parallel_tests) as pool:
            order = 1
            next_pass_to_schedule = 0
            pass_job_index = 0
            finished_jobs = 0
            self.timeout_count = 0
            self.giveup_reported = False
            success_cnt = 0
            best_success_env = None
            best_success_pass = None
            best_success_improv = None
            best_success_when = None
            finished_job_improves = []
            recent_success_job_counter = None
            best_success_job_counter = None
            start_time = time.monotonic()

            successes = [(s, p, i) for s, p, i in self.next_successes_hint if not isinstance(s, list)] + self.successes_hint
            successes.sort(key=lambda i: -i[2])
            self.successes_hint = successes[:self.parallel_tests]
            self.next_successes_hint = []
            attempted_merges = set()

            while any(self.states):
                # print(f'TestManager.run_parallel_tests: while iteration')

                # logging.info(f'run_parallel_tests: true states cnt {len(list(filter(None, self.states)))} success_cnt={success_cnt}')
                # do not create too many states
                if len(self.futures) >= self.parallel_tests:
                    wait(self.futures, return_when=FIRST_COMPLETED)

                cnt_before = len(self.futures)
                quit_loop = self.process_done_futures()
                cnt_after = len(self.futures)
                finished_jobs += cnt_before - cnt_after
                assert finished_jobs <= order
                finished_job_improves += [0] * (cnt_before - cnt_after)
                assert len(finished_job_improves) == finished_jobs
                if quit_loop and len(self.current_passes) == 1:
                    if success_cnt > 0:
                        self.current_pass = best_success_pass
                        logging.info(f'run_parallel_tests: proceeding on quit_loop: best improv={best_success_improv} from pass={self.current_pass} state={best_success_env.state}')
                        self.terminate_all(pool)
                        return best_success_env
                    else:
                        success = self.wait_for_first_success()
                        logging.info(f'run_parallel_tests: wait_for_first_success returned {success.state if success else None}')
                        self.terminate_all(pool)
                        return success

                tmp_futures = copy.copy(self.futures)
                for future in tmp_futures:
                    if future.done() and not future.exception():
                        env = future.result()
                        assert env
                        pass_ = self.future_to_pass[future]
                        assert pass_
                        self.current_pass = pass_
                        if self.check_pass_result(env) == PassCheckingOutcome.ACCEPT:
                            success_cnt += 1
                            finished_jobs += 1
                            assert finished_jobs <= order
                            improv = env.size_improvement
                            assert improv == self.run_test_case_size - get_file_size(env.test_case_path)
                            finished_job_improves.append(improv)
                            assert len(finished_job_improves) == finished_jobs
                            logging.info(f'observed success success_cnt={success_cnt} improv={improv} is_regular_iteration={env.is_regular_iteration} pass={pass_} state={env.state} order={env.order} finished_jobs={finished_jobs} comparison_key={self.get_state_comparison_key(env.state, improv)}')
                            if hasattr(pass_, 'on_success_observed'):
                                pass_.on_success_observed(env.state)
                            self.next_successes_hint.append((env.state, pass_, improv))
                            recent_success_job_counter = finished_jobs
                            if best_success_improv is None or self.get_state_comparison_key(env.state, improv) < self.get_state_comparison_key(best_success_env.state, best_success_improv):
                                best_success_env = env
                                best_success_pass = pass_
                                best_success_improv = improv
                                best_success_when = time.monotonic()
                                best_success_job_counter = finished_jobs
                                schedule_rmfolder(self.tmp_for_best)
                                self.tmp_for_best = tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}best-')
                                pa = os.path.join(self.tmp_for_best, os.path.basename(future.result().test_case_path))
                                logging.debug(f'run_parallel_tests: rename from {future.result().test_case_path} to {pa}')
                                os.rename(future.result().test_case_path, pa)
                                best_success_env.test_case = pa
                            self.release_future(future)
                        self.current_pass = None

                if finished_jobs >= self.parallel_tests and success_cnt > 0:
                    mean = statistics.mean(finished_job_improves)
                    sigma = statistics.stdev(finished_job_improves) if min(finished_job_improves) != max(finished_job_improves) else None
                    time_now = time.monotonic()
                    duration_till_now = time_now - start_time
                    duration_till_best = best_success_when - start_time
                    k = (duration_till_now / duration_till_best * best_success_improv - mean) / sigma if sigma else None
                    prob = math.floor(((finished_jobs - 1) / k**2 + 1) * (finished_jobs + 1) / finished_jobs) / (finished_jobs + 1) if sigma and k > 1 else None
                    # logging.info(f'run_parallel_tests: prob={prob} finished_jobs={finished_jobs} max={best_success_improv} mean={mean} sigma={sigma} duration_till_now={duration_till_now} duration_till_best={duration_till_best} k={k}')
                    # if (k > 1 and prob < 0.01 and finished_jobs - best_success_job_counter >= 2 * self.parallel_tests or
                    #     order > self.parallel_tests * 10):
                    if finished_jobs - recent_success_job_counter >= 2 * self.parallel_tests or finished_jobs - best_success_job_counter >= 4 * self.parallel_tests or finished_jobs > self.parallel_tests * 10:
                        logging.info(f'run_parallel_tests: proceeding: finished_jobs={finished_jobs} best_success_job_counter={best_success_job_counter} order={order} improv={best_success_improv} is_regular_iteration={best_success_env.is_regular_iteration} from pass={best_success_pass} state={best_success_env.state} strategy={self.strategy} comparison_key={self.get_state_comparison_key(best_success_env.state, best_success_improv)}')
                        for pass_id, state in dict((fu.pass_id, fu.state)
                                                for fu in sorted(self.futures, key=lambda fu: -fu.order)
                                                if not isinstance(fu.pass_id, list) and
                                                   fu.order > (self.last_finished_order[fu.pass_id] or 0)).items():
                            # logging.info(f'run_parallel_tests: rewinding {passes[pass_id]} from {self.states[pass_id]} to {state}')
                            self.states[pass_id] = state
                        self.terminate_all(pool)
                        return best_success_env

                merge_cands = [(sta, pa, imp) for sta, pa, imp in self.next_successes_hint if hasattr(pa, 'supports_merging') and pa.supports_merging() and not isinstance(sta, list)]
                merge_train = []
                for sta, pa, imp in sorted(merge_cands, key=lambda item: self.get_state_comparison_key(item[0], item[2])):
                    boardable = True
                    my_files = set(sta.split_per_file.keys())
                    for prev_sta, prev_pa, prev_imp in merge_train:
                        prev_files = set(prev_sta.split_per_file.keys())
                        if prev_pa != pa and not my_files.isdisjoint(prev_files):
                            boardable = False
                    if boardable:
                        merge_train.append((sta, pa, imp))
                merge_improv = sum(imp for sta, pa, imp in merge_train)
                logging.debug(f'run_parallel_tests: merge_train={merge_train} merge_improv={merge_improv} in_attempted={merge_improv in attempted_merges}')
                state = None
                if len(merge_train) >= 2 and merge_improv > 0 and merge_improv not in attempted_merges:
                    pass_id = list(sorted(set(passes.index(pa) for sta, pa, imp in merge_train)))
                    pass_ = [passes[i] if i in pass_id else None for i in range(len(passes))]
                    path_pass_state_tuples = []
                    for sta, pa, imp in merge_train:
                        for path, substate in sta.split_per_file.items():
                            path_pass_state_tuples.append((path, passes.index(pa), substate))
                    merged_state = MergedState(path_pass_state_tuples)
                    if (best_success_improv is None or
                        self.get_state_comparison_key(merged_state, merge_improv) <
                        self.get_state_comparison_key(best_success_env.state, best_success_improv)):
                        logging.debug(f'attempting merge state={merged_state} merge_improv={merge_improv} comparison_key={self.get_state_comparison_key(merged_state, merge_improv)}')
                        state = merged_state
                        attempted_merges.add(merge_improv)
                        should_advance = False
                if not state:
                    pass_job_index += 1
                    while pass_job_index >= passes[next_pass_to_schedule].jobs or not self.states[next_pass_to_schedule] or passes[next_pass_to_schedule].strategy and passes[next_pass_to_schedule].strategy != self.strategy:
                        pass_job_index = 0
                        if next_pass_to_schedule + 1 < len(passes):
                            next_pass_to_schedule += 1
                        else:
                            next_pass_to_schedule = 0
                    pass_id = next_pass_to_schedule
                    state = self.states[pass_id]
                    should_advance = True
                    pass_ = passes[pass_id]

                tmp_parent_dir = self.roots[pass_id[0]] if isinstance(state, MergedState) else self.roots[pass_id]
                folder = Path(tempfile.mkdtemp(f'{self.TEMP_PREFIX}job{order}', dir=tmp_parent_dir))
                test_case = [self.roots[i+len(passes)] / self.current_test_case.name for i in pass_id] if isinstance(state, MergedState) else \
                    (self.roots[pass_id+len(passes)] / self.current_test_case.name)
                transform = [p.transform for p in passes] if isinstance(state, MergedState) else pass_.transform
                test_env = TestEnvironment(
                    state,
                    order,
                    self.test_script,
                    folder,
                    test_case,
                    self.test_cases,
                    transform,
                    self.pid_queue,
                )
                test_env.is_regular_iteration = should_advance
                future = pool.schedule(test_env.run, timeout=self.timeout)
                future.order = order
                future.pass_id = pass_id
                future.state = state
                self.future_to_pass[future] = pass_
                assert future not in self.temporary_folders
                self.temporary_folders[future] = folder
                self.futures.append(future)
                # self.pass_statistic.add_executed(self.current_pass)
                # on_scheduled(self.current_pass, self.total_file_size)
                order += 1
                if should_advance:
                    old_state = copy.copy(state)
                    new_path = os.path.join(self.roots[pass_id+len(passes)], os.path.basename(self.current_test_case))
                    state = pass_.advance(new_path, state)
                    # logging.info(f'advance: from {old_state} to {state} pass={pass_}')
                    self.states[pass_id] = state
                    self.last_state_hint[pass_id] = state
                    if state is None and len(passes) == 1:
                        if success_cnt > 0:
                            self.current_pass = best_success_pass
                            logging.info(f'run_parallel_tests: proceeding on end state with best: improv={best_success_improv} is_regular_iteration={best_success_env.is_regular_iteration} from pass={self.current_pass} state={best_success_env.state}')
                            self.terminate_all(pool)
                            return best_success_env
                        else:
                            success = self.wait_for_first_success()
                            logging.info(f'run_parallel_tests: proceeding on end after wait_for_first_success: state={success.state if success else None}')
                            self.terminate_all(pool)
                            return success

            # we are at the end of enumeration
            self.current_pass = best_success_pass
            self.terminate_all(pool)
            logging.debug(f'TestManager.run_parallel_tests: }}END: end of enumeration')
            return best_success_env

    def set_desired_pace(self, pace):
        global desired_pace
        desired_pace = pace

    def get_estimated_pace(self, p):
        conf_r = get_conf_interval(p)
        logging.info(f'get_estimated_pace: conf_r={conf_r} mean={ME}')
        return ME

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
        m = Manager()
        self.pid_queue = m.Queue()
        self.create_root()
        self.create_root(suffix='init')
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
                new_path = os.path.join(self.roots[1], os.path.basename(self.current_test_case))
                logging.debug(f'run_pass: copy from {self.current_test_case} to {new_path}')
                shutil.copytree(self.current_test_case, new_path, symlinks=True)
                self.states = [self.current_pass.new(new_path, self.check_sanity)]
                self.skip = False

                self.timeout_count = 0
                while self.states[0] is not None and not self.skip:
                    # print(f'TestManager.run_pass while iteration')
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
                    self.current_pass = pass_
                    self.kill_pid_queue()

                    if success_env:
                        self.process_result(success_env)
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
            self.current_passes = None
        except KeyboardInterrupt:
            logging.info('Exiting now ...')
            self.remove_root()
            sys.exit(1)

    def run_concurrent_passes(self, passes):
        logging.debug(f'run_concurrent_passes: BEGIN{{')

        self.current_pass = None
        self.current_passes = passes
        self.futures = []
        self.temporary_folders = {}
        m = Manager()
        self.pid_queue = m.Queue()
        if not self.tmp_for_best:
            self.tmp_for_best = tempfile.mkdtemp(prefix=f'{self.TEMP_PREFIX}best-')
        for i in range(2):
            for p in passes:
                self.create_root(p, 'init' if i > 0 else '')
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
                self.strategy = "size"                
                self.init_all_passes(passes)
                self.skip = False

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
                    success_env = self.run_parallel_tests(passes)
                    self.kill_pid_queue()

                    if success_env:
                        self.process_result(success_env)
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

                    self.strategy = 'topo' if self.strategy == 'size' else 'size'
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
            logging.debug(f'run_concurrent_passes: }}END')
        except KeyboardInterrupt:
            logging.info('Exiting now ...')
            self.remove_root()
            sys.exit(1)

    def init_all_passes(self, passes):
        self.states = []
        with pebble.ProcessPool(max_workers=len(passes)) as pool:
            futures = []
            for i, p in enumerate(passes):
                if not p.strategy or p.strategy == self.strategy:
                    schedule_rmfolder(self.roots[i+len(passes)])
                    self.recreate_root(i+len(passes), p=passes[i], suffix='init')
                    new_path = os.path.join(self.roots[i+len(passes)], os.path.basename(self.current_test_case))
                    env = SetupEnvironment(p, self.test_script, new_path, self.test_cases, self.save_temps,
                                           self.last_state_hint[i], strategy=self.strategy, current_test_case_origin=self.current_test_case)
                    futures.append(pool.schedule(env.execute))
                else:
                    futures.append(None)
            for i, fu in enumerate(futures):
                if fu is None:
                    self.states.append(None)
                else:
                    state = fu.result()
                    self.states.append(state)
                    self.last_state_hint[i] = state

    def process_result(self, test_env):
        if self.print_diff:
            diff_str = self.diff_files(self.current_test_case, test_env.test_case_path)
            if self.use_colordiff:
                diff_str = subprocess.check_output('colordiff', shell=True, encoding='utf8', input=diff_str)
            logging.info(diff_str)

        try:
            with tempfile.TemporaryDirectory(dir=self.current_test_case.parent) as tmp_dest:
                tmp_dest_subdir = Path(tmp_dest) / self.current_test_case.name
                logging.debug(f'process_result: copy from {test_env.test_case_path} to {tmp_dest_subdir}')
                shutil.copytree(test_env.test_case_path, tmp_dest_subdir, symlinks=True)
                os.replace(self.current_test_case, Path(tmp_dest) / 'tmp_old')
                os.replace(tmp_dest_subdir, self.current_test_case)
        except FileNotFoundError:
            raise RuntimeError(
                f"Can't find {self.current_test_case} -- did your interestingness test move it?"
            ) from None

        if len(self.states) == 1:
            self.states = [self.current_pass.advance_on_success(test_env.test_case_path, test_env.state)]
            new_path = os.path.join(self.roots[1], os.path.basename(self.current_test_case))
            logging.info(f'process_result: after advance_on_success: copy_from={test_env.test_case_path} copy_to={new_path}')
            logging.debug(f'process_result: rmtree {new_path}')
            shutil.rmtree(new_path, ignore_errors=True)
            logging.debug(f'process_result: copy from {test_env.test_case_path} to {new_path}')
            shutil.copytree(test_env.test_case_path, new_path, symlinks=True)
        # self.pass_statistic.add_success(self.current_pass)
        on_succeeded(self.current_pass, self.total_file_size)

        pct = 100 - (self.total_file_size * 100.0 / self.orig_total_file_size)
        notes = []
        notes.append(f'{round(pct, 1)}%')
        notes.append(f'{self.total_file_size} bytes')
        notes.append(f'{self.total_file_count} files')
        if self.total_line_count:
            notes.append(f'{self.total_line_count} lines')
        if len(self.test_cases) > 1:
            notes.append(str(test_env.test_case))

        logging.info('(' + ', '.join(notes) + ')')


class SetupEnvironment:
    def __init__(self, pass_, test_script, test_case, test_cases, save_temps, last_state_hint, strategy, current_test_case_origin):
        self.pass_ = pass_
        self.test_script = test_script
        self.test_case = test_case
        self.test_cases = test_cases
        self.save_temps = save_temps
        self.last_state_hint = last_state_hint
        self.strategy = strategy
        self.current_test_case_origin = current_test_case_origin
    
    def execute(self):
        logging.debug(f'SetupEnvironment.execute: copy from {self.current_test_case_origin} to {self.test_case}')
        shutil.copytree(self.current_test_case_origin, self.test_case, symlinks=True)
        return self.pass_.new(self.test_case, self.check_sanity, last_state_hint=self.last_state_hint, strategy=self.strategy)

    def check_sanity(self, verbose=False):
        logging.debug('perform sanity check... ')

        folder = Path(tempfile.mkdtemp(prefix=f'{TestManager.TEMP_PREFIX}sanity-'))
        test_env = TestEnvironment(None, 0, self.test_script, folder, list(self.test_cases)[0], self.test_cases, None)
        logging.debug(f'sanity check tmpdir = {test_env.folder}')

        returncode = test_env.run_test(verbose)
        if returncode == 0:
            rmfolder(folder)
            logging.debug('sanity check successful')
        else:
            if not self.save_temps:
                rmfolder(folder)
            raise InsaneTestCaseError(self.test_cases, self.test_script)
