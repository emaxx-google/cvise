import collections
import copy
import logging
import os
import psutil
import random
import shutil
import subprocess
import tempfile

from cvise.passes.abstract import AbstractPass, BinaryState, PassResult
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.misc import CloseableTemporaryFile


previous_state = {}


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

def choose_success_history_size():
    # Chosen heuristically to make the algorithm sufficiently adaptive: to keep
    # trying around the recently observed big successful leaps, but without
    # being stuck too long if no more such successes occur.
    #
    # The formula can be treated as mostly empirical. However, some grounding
    # for it can be loosely derived from Chebyshev's inequality in a similar way
    # as the statistics' "rule of three", if we aim for the "one of the parallel
    # processes successfully improves the window's maximum" event's probability
    # to be bounded at 50% with 99% confidence. The analytical solution in this
    # model would be "10 / (1 - 0.5 ** (1 / CORES))", but we roughly
    # approximated it with this very simple formula.
    return 15 * CORES

def choose_rnd_peak():
    if not success_history:
        return None
    return max(success_history)

class LineState(BinaryState):
    def __repr__(self):
        return f'LineState(chunk={self.chunk} index={self.index} instances={self.instances}, tp={self.tp})'

    @staticmethod
    def create(instances):
        global success_history
        success_history = collections.deque(maxlen=choose_success_history_size())

        if not instances:
            return None
        self = LineState()
        self.instances = instances
        self.chunk = instances
        self.index = 0
        self.tp = 0
        self.rnd_chunk = None
        return self
    
    def advance(self):
        success_history.append(0)
        state = copy.copy(self)
        if state.tp == 0:
            state.tp += 1
            state.prepare_rnd_step()
            return state
        bi = super().advance()
        if not bi:
            return None
        state.index = bi.index
        state.chunk = bi.chunk
        state.tp = 0
        return state
    
    def advance_on_success(self, instances):
        success_history.append(self.instances - instances)
        state = copy.copy(self)
        state.instances = instances
        state.tp = 0
        if state.index >= state.instances:
            return state.advance()
        else:
            return state

    def prepare_rnd_step(self):
        self.rnd_chunk = None
        peak = choose_rnd_peak()
        if peak is None:
            peak = 1
        le = min(self.chunk, self.instances)
        ri = self.instances
        peak = max(peak, le)
        peak = min(peak, ri)
        while self.rnd_chunk is None or self.rnd_chunk < le or self.rnd_chunk > ri:
            self.rnd_chunk = round(random.gauss(peak, peak))

class LinesPass(AbstractPass):
    def check_prerequisites(self):
        return self.check_external_program('topformflat')

    def __format(self, test_case, check_sanity):
        tmp = os.path.dirname(test_case)

        with (
            CloseableTemporaryFile(mode='w+', dir=tmp) as backup,
            CloseableTemporaryFile(mode='w+', dir=tmp) as tmp_file,
            CloseableTemporaryFile(mode='w+', dir=tmp) as stripped_tmp_file,
        ):
            backup.close()
            with open(test_case) as in_file:
                try:
                    cmd = [self.external_programs['topformflat'], self.arg]
                    with subprocess.Popen(cmd, stdin=in_file, stdout=subprocess.PIPE, text=True) as proc:
                        for line in proc.stdout:
                            if not line.isspace():
                                tmp_file.write(line)
                                linebreak = '\n' if line.endswith('\n') else ''
                                stripped_tmp_file.write(line.strip() + linebreak)
                except subprocess.SubprocessError:
                    return
            tmp_file.close()
            stripped_tmp_file.close()

            # we need to check that sanity check is still fine
            if check_sanity:
                shutil.copy(test_case, backup.name)
                # try the stripped file first, fall back to the original stdout if needed
                candidates = [stripped_tmp_file.name, tmp_file.name]
                for candidate in candidates:
                    shutil.copy(candidate, test_case)
                    try:
                        check_sanity()
                    except InsaneTestCaseError:
                        # logging.info('InsaneTestCaseError')
                        pass
                    else:
                        # logging.info(f'taking {candidate} out of {candidates}')
                        return
                shutil.copy(backup.name, test_case)
                # if we are not the first lines pass, we should bail out
                if self.arg != '0':
                    self.bailout = True
            else:
                shutil.copy(tmp_file.name, test_case)

    def __count_instances(self, test_case):
        with open(test_case) as in_file:
            lines = in_file.readlines()
            return len(lines)

    def new(self, test_case, check_sanity=None):
        self.bailout = False
        # None means no topformflat
        if self.arg != 'None':
            self.__format(test_case, check_sanity)
            if self.bailout:
                logging.warning('Skipping pass as sanity check fails for topformflat output')
                return None
        instances = self.__count_instances(test_case)
        state = LineState.create(instances)
        if self.arg in previous_state and previous_state[self.arg] is not None and previous_state[self.arg].chunk <= instances:
            logging.info(f'LinesPass.new: hint to start from chunk={previous_state[self.arg].chunk} instead of {state.chunk}')
            state.chunk = previous_state[self.arg].chunk
        return state

    def advance(self, test_case, state):
        state = state.advance()
        previous_state[self.arg] = state
        return state

    def advance_on_success(self, test_case, state):
        old = copy.copy(state)
        state = state.advance_on_success(self.__count_instances(test_case))
        logging.info(f'advance_on_success: delta={old.instances-state.instances} chunk={old.chunk if old.tp==0 else old.rnd_chunk} tp={old.tp}')
        previous_state[self.arg] = state
        return state

    def transform(self, test_case, state, process_event_notifier):
        # logging.info(f'[{os.getpid()}] LinesPass.transform: BEGIN{{')
        with open(test_case) as in_file:
            data = in_file.readlines()
        # logging.info(f'[{os.getpid()}] LinesPass.transform: read')

        old_len = len(data)
        if state.tp == 0:
            # logging.info(f'state={state} index={state.index} end={state.end()}')
            if state.end() <= state.index:
                logging.info(f'state={state}')
                assert False
            data = data[0 : state.index] + data[state.end() :]
        else:
            if state.rnd_chunk > state.instances:
                logging.info(f'state={state}')
                assert False
            start = random.randint(0, state.instances - state.rnd_chunk)
            assert state.rnd_chunk > 0
            # logging.info(f'state={state} start={start} rnd_chunk={state.rnd_chunk}')
            data = data[0 : start] + data[start + state.rnd_chunk :]
        assert len(data) < old_len
        # logging.info(f'[{os.getpid()}] LinesPass.transform: copied')

        tmp = os.path.dirname(test_case)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=tmp) as tmp_file:
            tmp_file.writelines(data)
        # logging.info(f'[{os.getpid()}] LinesPass.transform: wrote')

        shutil.move(tmp_file.name, test_case)

        # logging.info(f'[{os.getpid()}] LinesPass.transform: }}END')
        return (PassResult.OK, state)
