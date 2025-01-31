import copy
import collections
from enum import auto, Enum, unique
import logging
import math
import psutil
import random
import shutil
import subprocess


@unique
class PassResult(Enum):
    OK = auto()
    INVALID = auto()
    STOP = auto()
    ERROR = auto()


class BinaryState:
    def __init__(self):
        pass

    def __repr__(self):
        return f'BinaryState({self.index}-{self.end()}, {self.instances} instances, step: {self.chunk})'

    @staticmethod
    def create(instances):
        if not instances:
            return None
        self = BinaryState()
        self.instances = instances
        self.chunk = instances
        self.index = 0
        return self

    def copy(self):
        return copy.copy(self)
    
    def begin(self):
        return self.index

    def end(self):
        return min(self.index + self.chunk, self.instances)

    def real_chunk(self):
        return self.end() - self.index

    def advance(self):
        self = self.copy()
        self.index += self.chunk
        if self.index >= self.instances:
            self.chunk = int(self.chunk / 2)
            if self.chunk < 1:
                return None
            logging.debug(f'granularity reduced to {self.chunk}')
            self.index = 0
        else:
            logging.debug(f'***ADVANCE*** to {self}')
        return self

    def advance_on_success(self, instances):
        if not instances:
            return None
        self.instances = instances
        if self.index >= self.instances:
            return self.advance()
        else:
            return self


class AbstractPass:
    @unique
    class Option(Enum):
        slow = 'slow'
        windows = 'windows'

    def __init__(self, arg=None, external_programs=None):
        self.external_programs = external_programs
        self.arg = arg
        self.min_transforms = None

    def __repr__(self):
        if self.arg is not None:
            name = f'{type(self).__name__}::{self.arg}'
        else:
            name = f'{type(self).__name__}'

        if self.max_transforms is not None:
            name += f' ({self.max_transforms} T)'
        return name

    def check_external_program(self, name):
        program = self.external_programs[name]
        if not program:
            return False
        result = shutil.which(program) is not None
        if not result:
            logging.error(f'cannot find external program {name}')
        return result

    def check_prerequisites(self):
        raise NotImplementedError(f"Class {type(self).__name__} has not implemented 'check_prerequisites'!")

    def new(self, test_case, check_sanity):
        raise NotImplementedError(f"Class {type(self).__name__} has not implemented 'new'!")

    def advance(self, test_case, state):
        raise NotImplementedError(f"Class {type(self).__name__} has not implemented 'advance'!")

    def advance_on_success(self, test_case, state):
        raise NotImplementedError(f"Class {type(self).__name__} has not implemented 'advance_on_success'!")

    def transform(self, test_case, state, process_event_notifier):
        raise NotImplementedError(f"Class {type(self).__name__} has not implemented 'transform'!")

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

class FuzzyBinaryState(BinaryState):
    def __repr__(self):
        return f'FuzzyBinaryState(chunk={self.chunk} index={self.index} instances={self.instances} tp={self.tp} rnd_index={self.rnd_index} rnd_chunk={self.rnd_chunk})'

    @staticmethod
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
        #
        # TODO: doubling because half of runs are non-random.
        return 15 * CORES * 2

    @staticmethod
    def create(instances):
        # global success_history
        # success_history = collections.deque(maxlen=FuzzyBinaryState.choose_success_history_size())

        if not instances:
            return None
        self = FuzzyBinaryState()
        self.instances = instances
        self.chunk = instances
        self.index = 0
        self.tp = 0
        self.rnd_index = None
        self.rnd_chunk = None
        self.success_history = collections.deque(maxlen=FuzzyBinaryState.choose_success_history_size())
        return self
    
    @staticmethod
    def create_from_hint(instances, last_state_hint):
        if instances is not None and last_state_hint.chunk > instances:
            return None
        self = copy.copy(last_state_hint)
        if instances is not None:
            self.instances = instances
        if self.index >= self.instances:
            self.index = 0
        self.tp = 0
        self.rnd_index = None
        self.rnd_chunk = None
        return self
    
    def begin(self):
        if self.tp == 0:
            return super().begin()
        else:
            return self.rnd_index

    def end(self):
        if self.tp == 0:
            return super().end()
        else:
            return self.rnd_index + self.rnd_chunk

    def real_chunk(self):
        if self.tp == 0:
            return super().real_chunk()
        else:
            return self.rnd_chunk

    def advance(self):
        self.success_history.append(0)
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
        state.rnd_index = None
        state.rnd_chunk = None
        return state
    
    def advance_on_success(self, instances):
        self.success_history.append(self.instances - instances)
        state = copy.copy(self)
        state.instances = instances
        state.tp = 0
        state.rnd_index = None
        state.rnd_chunk = None
        if state.index >= state.instances:
            return state.advance()
        else:
            return state

    def choose_rnd_peak(self):
        if not self.success_history:
            return None
        return max(self.success_history)

    def prepare_rnd_step(self):
        if self.chunk < 1:
            logging.info(f'prepare_rnd_step: self={self}')
        assert self.chunk >= 1
        le = math.log(self.chunk)
        ri = math.log(self.instances)
        rndlog = random.uniform(le, ri)
        self.rnd_chunk = round(math.exp(rndlog))
        assert self.chunk <= self.rnd_chunk <= self.instances
        self.rnd_index = random.randint(0, self.instances - self.rnd_chunk)


@unique
class ProcessEventType(Enum):
    STARTED = auto()
    FINISHED = auto()


class ProcessEvent:
    def __init__(self, pid, event_type):
        self.pid = pid
        self.type = event_type


class ProcessEventNotifier:
    def __init__(self, pid_queue):
        self.pid_queue = pid_queue

    def run_process(self, cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False):
        if shell:
            assert isinstance(cmd, str)
        proc = subprocess.Popen(
            cmd,
            stdout=stdout,
            stderr=stderr,
            universal_newlines=True,
            encoding='utf8',
            shell=shell,
        )
        if self.pid_queue:
            self.pid_queue.put(ProcessEvent(proc.pid, ProcessEventType.STARTED))
        stdout, stderr = proc.communicate()
        if self.pid_queue:
            self.pid_queue.put(ProcessEvent(proc.pid, ProcessEventType.FINISHED))
        return (stdout, stderr, proc.returncode)
