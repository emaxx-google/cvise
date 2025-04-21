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
        dbg = f', file={ self.file_id}' if hasattr(self, 'file_id') else ''
        return f'BinaryState({self.index}-{self.end()}, {self.instances} instances, step: {self.chunk}{dbg})'

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
            if self.chunk <= 1:
                return None
            self.chunk = int((self.chunk + 1) / 2)
            # logging.debug(f'granularity reduced to {self.chunk}')
            self.index = 0
        else:
            # logging.debug(f'***ADVANCE*** to {self}')
            pass
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


class FuzzyBinaryState(BinaryState):
    def __repr__(self):
        return f'FuzzyBinaryState(chunk={self.chunk} index={self.index} instances={self.instances} tp={self.tp} rnd_index={self.rnd_index} rnd_chunk={self.rnd_chunk} dbg_file={self.dbg_file})'

    @staticmethod
    def create(instances, pass_repr='', start_small=False):
        if not instances:
            return None
        self = FuzzyBinaryState()
        self.instances = instances
        self.chunk = 1 if start_small else self.choose_initial_chunk(instances)
        self.index = 0
        self.tp = 0
        self.rnd_index = None
        self.rnd_chunk = None
        self.rnd_depth = None
        self.dbg_file = None
        self.pass_repr = pass_repr
        self.prepare_rnd_shift()
        return self

    @staticmethod
    def choose_initial_chunk(instances):
        return max(1, instances)

    @staticmethod
    def create_from_hint(instances, last_state_hint):
        self = copy.copy(last_state_hint)
        if instances is not None:
            self.instances = instances
        if self.index >= self.instances:
            self.index = 0
        if self.chunk > self.instances:
            self.chunk = self.instances
        self.tp = 0
        self.rnd_index = None
        self.rnd_chunk = None
        self.rnd_depth = None
        if self.rnd_shift % self.chunk or self.rnd_shift >= self.instances:
            self.prepare_rnd_shift()
        return self

    def prepare_rnd_shift(self):
        choices = (self.instances + self.chunk - 1) // self.chunk
        self.rnd_shift = random.randrange(choices) * self.chunk

    def begin(self):
        if self.tp == 0:
            return (self.index + self.rnd_shift) % self.instances
        else:
            return self.rnd_index

    def end(self):
        if self.tp == 0:
            return min(self.begin() + self.chunk, self.instances)
        else:
            return self.rnd_index + self.rnd_chunk

    def real_chunk(self):
        if self.tp == 0:
            return self.end() - self.begin()
        else:
            return self.rnd_chunk

    def get_success_history(self, success_histories):
        key = f'{self.pass_repr} {self.rnd_depth}'
        if key not in success_histories:
            success_histories[key] = collections.deque(maxlen=300)
        return success_histories[key]

    def advance(self, success_histories):
        state = copy.copy(self)
        state.dbg_file = None
        if state.tp == 0 and state.chunk < state.instances:
            state.tp += 1
            state.prepare_rnd_step(success_histories)
        else:
            bi = super().advance()
            if not bi:
                return None
            state.index = bi.index
            chunk_changed = state.chunk != bi.chunk
            state.chunk = bi.chunk
            state.tp = 0
            state.rnd_index = None
            state.rnd_chunk = None
            state.rnd_depth = None
            if chunk_changed:
                state.prepare_rnd_shift()
        self.get_success_history(success_histories).append(0)
        # logging.debug(f'***ADVANCE*** to {state}')
        return state

    def advance_on_success(self, instances):
        assert False, 'not implemented'

    def choose_rnd_peak(self, success_history):
        if not success_history:
            return None
        return max(success_history)

    def prepare_rnd_step(self, success_histories):
        self.rnd_chunk = None
        self.rnd_depth = None
        peak = self.choose_rnd_peak(self.get_success_history(success_histories))
        if peak is None or peak == 0:
            peak = 1
        chunk_le = 1
        chunk_ri = self.instances
        peak = max(peak, chunk_le)
        peak = min(peak, chunk_ri)
        while self.rnd_chunk is None or self.rnd_chunk < chunk_le or self.rnd_chunk > chunk_ri:
            self.rnd_chunk = round(random.gauss(peak, peak))
        assert self.rnd_chunk > 0
        pos_le = 0
        pos_ri = self.instances - self.rnd_chunk
        self.rnd_index = random.randint(pos_le, pos_ri)


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

    def run_process(self, cmd, stdout=None, stderr=None, shell=False, need_output=True):
        if shell:
            assert isinstance(cmd, str)
        if stdout is None:
            stdout = subprocess.PIPE if need_output else subprocess.DEVNULL
        if stderr is None:
            stderr = subprocess.PIPE if need_output else subprocess.DEVNULL
        universal_newlines = stdout != subprocess.DEVNULL or stderr != subprocess.DEVNULL
        encoding = 'utf8' if stdout != subprocess.DEVNULL or stderr != subprocess.DEVNULL else None
        proc = subprocess.Popen(
            cmd,
            stdout=stdout,
            stderr=stderr,
            universal_newlines=universal_newlines,
            encoding=encoding,
            shell=shell,
        )
        # if self.pid_queue:
        #     self.pid_queue.put(ProcessEvent(proc.pid, ProcessEventType.STARTED))
        stdout, stderr = proc.communicate()
        # if self.pid_queue:
        #     self.pid_queue.put(ProcessEvent(proc.pid, ProcessEventType.FINISHED))
        return (stdout, stderr, proc.returncode)
