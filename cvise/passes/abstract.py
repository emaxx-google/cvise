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
        self.strategy = None

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
        return f'FuzzyBinaryState(chunk={self.chunk} index={self.index} instances={self.instances} tp={self.tp} rnd_index={self.rnd_index} rnd_chunk={self.rnd_chunk} dbg_file={self.dbg_file} strategy={self.strategy if hasattr(self, "strategy") else None} improv_per_depth={self.improv_per_depth if hasattr(self, "improv_per_depth") else None})'

    @staticmethod
    def create(instances, strategy, depth_to_instances=None, pass_repr=''):
        if not instances:
            return None
        self = FuzzyBinaryState()
        self.instances = instances
        self.chunk = self.choose_initial_chunk(instances, strategy)
        self.index = 0
        self.tp = 0
        self.rnd_index = None
        self.rnd_chunk = None
        self.rnd_depth = None
        self.dbg_file = None
        self.depth_to_instances = depth_to_instances
        self.strategy = strategy
        self.pass_repr = pass_repr
        return self
    
    @staticmethod
    def choose_initial_chunk(instances, strategy):
        if strategy == 'topo':
            return 1
        elif strategy == 'size':
            return max(1, instances // 1000)
        else:
            assert False
    
    @staticmethod
    def create_from_hint(instances, strategy, last_state_hint, depth_to_instances=None):
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
        self.rnd_depth = None
        self.depth_to_instances = depth_to_instances
        self.strategy = strategy
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
        
    def get_success_history(self, success_histories):
        key = f'{self.pass_repr} {self.strategy} {self.rnd_depth}'
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
            state.chunk = bi.chunk
            state.tp = 0
            state.rnd_index = None
            state.rnd_chunk = None
            state.rnd_depth = None
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
        chunk_le = min(self.chunk, self.instances)
        chunk_ri = self.instances
        if self.strategy == 'topo':
            while True:
                self.rnd_depth = int(random.triangular(0, len(self.depth_to_instances), 0))
                instances_within_depth = sum(self.depth_to_instances[:self.rnd_depth+1])
                if instances_within_depth > 0:
                    break
            chunk_le = min(chunk_le, instances_within_depth)
            chunk_ri = min(chunk_ri, instances_within_depth)
        peak = max(peak, chunk_le)
        peak = min(peak, chunk_ri)
        while self.rnd_chunk is None or self.rnd_chunk < chunk_le or self.rnd_chunk > chunk_ri:
            self.rnd_chunk = round(random.gauss(peak, peak))
        assert self.rnd_chunk > 0
        if self.strategy == 'topo':
            pos_le = 0
            pos_ri = instances_within_depth
        else:
            pos_le = 0
            pos_ri = self.instances - self.rnd_chunk
        self.rnd_index = random.randint(pos_le, pos_ri)

class MultiFileFuzzyBinaryState(FuzzyBinaryState):
    def __repr__(self):
        return f'MultiFileFuzzyBinaryState(file_id={self.file_id} chunk={self.chunk} index={self.index} instances={self.instances} tp={self.tp} rnd_index={self.rnd_index} rnd_chunk={self.rnd_chunk} dbg_file={self.dbg_file})'

    @staticmethod
    def create(files, instances0):
        if not files:
            return None
        zigote = FuzzyBinaryState.create(instances0)
        self = MultiFileFuzzyBinaryState()
        self.__dict__.update(zigote.__dict__)
        self.file_id = 0
        return self

    def advance(self, all_files):
        in_file = super().advance()
        new = MultiFileFuzzyBinaryState()
        new.file_id = self.file_id
        while in_file is None:
            new.file_id += 1
            if new.file_id >= len(all_files):
                return None
            with open(all_files[new.file_id]) as f:
                instances = len(f.readlines())
            in_file = FuzzyBinaryState.create(instances)
        new.__dict__.update(in_file.__dict__)
        return new
    
    def advance_on_success(self, all_files):
        with open(all_files[self.file_id]) as f:
            instances = len(f.readlines())
        in_file = super().advance_on_success(instances)
        new = MultiFileFuzzyBinaryState()
        new.file_id = self.file_id
        if in_file is None:
            new.file_id += 1
            with open(all_files[new.file_id]) as f:
                instances = len(f.readlines())
            in_file = FuzzyBinaryState.create(instances)
        new.__dict__.update(in_file.__dict__)
        return new


class MergedState:
    def __init__(self, path_pass_state_tuples):
        self.path_pass_state_tuples = path_pass_state_tuples
    def __repr__(self):
        return f'MergedState({self.path_pass_state_tuples})'


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
