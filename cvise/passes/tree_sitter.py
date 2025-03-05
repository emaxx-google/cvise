import collections
import copy
import logging
from pathlib import Path
import random
import re
import subprocess
import tempfile
import types

from cvise.passes.abstract import AbstractPass, BinaryState, FuzzyBinaryState, PassResult


TOOL = '/usr/local/google/home/emaxx/cvise/cvise/tree_sit/func_body_remover'

success_histories = {}


class TreeSitterPass(AbstractPass):
    def __repr__(self):
        s = super().__repr__()
        if self.strategy is not None:
            s += f' (strategy {self.strategy})'
        return s

    def check_prerequisites(self):
        return True

    def supports_merging(self):
        return True

    def get_success_history(self, strategy):
        key = f'{self} {strategy}'
        return success_histories.setdefault(key, collections.deque(maxlen=300))

    def new(self, test_case, check_sanity=None, last_state_hint=None, strategy=None):
        files, path_to_depth = self.get_ordered_files_list(test_case, strategy)
        if not files:
            return None
        out = subprocess.check_output([TOOL], encoding='utf-8', stderr=subprocess.STDOUT, input='\n'.join(str(p) for p in files))
        s = [s.strip() for s in out.splitlines() if 'Total instances: ' in s][0]
        instances = int(s.split()[2])
        if last_state_hint:
            state = FuzzyBinaryState.create_from_hint(instances, last_state_hint)
        else:
            state = FuzzyBinaryState.create(instances)
            if state:
                state.chunk = min(state.chunk, 500)
        if state:
            state.success_history = self.get_success_history(strategy)
            state.strategy = strategy
        while state and strategy == 'topo' and state.tp == 0:
            state = state.advance(strategy)
        if state and state.tp == 1:
            state.rnd_chunk = min(state.rnd_chunk, 500)
        logging.debug(f'TreeSitterPass.new: state={state} instances={instances} stdout="{out.strip()}"')
        return state

    def advance(self, test_case, state):
        new = state.advance(state.strategy)
        logging.debug(f'TreeSitterPass.advance: old={state} new={new}')
        if new:
            new.strategy = state.strategy
        while new and new.strategy == 'topo' and new.tp == 0:
            new = new.advance(new.strategy)
        if new and new.tp == 1:
            new.rnd_chunk = min(new.rnd_chunk, 500)
        return new
    
    def advance_on_success(self, test_case, state):
        if not isinstance(state, list):
            self.get_success_history(state.strategy).append(state.end() - state.begin())
        return state
    
    def on_success_observed(self, state):
        if not isinstance(state, list):
            self.get_success_history(state.strategy).append(state.end() - state.begin())

    def merge_segments(self, segments):
        result = []
        for le, ri in sorted(segments):
            if result and result[-1][1] >= le:
                result[-1] = (result[-1][0], max(result[-1][1], ri))
            else:
                result.append((le, ri))
        return result

    def transform(self, test_case, state, process_event_notifier):
        logging.debug(f'TreeSitterPass.transform: test_case={test_case} state={state}')
        state_list = copy.copy(state) if isinstance(state, list) else [state]
        if not isinstance(state, list):
            state.split_per_file = {}

        files, path_to_depth = self.get_ordered_files_list(test_case, state_list[0].strategy)
        path_to_size = dict((p, p.stat().st_size) for p in files)
        max_depth = max(path_to_depth.values())

        segments = [(s.begin(), s.end()) for s in state_list]
        for le, ri in reversed(self.merge_segments(segments)):
            cmd = [TOOL, str(le+1), str(ri)]
            files_for_tool = files
            if hasattr(state_list[0], 'file_id'):
                files_for_tool = [test_case / state_list[0].file_id]
            logging.debug(f'TreeSitterPass.transform: running tool for {le+1}..{ri} for files_count={len(files_for_tool)}')
            with tempfile.NamedTemporaryFile('wt') as fs:
                fs.writelines([str(p)+'\n' for p in files_for_tool])
                fs.flush()
                fs.seek(0)
                out = subprocess.check_output(cmd, encoding='utf-8', stdin=fs, stderr=subprocess.STDOUT)
                best_dbg = None
                sum_of_splits = 0
                dbg_out = ' '.join(l.strip() for l in out.splitlines())
                logging.debug(f'TreeSitterPass.transform: out={dbg_out}')
                for l in out.splitlines():
                    m = re.match(r'editing (.*): old size ([0-9]+) new size ([0-9]+) instance shift ([0-9]+) instance count ([0-9]+)', l)
                    if m:
                        improv = int(m[2]) - int(m[3])
                        if not isinstance(state, list):
                            path = m[1]
                            if path.startswith('"') and path.endswith('"'):
                                path = path[1:-1]
                            path = Path(path)
                            rel_path = path.relative_to(test_case)
                            instance_shift = int(m[4])
                            instance_count = int(m[5])
                            state_for_file = BinaryState.create(instance_count)
                            state_for_file.file_id = rel_path
                            state_for_file.index = max(0, state.begin() - instance_shift)
                            state_for_file.chunk = min(instance_count, state.end() - instance_shift) - state_for_file.index
                            state_for_file.strategy = state.strategy
                            improv_per_depth = [0] * (2 + max_depth)
                            d = path_to_depth.get(path.resolve(), max_depth + 1)
                            improv_per_depth[d] = path_to_size[path] - path.stat().st_size
                            state_for_file.improv_per_depth = improv_per_depth
                            assert state_for_file.chunk > 0, f'state={state} l={l.strip()}'
                            sum_of_splits += state_for_file.chunk
                            state.split_per_file[rel_path] = state_for_file
                        if best_dbg is None or improv > best_dbg:
                            best_dbg = improv
                            for s in state_list:
                                if le <= s.begin() and s.end() <= ri:
                                    s.dbg_file = l.strip()
                if not isinstance(state, list):
                    assert sum_of_splits == ri - le, f'le={le} ri={ri} sum_of_splits={sum_of_splits} state={state}'

        improv_per_depth = [0] * (2 + max_depth)
        for p in files:
            d = path_to_depth.get(p.resolve(), max_depth + 1)
            improv_per_depth[d] += path_to_size[p] - p.stat().st_size
        for s in state_list:
            s.improv_per_depth = improv_per_depth

        return (PassResult.OK, state)

    def get_ordered_files_list(self, test_case, strategy):
        test_case = Path(test_case)
        if not test_case.is_dir():
            return [test_case], {test_case: 0}
        
        with open(Path(test_case) / 'target.makefile') as f:
            lines = f.readlines()
            for i, l in enumerate(lines):
                if '.o:' in l:
                    orig_command = lines[i+1].strip()
                    break
            else:
                raise RuntimeError("compile command not found in makefile")
            
        root_file = next(Path(test_case).rglob('*.cc'))
        orig_command = re.sub(r'\S*-fmodule\S*', '', orig_command).split()
        command = [
            '/usr/local/google/home/emaxx/clang-toys/calc-include-depth/calc-include-depth',
            root_file,
            '--',
            '-resource-dir=third_party/crosstool/v18/stable/toolchain/lib/clang/google3-trunk'] + orig_command
        path_and_depth = []
        out = subprocess.check_output(command, cwd=test_case, stderr=subprocess.DEVNULL, encoding='utf-8')
        for line in out.splitlines():
            if not line.strip():
                continue
            path, depth = line.rsplit(maxsplit=1)
            path = Path(path)
            if not path.is_absolute():
                path = Path(test_case) / path
            assert path.exists(), f'doesnt exist: {path}'
            path_and_depth.append((path.resolve(), int(depth)))
        path_to_depth = dict(path_and_depth)
        if not path_to_depth:
            path_to_depth[root_file] = 0

        max_depth = max(path_to_depth.values())
        depth_threshold = (max_depth + 1) // 2 if strategy == 'topo' else 1E10

        files = [f for f in Path(test_case).rglob('*')
                 if not f.is_dir() and not f.is_symlink() and f.name != 'target.makefile' and f.suffix != '.txt' and f.suffix != '.cppmap'
                    and path_to_depth.get(f.resolve(), 1E9) <= depth_threshold]
        files.sort(key=lambda f: (path_to_depth.get(f.resolve(), 1E9) if strategy == 'topo' else 0, f.suffix != '.cc', f))

        return files, path_to_depth
