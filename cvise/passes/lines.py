import collections
import copy
import logging
import os
from pathlib import Path
import random
import re
import shutil
import subprocess
import tempfile
import types

from cvise.passes.abstract import AbstractPass, BinaryState, FuzzyBinaryState, MultiFileFuzzyBinaryState, PassResult
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.misc import CloseableTemporaryFile


success_histories = {}


class LinesPass(AbstractPass):
    def __repr__(self):
        s = super().__repr__()
        if self.strategy is not None:
            s += f' (strategy {self.strategy})'
        return s

    def check_prerequisites(self):
        return self.check_external_program('topformflat')

    def supports_merging(self):
        return True
    
    def get_success_history(self, strategy):
        key = f'{self} {strategy}'
        return success_histories.setdefault(key, collections.deque(maxlen=300))

    def new(self, test_case, check_sanity=None, last_state_hint=None, strategy=None):
        files, path_to_depth = self.get_ordered_files_list(test_case, strategy)
        if not files:
            return None
        instances = 0
        for file in files:
            instances += self.count_instances(self.reformat_file(file, self.arg))
        if last_state_hint:
            state = FuzzyBinaryState.create_from_hint(instances, last_state_hint)
        else:
            state = FuzzyBinaryState.create(instances, strategy)
        if state:
            state.success_history = self.get_success_history(strategy)
            state.strategy = strategy
        while state and strategy == 'topo' and state.tp == 0:
            state = state.advance(strategy)
        # if state:
        #     state.chunk = min(state.chunk, 500)
        #     if state.tp == 1:
        #         state.rnd_chunk = min(state.rnd_chunk, 500)
        logging.debug(f'LinesPass.new: state={state} instances={instances}')
        return state
    
    def reformat_file(self, file, arg):
        assert arg is not None
        with open(file) as f:
            cmd = [self.external_programs['topformflat'], arg]
            with subprocess.Popen(cmd, stdin=f, stdout=subprocess.PIPE, text=True) as proc:
                lines = []
                for line in proc.stdout:
                    if not line.isspace():
                        linebreak = '\n' if line.endswith('\n') else ''
                        lines.append(line.strip() + linebreak)
        return lines
    
    def count_instances(self, lines):
        if self.arg == '10':
            return len(lines)
        return sum(not s.strip().startswith('#') for s in lines)

    def advance(self, test_case, state):
        new = state.advance(state.strategy)
        if new:
            new.strategy = state.strategy
        while new and new.strategy == 'topo' and new.tp == 0:
            new = new.advance(new.strategy)
        # if new and new.tp == 1:
        #     new.rnd_chunk = min(new.rnd_chunk, 500)
        logging.debug(f'LinesPass.advance: old={state} new={new}')
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
        logging.debug(f'LinesPass.transform: test_case={test_case} state={state}')
        state_list = copy.copy(state) if isinstance(state, list) else [state]
        if not isinstance(state, list):
            state.split_per_file = {}

        files, path_to_depth = self.get_ordered_files_list(test_case, state_list[0].strategy)
        path_to_size = dict((p, p.stat().st_size) for p in files)
        max_depth = max(path_to_depth.values())

        segments = [(s.begin(), s.end()) for s in state_list]
        dbg_files = []
        for le, ri in reversed(self.merge_segments(segments)):
            if hasattr(state_list[0], 'file_id'):
                cand_files = [test_case / state_list[0].file_id]
            else:
                cand_files = files
            for file in cand_files:
                lines = self.reformat_file(file, self.arg)
                instances = self.count_instances(lines)
                if le < instances:
                    current_le = le
                    current_ri = min(instances,ri)
                    new_lines = []
                    cnt = 0
                    for s in lines:
                        if self.arg == '10' or not s.strip().startswith('#'):
                            cnt += 1
                            if cnt <= current_le or cnt > current_ri:
                                new_lines.append(s)
                        else:
                            new_lines.append(s)
                    with open(file, 'w') as f:
                        f.writelines(new_lines)

                    path = Path(file)
                    rel_path = path.relative_to(test_case)
                    dbg_files.append(str(rel_path))

                    if not isinstance(state, list):
                        state_for_file = BinaryState.create(len(lines))
                        state_for_file.file_id = rel_path
                        state_for_file.index = current_le
                        state_for_file.chunk = current_ri - current_le
                        state_for_file.strategy = state.strategy
                        improv_per_depth = [0] * (2 + max_depth)
                        d = path_to_depth.get(path.resolve(), max_depth + 1)
                        improv_per_depth[d] = path_to_size[path] - path.stat().st_size
                        state_for_file.improv_per_depth = improv_per_depth
                        assert state_for_file.chunk > 0, f'state={state} state_for_file'
                        state.split_per_file[rel_path] = state_for_file

                le = max(0, le - instances)
                ri -= instances
                if le >= ri:
                    break
            else:
                assert False, f'jumped beyond end: le={le} ri={ri} cand_files={len(cand_files)} state={state}'

        improv_per_depth = [0] * (2 + max_depth)
        for p in files:
            d = path_to_depth.get(p.resolve(), max_depth + 1)
            improv_per_depth[d] += path_to_size[p] - p.stat().st_size
        for s in state_list:
            s.improv_per_depth = improv_per_depth
        state_list[0].dbg_file = ','.join(dbg_files)

        logging.debug(f'{self}.transform: state={state}')
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

        files = [f for f in Path(test_case).rglob('*')
                 if not f.is_dir() and not f.is_symlink() and f.name != 'target.makefile' and f.suffix != '.txt' and f.suffix != '.cppmap']
        files.sort(key=lambda f: (path_to_depth.get(f.resolve(), 1E9) if strategy == 'topo' else 0, f.suffix != '.cc', f))

        return files, path_to_depth
