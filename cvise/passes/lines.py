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
    def check_prerequisites(self):
        return self.check_external_program('topformflat')

    def supports_merging(self):
        return True
    
    def choose_rnd_file(self, files, path_to_depth, strategy):
        RND_BIAS = 2
        if strategy == "size":
            order = copy.copy(files)
            order.sort(key=lambda p: (-p.stat().st_size, p))
            choice_idx = min(random.randrange(len(files)) for _ in range(RND_BIAS))
            choice = order[choice_idx]
        elif strategy == "topo":
            depths = list(set(path_to_depth.values()))
            depth_idx = min(random.randrange(len(depths)) for _ in range(RND_BIAS))
            depth_choice = depths[depth_idx]
            choice = random.choice([f for f, d in path_to_depth.items() if d == depth_choice])
        else:
            assert False, f'unknown strategy {strategy}'
        return files.index(choice)
    
    def get_success_history(self, strategy):
        key = f'{self} {strategy}'
        return success_histories.setdefault(key, collections.deque(maxlen=300))

    def choose_rnd_chunk(self, file, lines, success_history):
        if success_history and max(success_history) > 0:
            peak = max(success_history)
        else:
            peak = len(lines)
        le = 1
        ri = len(lines)
        peak = max(peak, le)
        peak = min(peak, ri)
        chunk = None
        while chunk is None or chunk < le or chunk > ri:
            chunk = round(random.gauss(peak, peak))

        begin = random.randrange(len(lines) - chunk + 1)
        return begin, begin + chunk

    def new(self, test_case, check_sanity=None, last_state_hint=None, strategy=None):
        files, path_to_depth = self.get_ordered_files_list(test_case)
        if not files:
            return None
        while True:
            state = types.SimpleNamespace()
            state.counter = 1
            state.arg = self.arg
            state.strategy = strategy
            state.file_id = self.choose_rnd_file(files, path_to_depth, strategy)
            file = files[state.file_id]
            lines = self.reformat_file(file, self.arg)
            if not lines:
                continue
            state.begin, state.end = self.choose_rnd_chunk(file, lines, self.get_success_history(strategy))
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

    def advance(self, test_case, state):
        new = self.new(test_case, last_state_hint=state, strategy=state.strategy)
        if new:
            new.counter = state.counter + 1
        self.get_success_history(state.strategy).append(0)
        return new

    def advance_on_success(self, test_case, state):
        if not isinstance(state, list):
            self.get_success_history(state.strategy).append(state.end - state.begin)
        return state
    
    def on_success_observed(self, state):
        if not isinstance(state, list):
            self.get_success_history(state.strategy).append(state.end - state.begin)

    def get_ordered_files_list(self, test_case):
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
                 if not f.is_dir() and not f.is_symlink() and f.name != 'target.makefile' and f.suffix != '.txt']
        files.sort(key=lambda f: (path_to_depth.get(f.resolve(), 1E9), f.suffix != '.cc', -f.stat().st_size, f))
        return files, path_to_depth

    def transform(self, test_case, state, process_event_notifier):
        # if isinstance(state, list):
        #     logging.info(f'[{os.getpid()}] LinesPass.transform: arg={self.arg} state={state} test_case={test_case}')
        files, path_to_depth = self.get_ordered_files_list(test_case)
        max_depth = max(path_to_depth.values())

        state_list = copy.copy(state) if isinstance(state, list) else [state]
        state_list.sort(key=lambda s: (s.file_id, -s.begin))
        improv_per_depth = [0] * (2 + max_depth)
        for s in state_list:
            file = files[s.file_id] if isinstance(s.file_id, int) else (test_case / s.file_id)
            size_before = file.stat().st_size
            s.dbg_file = str(file)
            lines_orig = self.reformat_file(file, s.arg)
            lines = lines_orig[:s.begin] + lines_orig[s.end:]
            with open(file, 'w') as f:
                f.writelines(lines)
            size_after = file.stat().st_size
            depth = path_to_depth.get(file, max_depth + 1)
            improv_per_depth[depth] += size_before - size_after
            s.improv_per_depth = improv_per_depth

        if not isinstance(state, list):
            state_for_file = copy.copy(state)
            state_for_file.file_id = files[s.file_id].relative_to(test_case)
            path = files[state.file_id].relative_to(test_case)
            state.split_per_file = {path: state_for_file}

        return (PassResult.OK, state)
