import collections
import copy
import logging
from pathlib import Path
import random
import re
import subprocess
import types

from cvise.passes.abstract import AbstractPass, FuzzyBinaryState, PassResult


TOOL = '~/cvise/cvise/tree_sit/func_body_remover'

success_histories = {}


class TreeSitterPass(AbstractPass):
    def check_prerequisites(self):
        return True

    def supports_merging(self):
        return "tree_sitter"
    
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
    
    def choose_rnd_chunk(self, instances, success_history):
        if success_history and max(success_history) > 0:
            peak = max(success_history)
        else:
            peak = instances
        le = 1
        ri = instances
        peak = max(peak, le)
        peak = min(peak, ri)
        chunk = None
        while chunk is None or chunk < le or chunk > ri:
            chunk = round(random.gauss(peak, peak))

        begin = random.randrange(instances - chunk + 1)
        return begin, begin + chunk

    def new(self, test_case, check_sanity=None, last_state_hint=None, strategy=None):
        files, path_to_depth = self.get_ordered_files_list(test_case)
        if not files:
            return None
        while True:
            file_id = self.choose_rnd_file(files, path_to_depth, strategy)
            file = files[file_id]
            sz = file.stat().st_size
            cmd = f'{TOOL} {file}'
            out = subprocess.check_output(cmd, shell=True, encoding='utf-8', stderr=subprocess.STDOUT)
            s = [s.strip() for s in out.splitlines() if 'Total instances: ' in s][0]
            instances = int(s.split()[2])
            if not instances:
                continue
            new_sz = file.stat().st_size
            assert sz == new_sz, f'sz={sz} new_sz={new_sz}'
            state = types.SimpleNamespace()
            state.counter = 1
            state.arg = self.arg
            state.strategy = strategy
            state.file = file.relative_to(test_case)
            state.begin, state.end = self.choose_rnd_chunk(instances, self.get_success_history(strategy))
            state.max_depth = max(path_to_depth.values())
            state.depth = path_to_depth.get(file, state.max_depth+1)
            state.instances = instances
            # logging.info(f'TreeSitterPass.new: state={state} instances={instances} file={file} size={sz} cmd="{cmd}" stdout="{out}"')
            return state

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

    def transform(self, test_case, state, process_event_notifier):
        # logging.info(f'TreeSitterPass.transform: state={state}')
        state_list = copy.copy(state) if isinstance(state, list) else [state]
        state_list.sort(key=lambda s: (s.file, -s.begin))
        improv_per_depth = [0] * (2 + state_list[0].max_depth)

        for s in state_list:
            file = test_case / s.file
            old_size = file.stat().st_size

            cmd = f'{TOOL} {file} {s.begin+1} {s.end}'
            proc = subprocess.Popen(cmd, shell=True, encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = proc.communicate()
            if proc.returncode:
                raise RuntimeError(f'Failed: stdout:\n{out}\nstderr:\n{err}')

            new_size = file.stat().st_size
            improv_per_depth[s.depth] = old_size - new_size
            s.improv_per_depth = improv_per_depth

        return (PassResult.OK, state)

    def get_ordered_files_list(self, test_case):
        test_case = Path(test_case)
        if not test_case.is_dir():
            return [test_case], {test_case: 0}
        
        with open(Path(test_case) / 'target.makefile') as f:
            lines = f.readlines()
            for i, l in enumerate(lines):
                if '.o:' in l:
                    command = lines[i+1].strip()
                    break
            else:
                raise RuntimeError("compile command not found in makefile")
            
        root_file = next(Path(test_case).rglob('*.cc'))
        command = re.sub(r'\S*-fmodule\S*', '', command)
        command = f'~/clang-toys/calc-include-depth/calc-include-depth {root_file} -- {command} -resource-dir=third_party/crosstool/v18/stable/toolchain/lib/clang/google3-trunk'
        path_and_depth = []
        out = subprocess.check_output(command, shell=True, cwd=test_case, stderr=subprocess.DEVNULL, encoding='utf-8')
        for line in out.splitlines():
            path, depth = line.split()
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
        files.sort(key=lambda f: (path_to_depth.get(f.resolve(), 1E9), f.suffix != '.cc', -f.stat().st_size, f))
        return files, path_to_depth
