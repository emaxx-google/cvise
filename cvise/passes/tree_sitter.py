import collections
import copy
import logging
from pathlib import Path
import random
import re
import subprocess
import tempfile
import types

from cvise.passes.abstract import AbstractPass, FuzzyBinaryState, PassResult


TOOL = '/usr/local/google/home/emaxx/cvise/cvise/tree_sit/func_body_remover'
TOPO_BIAS = 3

success_histories = {}


class TreeSitterPass(AbstractPass):
    def check_prerequisites(self):
        return True

    def supports_merging(self):
        return "tree_sitter"

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
            state.success_history = self.get_success_history(strategy)
            state.strategy = strategy
        # logging.info(f'TreeSitterPass.new: state={state} instances={instances} stdout="{out}"')
        return state

    def advance(self, test_case, state):
        new = state.advance()
        if new and new.tp == 1 and state.strategy == 'topo':
            for _ in range(TOPO_BIAS-1):
                another = state.advance()
                if another and another.begin() < new.begin():
                    new = another
        # logging.info(f'TreeSitterPass.advance: old={state} new={new}')
        if new:
            new.strategy = state.strategy
        return new
    
    def advance_on_success(self, test_case, state):
        if not isinstance(state, list):
            self.get_success_history(state.strategy).append(state.end() - state.begin())
        return state
    
    def on_success_observed(self, state):
        if not isinstance(state, list):
            self.get_success_history(state.strategy).append(state.end() - state.begin())

    def transform(self, test_case, state, process_event_notifier):
        # logging.info(f'TreeSitterPass.transform: state={state}')
        state_list = copy.copy(state) if isinstance(state, list) else [state]

        files, path_to_depth = self.get_ordered_files_list(test_case, state_list[0].strategy)
        path_to_size = dict((p, p.stat().st_size) for p in files)

        state_list.sort(key=lambda s: -s.begin())

        for s in state_list:
            cmd = [TOOL, str(s.begin()+1), str(s.end())]
            with tempfile.NamedTemporaryFile('wt') as fs:
                fs.writelines([str(p)+'\n' for p in files])
                fs.seek(0)
                out = subprocess.check_output(cmd, encoding='utf-8', stdin=fs, stderr=subprocess.STDOUT)
            s.dbg_file = out.strip()[-100:]

        max_depth = max(path_to_depth.values())
        improv_per_depth = [0] * (2 + max_depth)
        for p in files:
            d = path_to_depth.get(p, max_depth + 1)
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
        files.sort(key=lambda f: (path_to_depth.get(f.resolve(), 1E9) if strategy == 'topo' else 0, f.suffix != '.cc', f))
        return files, path_to_depth
