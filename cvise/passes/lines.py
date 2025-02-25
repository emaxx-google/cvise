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

from cvise.passes.abstract import AbstractPass, FuzzyBinaryState, MultiFileFuzzyBinaryState, PassResult
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.misc import CloseableTemporaryFile


class LinesPass(AbstractPass):
    def check_prerequisites(self):
        return self.check_external_program('topformflat')

    def __format(self, test_case, check_sanity):
        with (
            tempfile.TemporaryDirectory() as backup,
            tempfile.TemporaryDirectory() as tmp_dir,
            tempfile.TemporaryDirectory() as stripped_tmp_dir,
        ):
            files = self.get_ordered_files_list(test_case)
            for in_file in files:
                filerel = os.path.relpath(in_file, start=test_case)
                with open(in_file) as file:
                    tmp_file_path = Path(tmp_dir) / filerel
                    tmp_file_path.parent.mkdir(parents=True, exist_ok=True)
                    stripped_tmp_file_path = Path(stripped_tmp_dir) / filerel
                    stripped_tmp_file_path.parent.mkdir(parents=True, exist_ok=True)
                    if in_file.name == 'target.makefile' or in_file.suffix == '.txt' or in_file.suffix == '.cppmap':
                        shutil.copy2(in_file, tmp_file_path)
                        shutil.copy2(in_file, stripped_tmp_file_path)
                        continue
                    with open(tmp_file_path, 'w') as tmp_file:
                        with open(stripped_tmp_file_path, 'w') as stripped_tmp_file:
                            try:
                                cmd = [self.external_programs['topformflat'], self.arg]
                                with subprocess.Popen(cmd, stdin=file, stdout=subprocess.PIPE, text=True) as proc:
                                    for line in proc.stdout:
                                        if not line.isspace():
                                            tmp_file.write(line)
                                            linebreak = '\n' if line.endswith('\n') else ''
                                            stripped_tmp_file.write(line.strip() + linebreak)
                            except subprocess.SubprocessError:
                                return False

            # we need to check that sanity check is still fine
            if check_sanity:
                shutil.copytree(test_case, Path(backup), symlinks=True, dirs_exist_ok=True)
                # try the stripped file first, fall back to the original stdout if needed
                candidates = [stripped_tmp_dir, tmp_dir]
                for candidate in candidates:
                    shutil.copytree(candidate, test_case, symlinks=True, dirs_exist_ok=True)
                    try:
                        check_sanity()
                    except InsaneTestCaseError:
                        logging.info('LinesPass.__format: InsaneTestCaseError')
                        pass
                    else:
                        # logging.info(f'taking {candidate} out of {candidates}')
                        return stripped_tmp_file.name == candidate
                shutil.copy(backup.name, test_case)
                # if we are not the first lines pass, we should bail out
                if self.arg != '0':
                    self.bailout = True
            else:
                shutil.copytree(stripped_tmp_dir, test_case, symlinks=True, dirs_exist_ok=True)
                return True

    def __count_instances(self, test_case):
        instances = 0
        for file in self.get_ordered_files_list(test_case):
            with open(file) as in_file:
                lines = in_file.readlines()
                instances += len(lines)
        return instances
        
    def supports_merging(self):
        return True

    def new(self, test_case, check_sanity=None, last_state_hint=None, strategy=None):
        self.bailout = False
        # None means no topformflat
        if self.arg != 'None':
            stripped_topformlat_ok = self.__format(test_case, None)
            if self.bailout:
                logging.warning('Skipping pass as sanity check fails for topformflat output')
                return None
        state = types.SimpleNamespace()
        state.id = 1
        state.strategy = strategy
        return state
        # instances = self.__count_instances(test_case)
        files = self.get_ordered_files_list(test_case)
        # logging.info(f'LinesPass.new: arg={self.arg} files={len(files)} test_case={test_case}')
        state = None
        # if last_state_hint:
        #     state = MultiFileFuzzyBinaryState.create_from_hint(instances, last_state_hint)
        #     if state:
        #         logging.info(f'LinesPass.new: arg={self.arg} hint to start from chunk={state.chunk} index={state.index} instead of {instances}')
        if not state:
            with open(files[0]) as f:
                instances0 = len(f.readlines())
            state = MultiFileFuzzyBinaryState.create(len(files), instances0)
        if state and last_state_hint:
            state.success_history = last_state_hint.success_history
        if state:
            # assert state.instances == instances
            state.stripped_topformlat_ok = stripped_topformlat_ok
        if state:
            # logging.info(f'LinesPass: files={files[:10]}')
            filesizes = []
            for file in files:
                with open(file) as f:
                    lines = f.readlines()
                    filesizes.append(len(lines))
            state.chunk = min(state.chunk, max(filesizes))
        # logging.info(f'[{os.getpid()}] LinesPass.new: test_case={test_case} arg={self.arg} state={state}')
        return state

    def advance(self, test_case, state):
        new = types.SimpleNamespace()
        new.id = state.id + 1
        new.strategy = state.strategy
        return new
        old = copy.copy(state)
        all_files = self.get_ordered_files_list(test_case)
        new_state = state.advance(all_files)
        if new_state:
            new_state.stripped_topformlat_ok = state.stripped_topformlat_ok
        # if new_state and new_state.tp == 1:
        #     filesizes = []
        #     for file in all_files:
        #         with open(file) as f:
        #             lines = f.readlines()
        #             filesizes.append(len(lines))
        #     new_state.rnd_chunk = min(new_state.rnd_chunk, max(filesizes))
        # logging.info(f'LinesPass.advance: old={old} new={new_state} test_case={test_case}')
        if new_state and old.chunk != new_state.chunk:
            # logging.info(f'LinesPass.advance: reducing granularity old={old} new={new_state}')
            pass
        if new_state:
            new_state.dbg_file = None
            new_state.dbg_before = None
            new_state.dbg_after = None
        return new_state

    def advance_on_success(self, test_case, state):
        return state
        if not isinstance(state, FuzzyBinaryState):
            state = state[-1]
        all_files = self.get_ordered_files_list(test_case)
        old = copy.copy(state)
        state = state.advance_on_success(all_files)
        if state:
            state.stripped_topformlat_ok = old.stripped_topformlat_ok
        logging.info(f'LinesPass.advance_on_success: delta={old.instances-state.instances} chunk={old.chunk if old.tp==0 else old.rnd_chunk} tp={old.tp}')
        return state

    def get_ordered_files_list(self, test_case):
        if not os.path.isdir(test_case):
            return [Path(test_case)]
        
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
        for line in subprocess.check_output(command, shell=True, cwd=test_case, stderr=subprocess.DEVNULL, encoding='utf-8').splitlines():
            path, depth = line.split()
            path = Path(path)
            if not path.is_absolute():
                path = Path(test_case) / path
            assert path.exists(), f'doesnt exist: {path}'
            path_and_depth.append((path.resolve(), int(depth)))
        path_to_depth = dict(path_and_depth)

        def find_depth(path):
            return path_to_depth.get(path.resolve(), 1E9)
            # for p, d in path_to_depth:
            #     if Path(p) == path:
            #         return d
            # return 1E9

        files = [f for f in Path(test_case).rglob('*')
                 if not f.is_dir() and not f.is_symlink() and f.name != 'target.makefile' and f.suffix != '.txt']
        files.sort(key=lambda f: (find_depth(f), f.suffix != '.cc', -f.stat().st_size, f))
        return files

    def transform(self, test_case, state, process_event_notifier):
        # logging.info(f'[{os.getpid()}] LinesPass.transform: arg={self.arg} state={state} test_case={test_case}')

        files = self.get_ordered_files_list(test_case)

        while True:
            FILE_ID_SKEW = 100
            #  if state.strategy == "topo" else 1 if state.strategy == "size" else None
            file_id = min(random.randrange(len(files)) for _ in range(FILE_ID_SKEW))
            with open(files[file_id]) as f:
                lines = f.readlines()
            if not lines:
                continue
            chunk = min(random.randint(1, len(lines)) for _ in range(2))
            begin = random.randrange(len(lines) - chunk + 1)
            data = lines[:begin] + lines[begin+chunk:]
            assert len(lines) > len(data)
            with open(files[file_id], 'w') as f:
                f.writelines(data)
            state.dbg_file = files[file_id]
            state.dbg_file_id = file_id
            state.dbg_before = len(lines)
            state.dbg_after = len(data)
            return (PassResult.OK, state)

        states = [state] if isinstance(state, MultiFileFuzzyBinaryState) else state
        for state in states:
            path = files[state.file_id]
            with open(path) as in_file:
                data = in_file.readlines()
            old_len = len(data)
            assert old_len == state.instances, f'wrong line count: got {old_len} in {path}, expected from state {state}'
            data = data[0 : state.begin()] + data[state.end():]
            assert old_len > len(data)
            with open(path, 'w') as out_file:
                out_file.writelines(data)
            state.dbg_file = path
            state.dbg_before = old_len
            state.dbg_after = len(data)

        return (PassResult.OK, state)
