import copy
import logging
import os
import shutil
import subprocess
import tempfile

from cvise.passes.abstract import AbstractPass, FuzzyBinaryState, PassResult
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.misc import CloseableTemporaryFile


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
                    return False
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
                shutil.copy(stripped_tmp_file.name, test_case)
                return True

    def __count_instances(self, test_case):
        with open(test_case) as in_file:
            lines = in_file.readlines()
            return len(lines)
        
    def supports_merging(self):
        return True

    def new(self, test_case, check_sanity=None, last_state_hint=None):
        self.bailout = False
        # None means no topformflat
        if self.arg != 'None':
            stripped_topformlat_ok = self.__format(test_case, check_sanity if not last_state_hint or not last_state_hint.stripped_topformlat_ok else None)
            if self.bailout:
                logging.warning('Skipping pass as sanity check fails for topformflat output')
                return None
        instances = self.__count_instances(test_case)
        state = None
        if last_state_hint:
            state = FuzzyBinaryState.create_from_hint(instances, last_state_hint)
            if state:
                logging.info(f'LinesPass.new: arg={self.arg} hint to start from chunk={state.chunk} index={state.index} instead of {instances}')
        if not state:
            state = FuzzyBinaryState.create(instances)
        if state and last_state_hint:
            state.success_history = last_state_hint.success_history
        if state:
            assert state.instances == instances
            state.stripped_topformlat_ok = stripped_topformlat_ok
        # logging.info(f'[{os.getpid()}] LinesPass.new: test_case={test_case} arg={self.arg} formatted_len={instances} state={state}')
        return state

    def advance(self, test_case, state):
        new_state = state.advance()
        if new_state:
            new_state.stripped_topformlat_ok = state.stripped_topformlat_ok
        return new_state

    def advance_on_success(self, test_case, state):
        if not isinstance(state, FuzzyBinaryState):
            state = state[-1]
        old = copy.copy(state)
        state = state.advance_on_success(self.__count_instances(test_case))
        if state:
            state.stripped_topformlat_ok = old.stripped_topformlat_ok
        logging.info(f'LinesPass.advance_on_success: delta={old.instances-state.instances} chunk={old.chunk if old.tp==0 else old.rnd_chunk} tp={old.tp}')
        return state

    def transform(self, test_case, state, process_event_notifier):
        with open(test_case) as in_file:
            data = in_file.readlines()
        # logging.info(f'[{os.getpid()}] LinesPass.transform: arg={self.arg} state={state} len={len(data)}')

        old_len = len(data)
        if isinstance(state, FuzzyBinaryState):
            assert state.begin() < state.instances
            assert len(data) == state.instances
            data = data[0 : state.begin()] + data[state.end() :]
        else:
            for s in state:
                assert isinstance(s, FuzzyBinaryState)
                assert len(data) == s.instances
            segs = list(sorted([(s.begin(), s.end()) for s in state]))
            segs.append((len(data), len(data)))
            ndata = []
            prevend = 0
            for s in segs:
                ndata += data[prevend: s[0]]
                prevend = s[1]
            data = ndata
        assert len(data) < old_len

        tmp = os.path.dirname(test_case)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=tmp) as tmp_file:
            tmp_file.writelines(data)

        shutil.move(tmp_file.name, test_case)

        return (PassResult.OK, state)
