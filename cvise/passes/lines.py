import collections
import copy
import logging
import os
import psutil
import random
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
                    return
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
                        return
                shutil.copy(backup.name, test_case)
                # if we are not the first lines pass, we should bail out
                if self.arg != '0':
                    self.bailout = True
            else:
                shutil.copy(stripped_tmp_file.name, test_case)

    def __count_instances(self, test_case):
        with open(test_case) as in_file:
            lines = in_file.readlines()
            return len(lines)

    def new(self, test_case, check_sanity=None, last_state_hint=None, successes_hint=None):
        self.bailout = False
        # None means no topformflat
        if self.arg != 'None':
            self.__format(test_case, check_sanity)
            if self.bailout:
                logging.warning('Skipping pass as sanity check fails for topformflat output')
                return None
        instances = self.__count_instances(test_case)
        # logging.info(f'[{os.getpid()}] LinesPass.new: test_case={test_case} arg={self.arg} formatted_len={instances}')
        state = FuzzyBinaryState.create(instances)
        hint_from_last = last_state_hint.chunk if last_state_hint else None
        hint_from_successes = max([s.real_chunk() for s in successes_hint], default=None) if successes_hint else None
        hint = max(list(filter(None, [hint_from_last, hint_from_successes])), default=None)
        if state and last_state_hint:
            state.success_history = last_state_hint.success_history
        if state and hint and hint < state.instances:
            logging.info(f'LinesPass.new: arg={self.arg} hint to start from chunk={hint} instead of {state.chunk} hint_from_last={hint_from_last} hint_from_successes={hint_from_successes}')
            state.chunk = hint
        return state

    def advance(self, test_case, state):
        return state.advance()

    def advance_on_success(self, test_case, state):
        old = copy.copy(state)
        state = state.advance_on_success(self.__count_instances(test_case))
        logging.info(f'LinesPass.advance_on_success: delta={old.instances-state.instances} chunk={old.chunk if old.tp==0 else old.rnd_chunk} tp={old.tp}')
        return state

    def transform(self, test_case, state, process_event_notifier):
        with open(test_case) as in_file:
            data = in_file.readlines()
        # logging.info(f'[{os.getpid()}] LinesPass.transform: test_case={test_case} arg={self.arg} state={state} len={len(data)}')
        # logging.info(f'[{os.getpid()}] LinesPass.transform: read')

        old_len = len(data)
        if state.tp == 0:
            # logging.info(f'state={state} index={state.index} end={state.end()}')
            if state.end() <= state.index:
                logging.info(f'state={state}')
                assert False
            data = data[0 : state.index] + data[state.end() :]
        else:
            if state.rnd_chunk > state.instances:
                logging.info(f'state={state}')
                assert False
            start = random.randint(0, state.instances - state.rnd_chunk)
            assert state.rnd_chunk > 0
            # logging.info(f'state={state} start={start} rnd_chunk={state.rnd_chunk}')
            data = data[0 : start] + data[start + state.rnd_chunk :]
        if len(data) >= old_len:
            logging.info(f'state={state}')
            assert False
        # logging.info(f'[{os.getpid()}] LinesPass.transform: copied')

        tmp = os.path.dirname(test_case)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=tmp) as tmp_file:
            tmp_file.writelines(data)
        # logging.info(f'[{os.getpid()}] LinesPass.transform: wrote')

        shutil.move(tmp_file.name, test_case)

        # logging.info(f'[{os.getpid()}] LinesPass.transform: }}END')
        return (PassResult.OK, state)
