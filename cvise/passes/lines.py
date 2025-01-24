import logging
import os
import shutil
import subprocess
import tempfile

from cvise.passes.abstract import AbstractPass, BinaryState, PassResult
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.misc import CloseableTemporaryFile


previous_state = {}


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
                        # logging.info('InsaneTestCaseError')
                        pass
                    else:
                        # logging.info(f'taking {candidate} out of {candidates}')
                        return
                shutil.copy(backup.name, test_case)
                # if we are not the first lines pass, we should bail out
                if self.arg != '0':
                    self.bailout = True
            else:
                shutil.copy(tmp_file.name, test_case)

    def __count_instances(self, test_case):
        with open(test_case) as in_file:
            lines = in_file.readlines()
            return len(lines)

    def new(self, test_case, check_sanity=None):
        self.bailout = False
        # None means no topformflat
        if self.arg != 'None':
            self.__format(test_case, check_sanity)
            if self.bailout:
                logging.warning('Skipping pass as sanity check fails for topformflat output')
                return None
        instances = self.__count_instances(test_case)
        state = BinaryState.create(instances)
        if self.arg in previous_state and previous_state[self.arg].chunk <= instances:
            logging.info(f'LinesPass.new: hint to start from chunk={previous_state[self.arg].chunk} instead of {state.chunk}')
            state.chunk = previous_state[self.arg].chunk
        return state

    def advance(self, test_case, state):
        state = state.advance()
        previous_state[self.arg] = state
        return state

    def advance_on_success(self, test_case, state):
        state = state.advance_on_success(self.__count_instances(test_case))
        previous_state[self.arg] = state
        return state

    def transform(self, test_case, state, process_event_notifier):
        # logging.info(f'[{os.getpid()}] LinesPass.transform: BEGIN{{')
        with open(test_case) as in_file:
            data = in_file.readlines()
        # logging.info(f'[{os.getpid()}] LinesPass.transform: read')

        old_len = len(data)
        data = data[0 : state.index] + data[state.end() :]
        assert len(data) < old_len
        # logging.info(f'[{os.getpid()}] LinesPass.transform: copied')

        tmp = os.path.dirname(test_case)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=tmp) as tmp_file:
            tmp_file.writelines(data)
        # logging.info(f'[{os.getpid()}] LinesPass.transform: wrote')

        shutil.move(tmp_file.name, test_case)

        # logging.info(f'[{os.getpid()}] LinesPass.transform: }}END')
        return (PassResult.OK, state)
