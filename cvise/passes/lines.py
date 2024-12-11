import logging
import os
import random
import shutil
import subprocess
import tempfile

from cvise.passes.abstract import AbstractPass, BinaryState, PassResult
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
        ):
            backup.close()
            with open(test_case) as in_file:
                try:
                    cmd = [self.external_programs['topformflat'], self.arg]
                    proc = subprocess.run(cmd, stdin=in_file, capture_output=True, text=True)
                except subprocess.SubprocessError:
                    return

            for line in proc.stdout.splitlines(keepends=True):
                if not line.isspace():
                    tmp_file.write(line)
            tmp_file.close()

            # we need to check that sanity check is still fine
            if check_sanity:
                shutil.copy(test_case, backup.name)
                shutil.copy(tmp_file.name, test_case)
                try:
                    check_sanity()
                except InsaneTestCaseError:
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
        r = BinaryState.create(instances)
        # logging.warning(f'LinesPass.new: instances={instances}')
        return r

    def advance(self, test_case, state):
        # Don't waste time on very small chunks on first runs.
        if state.chunk < 10 and int(self.arg) < 5:
            return None
        r = state.copy()
        # Try at least 10 times on the same (index, chunk) state, as we use randomization
        # and different strategies (see |transform()| below).
        r.counter += 1
        if r.counter <= r.chunk * 2 and r.counter <= max(2, r.chunk//10):
            return r
        # Otherwise, switch to the next (index, chunk) state according to the
        # standard logic.
        r.counter = 0
        return r.advance()

    def advance_on_success(self, test_case, state):
        r = state.advance_on_success(self.__count_instances(test_case))
        if r is None:
            return r
        r = r.copy()
        r.counter = 0
        return r

    def transform(self, test_case, state, process_event_notifier):
        with open(test_case) as in_file:
            data = in_file.readlines()

        # Randomize the cut block sizes a little bit, as the |chunk| parameter is
        # coming from a fixed sequence (|instances|, |instances//2|,
        # |instances//4|, ...).
        block = random.randint(state.chunk // 2 + 1, state.chunk)
        if state.index + block > state.instances:
            return (PassResult.INVALID, state)
        # Randomize the cut start positions as well, as the |index| parameter is
        # coming from a fixed sequence (0, |chunk|, |2*chunk|, |3*chunk|, ...).
        start_row = random.randint(state.index, min(state.index + state.chunk - 1, state.instances - block))
        if state.counter % 2 == 0:
            # Stategy 1: cut out the block of the size determined above with a fair dice roll.
            end_row = start_row + block
        else:
            # Strategy 2: grow the block until the braces balance is zero.
            bal = 0
            end_row = start_row
            while end_row < state.instances and (end_row - start_row < block or bal != 0):
                s = data[end_row]
                bal += s.count('{') - s.count('}')
                end_row += 1

        old_len = len(data)
        data = data[0 : start_row] + data[end_row :]
        assert len(data) < old_len

        tmp = os.path.dirname(test_case)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=tmp) as tmp_file:
            tmp_file.writelines(data)

        shutil.move(tmp_file.name, test_case)

        return (PassResult.OK, state)
