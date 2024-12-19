import copy
import logging
import math
import os
import random
import shutil
import subprocess
import tempfile

from cvise.passes.abstract import AbstractPass, PassResult
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.misc import CloseableTemporaryFile


class CurlyCutterState:
    def __repr__(self):
        return f'CurlyCutterState({self.index}, {self.instances} instances, braces_prio: {len(self.braces_prio)})'

    @staticmethod
    def create(instances, bal_per_line, braces_prio):
        if not instances:
            return None
        self = CurlyCutterState()
        self.instances = instances
        self.index = 0
        self.bal_per_line = bal_per_line
        self.braces_prio = braces_prio
        return self

    def copy(self):
        return copy.copy(self)

    def advance(self):
        self = self.copy()
        self.index += 1
        if self.index >= len(self.braces_prio):
            return None
        return self


class CurlyCutterPass(AbstractPass):
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
                    cmd = [self.external_programs['topformflat'], '100']
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

        self.__format(test_case, check_sanity)
        if self.bailout:
            logging.warning('Skipping pass as sanity check fails for topformflat output')
            return None

        with open(test_case) as in_file:
            data = in_file.readlines()
        bal_per_line, braces_prio = self.__get_brace_balance_per_line(data)
        if bal_per_line is None:
            assert False
        r = CurlyCutterState.create(len(data), bal_per_line, braces_prio)
        logging.info(f'[{os.getpid()}] CurlyCutterPass.new: r={r}')
        return r
    
    def advance(self, test_case, state):
        return state.advance()

    def advance_on_success(self, test_case, state):
        with open(test_case) as in_file:
            data = in_file.readlines()
        bal_per_line, braces_prio = self.__get_brace_balance_per_line(data)
        if bal_per_line is None:
            assert False
        r = CurlyCutterState.create(len(data), bal_per_line, braces_prio)
        logging.info(f'[{os.getpid()}] CurlyCutterPass.advance_on_success: r={r} delta={state.instances-r.instances}')
        return r

    def transform(self, test_case, state, process_event_notifier):
        _, close_line, open_line = state.braces_prio[state.index]

        with open(test_case) as in_file:
            data = in_file.readlines()
        orig_data = data
        
        old_len = len(data)
        data = data[0 : open_line + 1] + data[close_line :]
        assert len(data) < old_len

        tmp = os.path.dirname(test_case)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=tmp) as tmp_file:
            tmp_file.writelines(data)

        shutil.move(tmp_file.name, test_case)

        return (PassResult.OK, state)

    def __get_brace_balance_per_line(self, data):
        # h = hash(tuple(data))
        # if h in self.__bal_per_line_cache:
        #     return self.__bal_per_line_cache[h]
        bal = 0
        bal_per_line = [0]
        active_braces = []
        in_multiline_comment = False
        quotes = None
        byte_pos = 0
        braces_prio = []
        for i, line in enumerate(data):
            j = 0
            while j < len(line):
                c = line[j]
                next = line[j+1] if j+1 < len(line) else None
                if in_multiline_comment:
                    if c == '*' and next == '/':
                        in_multiline_comment = False
                elif quotes is not None:
                    if c == '\\':
                        j += 2
                        continue
                    if line[j:j+len(quotes)] == quotes:
                        # print(f"closing quotes={quotes}")
                        j += len(quotes)
                        quotes = None
                        continue
                elif c == '/' and next == '*':
                    in_multiline_comment = True
                elif c == '/' and next == '/':
                    break
                elif c == '"' or c == "'":
                    quotes = c
                    # print(f"opening quotes={quotes}")
                elif c == 'R' and next == '"':
                    bracket = line.find('(', j)
                    quotes = ')' + line[j+2:bracket] + '"'
                    # print(f'opening quotes={quotes}')
                    j += 2
                    continue
                elif c == '{':
                    bal += 1
                    active_braces.append((i, byte_pos + len(line)))
                elif c == '}':
                    open_line = active_braces[-1][0]
                    open_byte_pos = active_braces[-1][1]
                    open_bal = bal_per_line[open_line + 1]
                    if open_bal == bal and byte_pos > open_byte_pos:
                        braces_prio.append((byte_pos - open_byte_pos, i, open_line))
                    active_braces.pop()
                    bal -= 1
                if bal < 0:
                    logging.warning(f'__get_brace_balance_per_line: bal<0')
                    # self.__bal_per_line_cache[h] = None
                    with open('/usr/local/google/home/emaxx/tmp/cvise/ballt0.txt', 'wt') as f:
                        f.writelines(data)
                    return None, None
                j += 1
            bal_per_line.append(bal)
            byte_pos += len(line) + 1
        # print(f'bal={bal}')
        if in_multiline_comment or quotes is not None or bal != 0:
            logging.warning(f'__get_brace_balance_per_line: in_multiline_comment={in_multiline_comment} quotes={quotes} bal={bal}')
            # self.__bal_per_line_cache[h] = None
            return None, None
        # self.__bal_per_line_cache[h] = bal_per_line
        braces_prio.sort(reverse=True)
        return bal_per_line, braces_prio
