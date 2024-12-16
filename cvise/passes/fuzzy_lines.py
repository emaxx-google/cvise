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


class LinesState:
    def __repr__(self):
        return f'LinesState({self.end}, chunk: {self.chunk}, min_chunk: {self.min_chunk}, nesting_depth: {self.nesting_depth}, unsuccess counter: {self.unsuccess_counter}, {self.instances} instances)'

    @staticmethod
    def create(instances, min_chunk):
        if not instances:
            return None
        self = LinesState()
        self.instances = instances
        self.min_chunk = min_chunk
        self.nesting_depth = 0
        self.end = instances
        self.chunk = instances
        self.unsuccess_counter = 0
        self.success_on_current_level = False
        return self

    def copy(self):
        return copy.copy(self)
    
    def begin(self):
        return max(0, self.end - self.chunk)

    def advance(self):
        self = self.copy()
        if self.unsuccess_counter < 10: # math.isqrt(self.chunk):
            self.unsuccess_counter += 1
        else:
            self.unsuccess_counter = 0
            if self.end > self.chunk:
                self.end -= self.chunk
            elif self.success_on_current_level and False:  # DISABLED
                self.end = self.instances
                self.success_on_current_level = False
            elif self.nesting_depth < 5:
                self.nesting_depth += 1
                self.end = self.instances
            elif self.chunk // 2 >= self.min_chunk:
                self.chunk //= 2
                self.end = self.instances
                self.nesting_depth = 0
            else:
                return None
        return self

    def advance_on_success(self, instances):
        assert instances is not None
        if self.min_chunk > instances:
            return None
        self = self.copy()
        self.instances = instances
        self.end = min(self.end, instances)
        self.unsuccess_counter = 0
        self.success_on_current_level = True
        return self

class FuzzyLinesPass(AbstractPass):
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
        self.__bal_per_line_cache = {}

        # None means no topformflat
        if self.arg != 'None':
            self.__format(test_case, check_sanity)
            if self.bailout:
                logging.warning('Skipping pass as sanity check fails for topformflat output')
                return None

        with open(test_case) as in_file:
            data = in_file.readlines()
        if self.__get_brace_balance_per_line(data) is None:
            assert False
        instances = self.__count_instances(test_case)
        min_chunk = max(1, int(self.arg)) if self.arg is not None else 1
        if min_chunk > instances:
            return None
        r = LinesState.create(instances, min_chunk)
        # r.chunk = 10
        # r.chunk = 30
        # r.min_chunk = 10
        # r.nesting_depth = 1
        logging.info(f'[{os.getpid()}] FuzzyLinesPass.new: r={r}')
        return r

    def advance(self, test_case, state):
        old = state.copy()
        state = state.advance()

        # with open(test_case) as in_file:
        #     data = in_file.readlines()
        # bal_per_line = self.__get_brace_balance_per_line(data)
        # if bal_per_line is not None and False:  # DISABLED
        #     while state is not None and not self.__is_useful_state(bal_per_line, state):
        #         state.end = 0
        #         state.unsuccess_counter = float('inf')
        #         state = state.advance()

        # logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance: old={old} new={state}')
        return state

    def __is_useful_state(self, bal_per_line, state):
        best_block = 0
        need_block = max(state.min_chunk, state.chunk // 2 + 1)
        for bal in bal_per_line[:state.end]:
            if bal < state.nesting_depth:
                best_block = 0
            else:
                best_block += 1
                if best_block >= need_block:
                    return True
        return False

    def advance_on_success(self, test_case, state):
        old = state.copy()
        state = state.advance_on_success(self.__count_instances(test_case))
        logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance_on_success: delta={old.instances-state.instances} old={old} new={state}')
        return state

    def transform(self, test_case, state, process_event_notifier):
        with open(test_case) as in_file:
            data = in_file.readlines()
        orig_data = data

        bal_per_line = self.__get_brace_balance_per_line(data)
        if bal_per_line is None:
            # logging.info(f'FuzzyLinesPass.transform: INVALID: bal_per_line is None; state={state}')
            return (PassResult.INVALID, state)

        cut_begin = random.randint(state.begin(), min(state.end - 1, state.instances - state.min_chunk))
        # while cut_begin < state.end and bal_per_line[cut_begin] != state.nesting_depth:
        #     cut_begin += 1
        # if cut_begin == state.end or cut_begin + state.min_chunk >= state.instances:
        #     logging.info(f'FuzzyLinesPass.transform: INVALID: not selected cut_begin; state={state}')
        #     return (PassResult.INVALID, state)
        while cut_begin + state.min_chunk < state.instances and bal_per_line[cut_begin] != state.nesting_depth:
            cut_begin += 1
        if cut_begin + state.min_chunk >= state.instances:
            # logging.info(f'FuzzyLinesPass.transform: INVALID: not selected cut_begin; state={state}')
            return (PassResult.INVALID, state)

        cut_size_approx = random.randint(max(state.min_chunk, state.chunk // 2 + 1), state.chunk)
        cut_end = cut_begin
        cut_removing = 0
        is_removing = False
        nesting_at_block_begin = None
        retained = []
        dbg = ['***\n']
        if cut_begin > 0:
            dbg.append(f'      # bal={bal_per_line[cut_begin]} #{cut_begin-1}# {data[cut_begin-1]}')
        while cut_end < state.instances and (cut_removing < cut_size_approx or bal_per_line[cut_end] > state.nesting_depth):
            if not is_removing and bal_per_line[cut_end] >= state.nesting_depth:
                is_removing = True
                nesting_at_block_begin = bal_per_line[cut_end]
            if is_removing and bal_per_line[cut_end + 1] < state.nesting_depth:
                if bal_per_line[cut_end] != nesting_at_block_begin:
                    dbg.append(f'      # bal={bal_per_line[cut_end+1]} #{cut_end}# {data[cut_end]}')
                    s = ''.join(dbg[-10:])
                    # logging.info(f'FuzzyLinesPass.transform: INVALID: jump at cut_end; state={state} cut_begin={cut_begin} cut_end={cut_end} cut_removing={cut_removing} nesting_at_block_begin={nesting_at_block_begin} retained={len(retained)} bal_per_line[cut_end]={bal_per_line[cut_end]} bal_per_line[cut_end + 1]={bal_per_line[cut_end + 1]}:\n{s}\n***')
                    return (PassResult.INVALID, state)
                is_removing = False
                nesting_at_block_begin = None
            # logging.info(f'cut_end={cut_end} is_removing={is_removing} line={data[cut_end]}')
            if is_removing:
                cut_removing += 1
                dbg.append(f'---   # bal={bal_per_line[cut_end+1]} #{cut_end}# {data[cut_end]}')
            else:
                retained.append(data[cut_end])
                dbg.append(f'      # bal={bal_per_line[cut_end+1]} #{cut_end}# {data[cut_end]}')
            cut_end += 1
        if cut_end < state.instances:   
            dbg.append(f'      # bal={bal_per_line[cut_end+1]} #{cut_end}# {data[cut_end]}')
        dbg.append('***')
        # logging.info(f'cut_begin={cut_begin} cut_end={cut_end} state={state} cut_removing={cut_removing} retained={len(retained)}')
        if cut_removing < cut_size_approx:
            # logging.info(f'FuzzyLinesPass.transform: INVALID: cut_removing small; state={state}')
            return (PassResult.INVALID, state)
        assert cut_end - cut_begin >= state.min_chunk
        # logging.info(''.join(dbg))
        
        old_len = len(data)
        # data = data[0 : cut_begin] + data[cut_end :]
        data = data[0 : cut_begin] + retained + data[cut_end :]
        assert len(data) < old_len
        new_bal_per_line = self.__get_brace_balance_per_line(data)
        if new_bal_per_line is None:
            logging.info(f'state={state} cut_size_approx={cut_size_approx} cut_begin={cut_begin} cut_end={cut_end} cut_removing={cut_removing} retained={len(retained)}')
            logging.info(''.join(dbg))
            assert False

        # logging.info(f'FuzzyLinesPass.transform: state={state} cut_size_approx={cut_size_approx} cut_begin={cut_begin} cut_end={cut_end} cut_removing={cut_removing} old_len={len(orig_data)} new_len={len(data)}')

        tmp = os.path.dirname(test_case)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=tmp) as tmp_file:
            tmp_file.writelines(data)

        shutil.move(tmp_file.name, test_case)

        return (PassResult.OK, state)

    def __get_brace_balance_per_line(self, data):
        h = hash(tuple(data))
        if h in self.__bal_per_line_cache:
            return self.__bal_per_line_cache[h]
        bal = 0
        bal_per_line = [0]
        in_multiline_comment = False
        quotes = None
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
                elif c == '}':
                    bal -= 1
                if bal < 0:
                    logging.warning(f'__get_brace_balance_per_line: bal<0')
                    self.__bal_per_line_cache[h] = None
                    with open('/usr/local/google/home/emaxx/tmp/cvise/ballt0.txt', 'wt') as f:
                        f.writelines(data)
                    return None
                j += 1
            bal_per_line.append(bal)
        # print(f'bal={bal}')
        if in_multiline_comment or quotes is not None or bal != 0:
            logging.warning(f'__get_brace_balance_per_line: in_multiline_comment={in_multiline_comment} quotes={quotes} bal={bal}')
            self.__bal_per_line_cache[h] = None
            return None
        self.__bal_per_line_cache[h] = bal_per_line
        return bal_per_line
