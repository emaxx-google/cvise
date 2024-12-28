import collections
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


class FuzzyLinesState:
    def __repr__(self):
        return f'FuzzyLinesState(nesting_depth: {self.nesting_depth}, global_counter: {self.global_counter}, unsuccess counter: {self.unsuccess_counter}, {self.size} bytes, {self.instances} lines, {self.size_history[-1] if len(self.size_history) else None} current size, {self.size_history[0] if len(self.size_history) else None} old size, size history {len(self.size_history)}, begin_cands: {len(self.begin_cands)} percent_per_1000={round(100*self.size/self.size_history[0],2) if len(self.size_history)==self.size_history.maxlen else ""})'

    @staticmethod
    def create(size, instances, nesting_depth, line_len, bal_per_line, cands_coeff):
        self = FuzzyLinesState()
        self.global_counter = 0
        self.size = size
        self.instances = instances
        self.nesting_depth = nesting_depth
        self.unsuccess_counter = 0
        self.line_len = line_len
        self.bal_per_line = bal_per_line
        self.cands_coeff = cands_coeff
        self.calc_cands()
        self.size_history = collections.deque(maxlen=1000)
        self.size_history.append(size)
        if not self.begin_cands:
            return None
        return self

    def copy(self):
        return copy.copy(self)

    def advance(self, size):
        self = self.copy()
        self.size = size
        self.global_counter += 1
        self.size_history.append(size)
        if self.unsuccess_counter < len(self.begin_cands) * self.cands_coeff:
            self.unsuccess_counter += 1
        else:
            return None
        return self

    def advance_on_success(self, size, instances, bal_per_line):
        self = self.copy()
        self.size = size
        self.global_counter += 1
        self.size_history.append(size)
        self.instances = instances
        self.bal_per_line = bal_per_line
        self.calc_cands()
        if not self.begin_cands:
            return None
        return self

    def calc_cands(self):
        self.begin_cands = [i for i in range(0, self.instances) if self.bal_per_line[i] == self.nesting_depth]
        self.begin_cands_shuffled = copy.copy(self.begin_cands)
        random.shuffle(self.begin_cands_shuffled)


class FuzzyLinesPass(AbstractPass):
    def check_prerequisites(self):
        return self.check_external_program('topformflat')

    def __format(self, test_case, nesting_depth, check_sanity):
        tmp = os.path.dirname(test_case)

        with (
            CloseableTemporaryFile(mode='w+', dir=tmp) as backup,
            CloseableTemporaryFile(mode='w+', dir=tmp) as tmp_file,
        ):
            backup.close()
            with open(test_case) as in_file:
                try:
                    cmd = [self.external_programs['topformflat'], str(nesting_depth)]
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

    def new(self, test_case, check_sanity=None):
        self.bailout = False

        nesting_depth, cands_coeff = map(int, self.arg.split('-'))

        self.__format(test_case, nesting_depth, check_sanity)
        if self.bailout:
            logging.warning('Skipping pass as sanity check fails for topformflat output')
            return None

        with open(test_case) as in_file:
            data = in_file.readlines()
        line_len = [len(s) for s in data]
        bal_per_line = self.__get_brace_balance_per_line(data)
        if bal_per_line is None:
            assert False
        size = sum(len(s) for s in data)
        instances = len(data)
        r = FuzzyLinesState.create(size, instances, nesting_depth, line_len, bal_per_line, cands_coeff)
        logging.info(f'[{os.getpid()}] FuzzyLinesPass.new: r={r}')
        return r
    
    def advance(self, test_case, state):
        old = state.copy()
        with open(test_case) as in_file:
            data = in_file.readlines()
        size = sum(len(s) for s in data)
        state = state.advance(size)
        # logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance: old={old} new={state}')
        return state

    def advance_on_success(self, test_case, state):
        old = state.copy()
        logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance_on_success: BEGIN{{: old={old}')
        with open(test_case) as in_file:
            data = in_file.readlines()
        new_size = sum(len(s) for s in data)
        new_instances = len(data)
        bal_per_line = self.__get_brace_balance_per_line(data)
        if bal_per_line is None:
            assert False
        state = state.advance_on_success(new_size, new_instances, bal_per_line)
        logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance_on_success: }}END: sizedelta={old.size-new_size} linedelta={old.instances-new_instances} old={old} new={state}')
        return state

    def transform(self, test_case, state, process_event_notifier):
        bal_per_line = state.bal_per_line
        if bal_per_line is None:
            # logging.info(f'FuzzyLinesPass.transform: INVALID: bal_per_line is None; state={state}')
            # return (PassResult.INVALID, state)
            assert False
        # assert bal_per_line == self.__get_brace_balance_per_line(data)

        cut_begin = random.choice(state.begin_cands)
        if state.bal_per_line[cut_begin] != state.nesting_depth:
            # logging.info(f'cut_begin={cut_begin} state.nesting_depth={state.nesting_depth} state.bal_per_line[cut_begin]={state.bal_per_line[cut_begin]} begin_cands={state.begin_cands}')
            assert False

        cut_bytes_approx = 0
        while cut_bytes_approx < 1 or cut_bytes_approx > state.size:
            peak = state.size // 100
            cut_bytes_approx = round(random.gauss(peak, peak/2))
        cut_end = cut_begin
        cut_bytes_removing = 0
        is_removing = False
        nesting_at_block_begin = None
        retained = []
        # dbg = ['***\n']
        # if cut_begin > 0:
        #     dbg.append(f'      # bal={bal_per_line[cut_begin]} #{cut_begin-1}# {data[cut_begin-1]}')
        while cut_end < state.instances and (cut_bytes_removing < cut_bytes_approx or bal_per_line[cut_end] > state.nesting_depth):
            if not is_removing and bal_per_line[cut_end] >= state.nesting_depth:
                is_removing = True
                nesting_at_block_begin = bal_per_line[cut_end]
            if is_removing and bal_per_line[cut_end + 1] < state.nesting_depth:
                if bal_per_line[cut_end] != nesting_at_block_begin:
                    # dbg.append(f'      # bal={bal_per_line[cut_end+1]} #{cut_end}# {data[cut_end]}')
                    # s = ''.join(dbg[-10:])
                    # logging.info(f'FuzzyLinesPass.transform: INVALID: jump at cut_end; state={state} cut_begin={cut_begin} cut_end={cut_end} cut_removing={cut_removing} nesting_at_block_begin={nesting_at_block_begin} retained={len(retained)} bal_per_line[cut_end]={bal_per_line[cut_end]} bal_per_line[cut_end + 1]={bal_per_line[cut_end + 1]}:\n{s}\n***')
                    # return (PassResult.INVALID, state)
                    assert False
                is_removing = False
                nesting_at_block_begin = None
            # logging.info(f'cut_end={cut_end} is_removing={is_removing} line={data[cut_end]}')
            if is_removing:
                cut_bytes_removing += state.line_len[cut_end]
                # dbg.append(f'---   # bal={bal_per_line[cut_end+1]} #{cut_end}# {data[cut_end]}')
            else:
                retained.append(cut_end)
                # dbg.append(f'      # bal={bal_per_line[cut_end+1]} #{cut_end}# {data[cut_end]}')
            cut_end += 1
        # if cut_end < state.instances:   
        #     dbg.append(f'      # bal={bal_per_line[cut_end+1]} #{cut_end}# {data[cut_end]}')
        # dbg.append('***')
        # logging.info(f'cut_begin={cut_begin} cut_end={cut_end} state={state} cut_removing={cut_removing} retained={len(retained)}')
        if cut_bytes_removing < cut_bytes_approx or bal_per_line[cut_end] > state.nesting_depth:
            # logging.info(f'FuzzyLinesPass.transform: INVALID: cut_removing small; state={state} cut_begin={cut_begin} cut_end={cut_end} cut_removing={cut_removing} cut_size_approx={cut_size_approx}')
            return (PassResult.INVALID, state)
        # logging.info(''.join(dbg))

        with open(test_case) as in_file:
            data = in_file.readlines()
        orig_data = data
        
        old_len = len(data)
        data = data[0 : cut_begin] + [data[i] for i in retained] + data[cut_end :]
        assert len(data) < old_len

        # logging.info(f'FuzzyLinesPass.transform: state={state} cut_size_approx={cut_size_approx} cut_begin={cut_begin} cut_end={cut_end} cut_removing={cut_removing} old_lines={len(orig_data)} new_lines={len(data)}')

        tmp = os.path.dirname(test_case)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=tmp) as tmp_file:
            tmp_file.writelines(data)

        shutil.move(tmp_file.name, test_case)

        return (PassResult.OK, state)

    def __get_brace_balance_per_line(self, data):
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
                        j += len(quotes)
                        quotes = None
                        continue
                elif c == '/' and next == '*':
                    in_multiline_comment = True
                elif c == '/' and next == '/':
                    break
                elif c == '"' or c == "'":
                    quotes = c
                elif c == 'R' and next == '"':
                    bracket = line.find('(', j)
                    quotes = ')' + line[j+2:bracket] + '"'
                    j += 2
                    continue
                elif c == '{':
                    bal += 1
                elif c == '}':
                    bal -= 1
                if bal < 0:
                    logging.warning(f'__get_brace_balance_per_line: bal<0')
                    with open('/usr/local/google/home/emaxx/tmp/cvise/ballt0.txt', 'wt') as f:
                        f.writelines(data)
                    return None
                j += 1
            bal_per_line.append(bal)
        if in_multiline_comment or quotes is not None or bal != 0:
            logging.warning(f'__get_brace_balance_per_line: in_multiline_comment={in_multiline_comment} quotes={quotes} bal={bal}')
            return None
        return bal_per_line
