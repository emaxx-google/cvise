import collections
import copy
import logging
import math
import os
import psutil
import random
import shutil
import subprocess
import tempfile

from cvise.passes.abstract import AbstractPass, PassResult
from cvise.utils.error import InsaneTestCaseError
from cvise.utils.misc import CloseableTemporaryFile


def get_available_cores():
    try:
        # try to detect only physical cores, ignore HyperThreading
        # in order to speed up parallel execution
        core_count = psutil.cpu_count(logical=False)
        if not core_count:
            core_count = psutil.cpu_count(logical=True)
        # respect affinity
        try:
            affinity = len(psutil.Process().cpu_affinity())
            assert affinity >= 1
        except AttributeError:
            return core_count

        if core_count:
            core_count = min(core_count, affinity)
        else:
            core_count = affinity
        return core_count
    except NotImplementedError:
        return 1

CORES = get_available_cores()


class FuzzyLinesState:
    def __repr__(self):
        return f'FuzzyLinesState(nesting_depth: {self.nesting_depth}, global_counter: {self.global_counter}, {self.size} bytes, {self.instances} lines, {self.size_history[-1] if len(self.size_history) else None} current size, {self.size_history[0] if len(self.size_history) else None} old size, size history {len(self.size_history)}, begin_cands: {len(self.begin_cands)} percent_per_1000={round(100*self.size/self.size_history[0],2) if len(self.size_history)==self.size_history.maxlen else ""} cut_begin={self.cut_begin} cut_end={self.cut_end} peak_dbg=[{self.peak_dbg}] last_peak={self.last_peak} cut_len_approx={self.cut_len_approx} cut_actually_removing={self.cut_actually_removing})'

    @staticmethod
    def create(size, instances, nesting_depth, line_len, bal_per_line, cands_coeff):
        self = FuzzyLinesState()
        self.global_counter = 0
        self.size = size
        self.instances = instances
        self.nesting_depth = nesting_depth
        self.line_len = line_len
        self.bal_per_line = bal_per_line
        self.cands_coeff = cands_coeff
        self.calc_cands()
        self.size_history = collections.deque(maxlen=1000)
        self.size_history.append(size)
        self.successes = {}
        self.unsuccesses = {}
        self.success_history = collections.deque(maxlen=get_available_cores()*100)
        self.cut_begin = None
        self.cut_end = None
        self.cut_len_approx = None
        self.cut_actually_removing = None
        self.last_peak = None
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
        self.track_unsuccess(self.cut_actually_removing, -1)
        self.track_success(self.cut_actually_removing, +1)
        return self

    def calc_cands(self):
        self.begin_cands = [i for i in range(0, self.instances) if self.bal_per_line[i] == self.nesting_depth]
        self.begin_cands_shuffled = copy.copy(self.begin_cands)
        random.shuffle(self.begin_cands_shuffled)

    def choose_peak(self):
        cands = list(set(self.successes.keys()) | set(self.unsuccesses.keys()))
        best = None
        best_exp = None
        dbg = ''
        cnt_succ = 0
        cnt_unsucc = 0
        for x in sorted(cands, reverse=True):
            if x > self.instances:
                continue
            cnt_succ += self.successes.get(x, 0)
            assert cnt_succ >= 0
            assert cnt_succ <= self.success_history.maxlen
            cnt_unsucc += self.unsuccesses.get(x, 0)
            assert cnt_unsucc <= self.success_history.maxlen
            if cnt_succ > 0:
                exp = x / (1 + max(0, cnt_unsucc) / cnt_succ / CORES)
                dbg += f'cand={x} cnt_succ={cnt_succ} cnt_unsucc={cnt_unsucc} exp={exp}, '
                if best_exp is None or exp > best_exp:
                    best_exp = exp
                    best = x
        # logging.info(f'[{os.getpid()}] FuzzyLinesState.choose_peak: best={best} among {dbg}')
        self.peak_dbg = dbg
        return best

    def prepare_next_step(self):
        for _ in range(10):
            if self.prepare_next_step_internal():
                return True
        return False

    def prepare_next_step_internal(self):
        # logging.info(f'[{os.getpid()}] FuzzyLinesState.prepare_next_step_internal: BEGIN{{')
        self.cut_begin = None
        cut_begin = random.choice(self.begin_cands)
        assert self.bal_per_line[cut_begin] == self.nesting_depth
        peak = self.choose_peak()
        if peak is None:
            peak = 10
        if self.instances - self.begin_cands[0] < 10:
            return False
        peak = min(peak, self.instances - self.begin_cands[0])
        peak = max(peak, 10)
        cut_len_approx = None
        while cut_len_approx is None or cut_len_approx < 2 or cut_len_approx > self.instances:
            cut_len_approx = round(random.gauss(peak, peak/2))
            # logging.info(f'[{os.getpid()}] FuzzyLinesState.prepare_next_step: peak={peak} cut_len_approx={cut_len_approx} instances={self.instances}')
        cut_end = cut_begin
        cut_actually_removing = 0
        is_removing = False
        # dbg = ['***\n']
        # if cut_begin > 0:
        #     dbg.append(f'      # bal={bal_per_line[cut_begin]} #{cut_begin-1}# {data[cut_begin-1]}')
        while cut_end < self.instances and (cut_actually_removing < cut_len_approx or self.bal_per_line[cut_end] > self.nesting_depth):
            if not is_removing and self.bal_per_line[cut_end] >= self.nesting_depth:
                is_removing = True
            if is_removing and self.bal_per_line[cut_end + 1] < self.nesting_depth:
                is_removing = False
            if is_removing:
                cut_actually_removing += 1
            cut_end += 1
        if cut_actually_removing < cut_len_approx or self.bal_per_line[cut_end] > self.nesting_depth:
            self.track_unsuccess(cut_len_approx, +1)
            # logging.info(f'[{os.getpid()}] FuzzyLinesState.prepare_next_step_internal: }}END: unsuccess')
            return False
        self.cut_begin = cut_begin
        self.cut_end = cut_end
        self.last_peak = peak
        self.cut_len_approx = cut_len_approx
        self.cut_actually_removing = cut_actually_removing
        self.track_unsuccess(cut_actually_removing, +1)
        # logging.info(f'[{os.getpid()}] FuzzyLinesState.prepare_next_step_internal: }}END: success')
        return True

    def track_success(self, le, delta):
        self.upd(self.successes, le, delta)
        if len(self.success_history) == self.success_history.maxlen:
            self.upd([self.successes, self.unsuccesses][self.success_history[0][0]], self.success_history[0][1], -self.success_history[0][2])
        self.success_history.append((0, le, delta))
        assert len(self.successes) <= self.success_history.maxlen
        assert len(self.unsuccesses) <= self.success_history.maxlen

    def track_unsuccess(self, le, delta):
        self.upd(self.unsuccesses, le, delta)
        if len(self.success_history) == self.success_history.maxlen:
            self.upd([self.successes, self.unsuccesses][self.success_history[0][0]], self.success_history[0][1], -self.success_history[0][2])
        self.success_history.append((1, le, delta))
        assert len(self.successes) <= self.success_history.maxlen
        assert len(self.unsuccesses) <= self.success_history.maxlen

    def upd(self, arr, pos, delta):
        arr.setdefault(pos, 0)
        arr[pos] += delta
        if not arr[pos]:
            del arr[pos]


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
        if instances < 10:
            return None
        r = FuzzyLinesState.create(size, instances, nesting_depth, line_len, bal_per_line, cands_coeff)
        if r is None or not r.prepare_next_step():
            return None
        logging.info(f'[{os.getpid()}] FuzzyLinesPass.new: r={r}')
        return r
    
    def advance(self, test_case, state):
        old = state.copy()
        # logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance: BEGIN{{: old={old}')
        with open(test_case) as in_file:
            data = in_file.readlines()
        size = sum(len(s) for s in data)
        state = state.advance(size)
        if state is None or not state.prepare_next_step():
            return None
        if state.last_peak <= 10 and len(state.success_history) == state.success_history.maxlen:
            logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance: exiting: peak is too lot, state={state}')
            return None
        # logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance: }}END: old={old} new={state}')
        return state

    def advance_on_success(self, test_case, state):
        old = state.copy()
        logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance_on_success: BEGIN{{: old={old}')
        with open(test_case) as in_file:
            data = in_file.readlines()
        new_size = sum(len(s) for s in data)
        new_instances = len(data)
        if new_instances < 10:
            return None
        bal_per_line = self.__get_brace_balance_per_line(data)
        if bal_per_line is None:
            assert False
        state = state.advance_on_success(new_size, new_instances, bal_per_line)
        if state is None:
            return None
        assert state.cut_actually_removing == old.instances-new_instances
        if not state.prepare_next_step():
            return None
        if state.last_peak <= 10 and len(state.success_history) == state.success_history.maxlen:
            logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance_on_success: }}END: exiting: peak is too lot, state={state}')
            return None
        logging.info(f'[{os.getpid()}] FuzzyLinesPass.advance_on_success: }}END: sizedelta={old.size-new_size} linedelta={old.instances-new_instances} old={old} new={state} linedelta={old.instances-new_instances} old_peak={old.last_peak}')
        return state

    def transform(self, test_case, state, process_event_notifier):
        logging.info(f'[{os.getpid()}] FuzzyLinesPass.transform: BEGIN{{: state={state}')
        bal_per_line = state.bal_per_line
        if bal_per_line is None:
            # logging.info(f'FuzzyLinesPass.transform: INVALID: bal_per_line is None; state={state}')
            # return (PassResult.INVALID, state)
            assert False
        # assert bal_per_line == self.__get_brace_balance_per_line(data)
    
        cut_begin = state.cut_begin
        assert state.bal_per_line[cut_begin] == state.nesting_depth

        cut_end = state.cut_end
        assert state.bal_per_line[cut_end] == state.nesting_depth
        retained = []
        is_removing = False
        for i in range(cut_begin, cut_end):
            if not is_removing and state.bal_per_line[i] >= state.nesting_depth:
                is_removing = True
            if is_removing and state.bal_per_line[i + 1] < state.nesting_depth:
                is_removing = False
            if not is_removing:
                retained.append(i)

        with open(test_case) as in_file:
            data = in_file.readlines()
        orig_data = data

        old_len = len(data)
        data = data[0 : cut_begin] + [data[i] for i in retained] + data[cut_end :]
        assert len(data) < old_len

        # logging.info(f'[{os.getpid()}] FuzzyLinesPass.transform: state={state} cut_begin={cut_begin} cut_end={cut_end} old_lines={len(orig_data)} new_lines={len(data)}')

        # new_bal_per_line = self.__get_brace_balance_per_line(data)
        # if new_bal_per_line is None:
        #     logging.info(f'[{os.getpid()}] FuzzyLinesPass.transform: state={state} cut_begin={cut_begin} cut_end={cut_end} old_lines={len(orig_data)} new_lines={len(data)}')
        #     s = ''.join(orig_data)
        #     logging.info(f'[{os.getpid()}] OLD:\n{s}')
        #     s = ''.join(data)
        #     logging.info(f'[{os.getpid()}] NEW:\n{s}')
        #     import sys
        #     sys.exit(-1)
        #     assert False

        tmp = os.path.dirname(test_case)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=tmp) as tmp_file:
            tmp_file.writelines(data)

        shutil.move(tmp_file.name, test_case)

        logging.info(f'[{os.getpid()}] FuzzyLinesPass.transform: }}END: state={state}')

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
                    logging.warning(f'[{os.getpid()}] __get_brace_balance_per_line: bal<0')
                    with open('/usr/local/google/home/emaxx/tmp/cvise/ballt0.txt', 'wt') as f:
                        f.writelines(data)
                    return None
                j += 1
            bal_per_line.append(bal)
        if in_multiline_comment or quotes is not None or bal != 0:
            logging.warning(f'[{os.getpid()}] __get_brace_balance_per_line: in_multiline_comment={in_multiline_comment} quotes={quotes} bal={bal}')
            return None
        return bal_per_line
