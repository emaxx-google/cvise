import copy
import logging
import os
import re
import shutil
import subprocess
import time

from cvise.passes.abstract import AbstractPass, FuzzyBinaryState, PassResult
from cvise.utils.misc import CloseableTemporaryFile


previous_clang_delta_std = None


class ClangBinarySearchPass(AbstractPass):
    QUERY_TIMEOUT = 100

    def __init__(self, arg=None, external_programs=None):
        super().__init__(arg, external_programs)
        self.previous_clang_delta_std = previous_clang_delta_std
        self.clang_delta_std = None

    def check_prerequisites(self):
        return self.check_external_program('clang_delta')

    def detect_best_standard(self, test_case):
        best = None
        best_count = -1
        for std in ('c++98', 'c++11', 'c++14', 'c++17', 'c++20', 'c++2b'):
            self.clang_delta_std = std
            start = time.monotonic()
            instances = self.count_instances(test_case)
            took = time.monotonic() - start

            # prefer newer standard if the # of instances is equal
            if instances >= best_count:
                best = std
                best_count = instances
            logging.debug('available transformation opportunities for %s: %d, took: %.2f s' % (std, instances, took))
        logging.info('using C++ standard: %s with %d transformation opportunities' % (best, best_count))
        # Use the best standard option
        self.clang_delta_std = best

    def new(self, test_case, _=None, last_state_hint=None):
        global previous_clang_delta_std
        if self.user_clang_delta_std:
            self.clang_delta_std = self.user_clang_delta_std
        elif previous_clang_delta_std is not None:
            self.clang_delta_std = previous_clang_delta_std
        elif self.previous_clang_delta_std is not None:
            self.clang_delta_std = self.previous_clang_delta_std
        else:
            self.detect_best_standard(test_case)
            previous_clang_delta_std = self.clang_delta_std
            self.previous_clang_delta_std = self.clang_delta_std

        state = None
        if last_state_hint:
            state = FuzzyBinaryState.create_from_hint(None, last_state_hint)
            if state:
                logging.info(f'ClangBinarySearchPass.new: arg={self.arg} hint to start from chunk={state.chunk} index={state.index}')
        if not state:
            instances = self.count_instances(test_case)
            state = FuzzyBinaryState.create(instances)
        if state and last_state_hint:
            state.success_history = last_state_hint.success_history
        if state:
            state.clang_delta_std = self.clang_delta_std
        return state

    def advance(self, test_case, state):
        if not self.previous_clang_delta_std:
            self.previous_clang_delta_std = state.clang_delta_std
        if not self.clang_delta_std:
            self.clang_delta_std = state.clang_delta_std
        global previous_clang_delta_std
        if not previous_clang_delta_std:
            previous_clang_delta_std = state.clang_delta_std

        return state.advance()

    def advance_on_success(self, test_case, state):
        old = copy.copy(state)
        instances = state.real_num_instances - state.real_chunk()
        state = state.advance_on_success(instances)
        if state:
            state.real_num_instances = None
        logging.info(f'ClangBinarySearchPass.advance_on_success: delta_instances={old.instances-state.instances} chunk={old.rnd_chunk if old.tp==2 else old.chunk} tp={old.tp}')
        return state

    def count_instances(self, test_case):
        assert self.clang_delta_std
        args = [
            self.external_programs['clang_delta'],
            f'--query-instances={self.arg}',
            f'--std={self.clang_delta_std}',
        ]
        if self.clang_delta_preserve_routine:
            args.append(f'--preserve-routine="{self.clang_delta_preserve_routine}"')
        cmd = args + [test_case]

        try:
            proc = subprocess.run(cmd, text=True, capture_output=True, timeout=self.QUERY_TIMEOUT)
        except subprocess.TimeoutExpired:
            logging.warning(
                f'clang_delta --query-instances (--std={self.clang_delta_std}) {self.QUERY_TIMEOUT}s timeout reached'
            )
            return 0
        except subprocess.SubprocessError as e:
            logging.warning(f'clang_delta --query-instances (--std={self.clang_delta_std}) failed: {e}')
            return 0

        if proc.returncode != 0:
            logging.warning(
                f'clang_delta --query-instances failed with exit code {proc.returncode}: {proc.stderr.strip()}'
            )

        m = re.match('Available transformation instances: ([0-9]+)$', proc.stdout)

        if m is None:
            return 0
        else:
            return int(m.group(1))

    def parse_stderr(self, state, stderr):
        for line in stderr.split('\n'):
            if line.startswith('Available transformation instances:'):
                real_num_instances = int(line.split(':')[1])
                state.real_num_instances = real_num_instances
            elif line.startswith('Warning: number of transformation instances exceeded'):
                # TODO: report?
                pass

    def transform(self, test_case, state, process_event_notifier):
        # logging.info(f'transform: arg={self.arg} state={state}')
        old_state = copy.copy(state)

        if not self.clang_delta_std:
            self.clang_delta_std = state.clang_delta_std

        tmp = os.path.dirname(test_case)
        with CloseableTemporaryFile(mode='w', dir=tmp) as tmp_file:
            args = [
                f'--transformation={self.arg}',
                f'--counter={state.begin() + 1}',
                f'--to-counter={state.end()}',
                '--warn-on-counter-out-of-bounds',
                '--report-instances-count',
            ]
            if self.clang_delta_std:
                args.append(f'--std={self.clang_delta_std}')
            if self.clang_delta_preserve_routine:
                args.append(f'--preserve-routine="{self.clang_delta_preserve_routine}"')
            cmd = [self.external_programs['clang_delta']] + args + [test_case]
            logging.debug(' '.join(cmd))

            stdout, stderr, returncode = process_event_notifier.run_process(cmd)
            self.parse_stderr(state, stderr)
            tmp_file.write(stdout)
            tmp_file.close()
            if returncode == 0:
                shutil.copy(tmp_file.name, test_case)
                assert old_state.instances == state.instances
                # logging.info(f'transform: arg={self.arg} returning OK: instances={state.instances} old_state.instances={old_state.instances}')
                return (PassResult.OK, state)
            elif returncode == -11:
                instances = self.count_instances(test_case)
                logging.info(f'transform: arg={self.arg} recalculated instance from {state.instances} to {instances}')
                if instances == 0:
                    return (PassResult.STOP, None)
                else:
                    state.instances = instances
                    return (PassResult.INVALID, state)
            else:
                return (
                    PassResult.STOP if returncode == 255 else PassResult.ERROR,
                    state,
                )
