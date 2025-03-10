import logging
from pathlib import Path
import subprocess

from cvise.passes.abstract import AbstractPass, FuzzyBinaryState, PassResult


TOOL = Path(__file__).parent / 'delete-unused-files.sh'

success_histories = {}


class DeleteFilePass(AbstractPass):
    def check_prerequisites(self):
        return True

    def new(self, test_case, check_sanity=None, last_state_hint=None, strategy=None):
        try:
            out = subprocess.check_output(f'{TOOL} dry', shell=True, cwd=test_case, encoding='utf-8', stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            logging.error(f'{e}\nstdout+stderr:\n{e.output}')
            raise
        if 'nothing to delete' in out:
            return None
        s = [s.strip() for s in out.splitlines() if 'to delete: ' in s][0]
        state = FuzzyBinaryState.create(int(s.split()[2]), strategy)
        if state:
            state.files_deleted = state.end() - state.begin()
        dbg = ' '.join(l.strip() for l in out.splitlines())
        logging.debug(f'DeleteFilePass.new: state={state} stdout={dbg}')
        return state

    def advance(self, test_case, state):
        new = state.advance(success_histories)
        if new:
            new.files_deleted = new.end() - new.begin()
        return new
    
    def advance_on_success(self, test_case, state):
        return None

    def on_success_observed(self, state):
        if not isinstance(state, list):
            state.get_success_history(success_histories).append(state.end() - state.begin())

    def transform(self, test_case, state, process_event_notifier):
        logging.debug(f'DeleteFilePass.transform: {state}')
        proc = subprocess.Popen(
            f'{TOOL} del {state.begin()+1} {state.end()+1}', shell=True, cwd=test_case,
            encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if proc.returncode:
            raise RuntimeError(f'Failed: stdout:\n{out}\nstderr:\n{err}')
        s = [s.strip() for s in out.splitlines() if 'files for deletion:' in s]
        if s:
            state.dbg_file = s[0].split(':')[1].strip()
        return (PassResult.OK, state)
