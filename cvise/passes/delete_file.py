import logging
import subprocess

from cvise.passes.abstract import AbstractPass, BinaryState, PassResult


TOOL = '~/clang-toys/reproducer-tool/delete-unused-files.sh'


class DeleteFilePass(AbstractPass):
    def check_prerequisites(self):
        return True

    def new(self, test_case, check_sanity=None, last_state_hint=None, strategy=None):
        out = subprocess.check_output(f'{TOOL} dry', shell=True, cwd=test_case, encoding='utf-8', stderr=subprocess.STDOUT)
        if 'nothing to delete' in out:
            return None
        s = [s.strip() for s in out.splitlines() if 'to delete: ' in s][0]
        state = BinaryState.create(int(s.split()[2]))
        logging.info(f'DeleteFilePass.new: state={state}')
        return state

    def advance(self, test_case, state):
        return state.advance()
    
    def advance_on_success(self, test_case, state):
        return None

    def transform(self, test_case, state, process_event_notifier):
        logging.info(f'DeleteFilePass.transform: {state}')
        subprocess.check_output(f'{TOOL} del {state.begin()+1} {state.end()+1}', shell=True, cwd=test_case, encoding='utf-8', stderr=subprocess.STDOUT)
        return (PassResult.OK, state)
