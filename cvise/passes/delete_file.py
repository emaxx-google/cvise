import logging
import subprocess

from cvise.passes.abstract import AbstractPass, PassResult


class DeleteFilePass(AbstractPass):
    def check_prerequisites(self):
        return True

    def new(self, test_case, check_sanity=None, last_state_hint=None):
        return 1

    def advance(self, test_case, state):
        return None
    
    def advance_on_success(self, test_case, state):
        return None

    def transform(self, test_case, state, process_event_notifier):
        out = subprocess.check_output('~/clang-toys/reproducer-tool/delete-unused-files.sh', shell=True, cwd=test_case, encoding='utf-8', stderr=subprocess.STDOUT)
        if 'nothing to delete' in out:
            # logging.info(f'DeleteFilePass.transform: nothing to delete')
            return (PassResult.INVALID, state)
        dbg = [s.strip() for s in out.splitlines() if 'to delete: ' in s][0]
        logging.info(f'DeleteFilePass.transform: attempting: {dbg}')
        return (PassResult.OK, state)
