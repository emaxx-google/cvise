import json
import subprocess

from cvise.passes.abstract import BinaryState, SubsegmentState
from cvise.passes.hint_based import HintBasedPass
from cvise.utils.hint import HintBundle


class TreeSitterPass(HintBasedPass):
    def check_prerequisites(self):
        return self.check_external_program('treesitter_delta')

    def generate_hints(self, test_case):
        cmd = [
            self.external_programs['treesitter_delta'],
            self.arg,
            test_case,
        ]
        proc = subprocess.run(cmd, text=True, capture_output=True)

        if proc.returncode != 0:
            stderr = proc.stderr.strip()
            delim = ': ' if stderr else ''
            raise RuntimeError(f'treesitter_delta failed with exit code {proc.returncode}{delim}{stderr}')

        # When reading, gracefully handle EOF because the tool might've failed with no output.
        stdout = iter(proc.stdout.splitlines())
        vocab_line = next(stdout, None)
        vocab = json.loads(vocab_line) if vocab_line else []

        hints = []
        for line in stdout:
            if not line.isspace():
                hints.append(json.loads(line))
        return HintBundle(vocabulary=vocab, hints=hints)

    def create_elementary_state(self, hint_count: int):
        if self.arg in ('remove-class', 'remove-function', 'remove-namespace'):
            return BinaryState.create_from_sqrt(instances=hint_count)
        return BinaryState.create(instances=hint_count)
