import copy
import json
import logging
import os
from pathlib import Path
import pickle
import re
import subprocess
import types

from cvise.passes.abstract import AbstractPass, BinaryState, FuzzyBinaryState, PassResult


INCLUDE_DEPTH_TOOL = '/usr/local/google/home/emaxx/cvise/cvise/calc-include-depth/calc-include-depth'

success_histories = {}


class GenericPass(AbstractPass):
    def __repr__(self):
        s = super().__repr__()
        if self.strategy is not None:
            s += f' (strategy {self.strategy})'
        return s

    def check_prerequisites(self):
        return True

    def supports_merging(self):
        return True
    
    def regroup_hints(self, hints):
        file_to_locs = {}
        for h in hints:
            if h['type'] == 'fileref':
                file_to_locs.setdefault(h['name'], []).extend(h['locations'])
        new_hints = []
        for h in hints:
            if h['type'] != 'fileref':
                new_hints.append(h)
        for file, locs in file_to_locs.items():
            new_hints.append({
                'type': 'fileref',
                'name': file,
                'locations': locs,
            })
        return new_hints

    def new(self, test_case, check_sanity=None, last_state_hint=None, strategy=None):
        test_case = Path(test_case)

        files, path_to_depth = self.get_ordered_files_list(test_case, strategy)
        if not files:
            return None
        max_depth = max(path_to_depth.values())

        hints = []
        hints += self.generate_makefile_hints(test_case)
        hints += self.generate_cppmaps_hints(test_case)
        hints += self.generate_line_hints(test_case)

        def hint_main_file(h):
            return test_case / (h['name'] if h['type'] == 'fileref' else h['locations'][0]['file'])

        hints = self.regroup_hints(hints)
        hints.sort(key=lambda h: path_to_depth.get(hint_main_file(h), max_depth + 1))
        instances = len(hints)

        depth_to_instances = [0] * (max_depth + 2)
        for h in hints:
            d = path_to_depth.get(hint_main_file(h), max_depth + 1)
            depth_to_instances[d] += 1
        logging.info(f'{self}.new: len(hints)={instances} depth_to_instances={depth_to_instances}')

        if logging.getLogger().isEnabledFor(logging.DEBUG) and False:  # DISABLED
            for hint in hints:
                if hint['type'] == 'fileref':
                    logging.debug(f'*** MENTION OF {hint["name"]}')
                else:
                    logging.debug(f'***')
                for l in hint['locations']:
                    logging.debug(f'    in {l["file"]}:')
                    with open(test_case / l["file"]) as f:
                        contents = f.read()
                    for c in l['chunks']:
                        dbg = contents[c['begin']:c['end']].replace('\n', ' ')
                        logging.debug(f'       {c["begin"]}..{c["end"]}: "{dbg}"')

        with open(self.extra_file_path(test_case), 'w') as f:
            json.dump(dict((str(s.relative_to(test_case)), v) for s,v in path_to_depth.items()), f)
            f.write('\n')
            for hint in hints:
                json.dump(hint, f)
                f.write('\n')

        if not hints:
            return None

        if last_state_hint:
            state = FuzzyBinaryState.create_from_hint(instances, strategy, last_state_hint, depth_to_instances)
        else:
            state = FuzzyBinaryState.create(instances, strategy, depth_to_instances, repr(self))
        while state and strategy == 'topo' and state.tp == 0:
            state = state.advance(success_histories)
        return state

    def advance(self, test_case, state):
        new = state.advance(success_histories)
        while new and new.strategy == 'topo' and new.tp == 0:
            new = new.advance(success_histories)
        return new

    def on_success_observed(self, state):
        if not isinstance(state, list):
            state.get_success_history(success_histories).append(state.end() - state.begin())

    def transform(self, test_case, state, process_event_notifier):
        test_case = Path(test_case)
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'{self}.transform: state={state}')

        hints_to_load = set()
        if isinstance(state, list):
            for s in state:
                if s.type == 'edit':
                    hints_to_load.update(s.hints)
        else:
            hints_to_load = set(range(state.begin(), state.end()))

        hints = {}
        with open(self.extra_file_path(test_case), 'r') as f:
            path_to_depth = json.loads(next(f))
            path_to_depth = dict((test_case / s, v) for s, v in path_to_depth.items())
            for i, line in enumerate(f):
                if i in hints_to_load:
                    hints[i] = json.loads(line)
        max_depth = max(path_to_depth.values())

        file_to_edit_hints = {}
        files_for_deletion = set()
        if isinstance(state, list):
            for s in state:
                if s.type == 'delete':
                    files_for_deletion.add(s.file)
            for s in state:
                if s.type == 'edit' and s.file not in files_for_deletion:
                    file_to_edit_hints.setdefault(s.file, set()).update(s.hints)
        else:
            files_for_deletion = set(h['name'] for h in hints.values() if h['type'] == 'fileref')
            for i, h in hints.items():
                for l in h['locations']:
                    if l['file'] not in files_for_deletion:
                        file_to_edit_hints.setdefault(l['file'], set()).add(i)
            
            state.split_per_file = {}
            for file in files_for_deletion:
                path = test_case / file
                improv_per_depth = [0] * (2 + max_depth)
                d = path_to_depth.get(path, max_depth + 1)
                improv_per_depth[d] = path.stat().st_size
                state.split_per_file[file] = types.SimpleNamespace(file=file, type='delete', improv_per_depth=improv_per_depth)
            for file, hint_ids in file_to_edit_hints.items():
                if file not in files_for_deletion:
                    path = test_case / file
                    chunks = []
                    for hint_id in hint_ids:
                        for l in hints[hint_id]['locations']:
                            if l['file'] == file:
                                chunks += l['chunks']
                    improv_per_depth = [0] * (2 + max_depth)
                    d = path_to_depth.get(path, max_depth + 1)
                    improv_per_depth[d] = sum(c['end'] - c['begin'] for c in self.merge_chunks(chunks))
                    state.split_per_file[file] = types.SimpleNamespace(file=file, type='edit', hints=list(hint_ids), improv_per_depth=improv_per_depth)
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'files_for_deletion={files_for_deletion} file_to_edit_hints={file_to_edit_hints}')

        improv_per_depth = [0] * (2 + max_depth)
        for file, hint_ids in file_to_edit_hints.items():
            chunks = []
            for hint_id in hint_ids:
                for l in hints[hint_id]['locations']:
                    if l['file'] == file:
                        chunks += l['chunks']
            path = test_case / file
            improv = self.edit_file(path, chunks)
            d = path_to_depth.get(path, max_depth + 1)
            improv_per_depth[d] += improv
        for file in files_for_deletion:
            path = test_case / file
            improv = path.stat().st_size
            d = path_to_depth.get(path, max_depth + 1)
            improv_per_depth[d] += improv
            os.unlink(path)

        for s in (state if isinstance(state, list) else [state]):
            s.improv_per_depth = improv_per_depth
        return (PassResult.OK, state)

    def edit_file(self, file, chunks_to_delete):
        with open(file, 'rb') as f:
            data = f.read()
        merged = self.merge_chunks(chunks_to_delete)
        new_data = b''
        ptr = 0
        for c in merged:
            new_data += data[ptr:c['begin']]
            ptr = c['end']
        new_data += data[ptr:]
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'file={file} before:\n{data}\nafter:\n{new_data}')
        with open(file, 'wb') as f:
            f.write(new_data)
        return sum(c['end'] - c['begin'] for c in merged)

    def merge_chunks(self, chunks):
        result = []
        for c in sorted(chunks, key=lambda c: c['begin']):
            if result and result[-1]['end'] >= c['begin']:
                result[-1]['end'] = max(result[-1]['end'], c['end'])
            else:
                result.append(c)
        return result

    def extra_file_path(self, test_case):
        return Path(test_case).parent / f'extra{self}.dat'

    def generate_makefile_hints(self, test_case):
        targets = {}
        cppmap_to_target = {}
        file_mentions = {}

        makefile_rel_path = Path('target.makefile')
        makefile_path = test_case / makefile_rel_path
        with open(makefile_path) as f:
            line_start_pos = 0
            cur_target = None
            for line in f:
                line_end_pos = line_start_pos + len(line)

                if line.strip() and not line.startswith('\t') and ':' in line:
                    cur_target_name = line.split(':')[0].strip()
                    assert cur_target_name
                    cur_target = targets.setdefault(cur_target_name, [])
                    cur_target.append({
                        'begin': line_start_pos,
                        'end': line_end_pos,
                    })
                    for dep_name in line.split(':', maxsplit=2)[1].split():
                        mention_pos = line_start_pos + line.find(dep_name)
                        targets.setdefault(dep_name, []).append({
                            'begin': mention_pos,
                            'end': mention_pos + len(dep_name),
                        })
                elif line.startswith('\t'):
                    assert cur_target
                    cur_target.append({
                        'begin': line_start_pos,
                        'end': line_end_pos,
                    })
                    token_search_pos = 0
                    for token in line.split():
                        token_pos = line.find(token, token_search_pos)
                        assert token_pos != -1, f'token "{token}" not found in line "{line}"'
                        token_search_pos = token_pos + 1
                        match = re.match(r'([^=]*=)*([^=]+\.(txt|cppmap|pcm|o))$', token)
                        if match:
                            mentioned_file = match.group(2)
                            is_main_rule_for_cppmap = not match.group(1) and Path(mentioned_file).suffix == '.cppmap'
                            if is_main_rule_for_cppmap:
                                cppmap_to_target[mentioned_file] = cur_target_name
                            else:
                                mention_pos = line_start_pos + token_pos
                                file_mentions.setdefault(mentioned_file, []).append({
                                    'begin': mention_pos,
                                    'end': mention_pos + len(token),
                                })
                else:
                    cur_target = None
                    cur_target_name = None

                line_start_pos = line_end_pos

        hints = []
        candidates = set(list(file_mentions.keys()) + list(cppmap_to_target.keys()))
        for path in candidates:
            full_path = test_case / path
            if not full_path.exists():
                continue
            chunks = []
            chunks += file_mentions.get(path, [])
            if path in cppmap_to_target:
                target_name = cppmap_to_target[path]
                chunks += targets[target_name]
                chunks += file_mentions.get(target_name, [])
            hints.append({
                'type': 'fileref',
                'name': path,
                'locations': [{
                    'file': str(makefile_rel_path),
                    'chunks': [{
                        'begin': chunk['begin'],
                        'end': chunk['end'],
                    } for chunk in chunks]
                }],
            })

        return hints

    def generate_cppmaps_hints(self, test_case):
        hints = []
        name_to_cppmaps = {}
        cppmap_to_uses = {}
        for cppmap_path in test_case.rglob('*.cppmap'):
            name, headers, uses = self.parse_cppmap(test_case, cppmap_path)
            if not name:
                # Parsing failed - ignore this map.
                continue
            name_to_cppmaps.setdefault(name, []).append(cppmap_path)
            cppmap_to_uses[cppmap_path] = uses
            cppmap_rel_path = str(cppmap_path.relative_to(test_case))
            for header_path, chunks in headers.items():
                hints.append({
                    'type': 'fileref',
                    'name': header_path,
                    'locations': [{
                        'file': cppmap_rel_path,
                        'chunks': chunks,
                    }],
                })

        for cppmap_path, uses in cppmap_to_uses.items():
            cppmap_rel_path = str(cppmap_path.relative_to(test_case))
            for use_name, chunks in uses.items():
                for other_cppmap_path in name_to_cppmaps.get(use_name, []):
                    hints.append({
                        'type': 'fileref',
                        'name': str(other_cppmap_path.relative_to(test_case)),
                        'locations': [{
                            'file': cppmap_rel_path,
                            'chunks': chunks,
                        }]
                    })

        return hints

    def parse_cppmap(self, test_case, cppmap_path):
        name = None
        headers = {}
        uses = {}
        with open(cppmap_path) as f:
            line_start_pos = 0
            for line in f:
                line_end_pos = line_start_pos + len(line)

                match = re.match (r'.*module\s+(\S+).*', line)
                if match and not name:
                    name = match.group(1).strip('"')

                match = re.match(r'.*header\s+"(.*)".*', line)
                if match:
                    mentioned_file = match.group(1)
                    if (test_case / mentioned_file).exists():
                        headers.setdefault(mentioned_file, []).append({
                            'begin': line_start_pos,
                            'end': line_end_pos,
                        })

                match = re.match(r'\s*use\s+(\S+)\s*', line)
                if match:
                    mentioned_module = match.group(1).strip('"')
                    uses.setdefault(mentioned_module, []).append({
                        'begin': line_start_pos,
                        'end': line_end_pos,
                    })

                line_start_pos = line_end_pos

        if not name:
            return None, None, None
        return name, headers, uses
    
    def generate_line_hints(self, test_case):
        hints = []
        for file in test_case.rglob('*'):
            if not file.is_dir() and not file.is_symlink():
                file_rel_path = str(file.relative_to(test_case))
                with open(file) as f:
                    line_start_pos = 0
                    for line in f:
                        line_end_pos = line_start_pos + len(line)
                        hints.append({
                            'type': 'edit',
                            'locations': [{
                                'file': file_rel_path,
                                'chunks': [{
                                    'begin': line_start_pos,
                                    'end': line_end_pos,
                                }],
                            }]
                        })
                        line_start_pos = line_end_pos
        return hints

    def get_ordered_files_list(self, test_case, strategy):
        if not test_case.is_dir():
            return [test_case], {test_case: 0}
        
        with open(Path(test_case) / 'target.makefile') as f:
            lines = f.readlines()
            for i, l in enumerate(lines):
                if '.o:' in l and i+1 < len(lines):
                    orig_command = lines[i+1].strip()
                    break
            else:
                orig_command = None

        root_file = next(Path(test_case).rglob('*.cc'))
            
        path_to_depth = {}
        if orig_command:
            orig_command = re.sub(r'\S*-fmodule\S*', '', orig_command).split()
            command = [
                INCLUDE_DEPTH_TOOL,
                root_file,
                '--',
                '-resource-dir=third_party/crosstool/v18/stable/toolchain/lib/clang/google3-trunk'] + orig_command
            path_and_depth = []
            out = subprocess.check_output(command, cwd=test_case, stderr=subprocess.DEVNULL, encoding='utf-8')
            for line in out.splitlines():
                if not line.strip():
                    continue
                path, depth = line.rsplit(maxsplit=1)
                path = Path(path)
                if not path.is_absolute():
                    path = Path(test_case) / path
                assert path.exists(), f'doesnt exist: {path}'
                path_and_depth.append((path.resolve(), int(depth)))
            path_to_depth = dict(path_and_depth)
        if not path_to_depth:
            path_to_depth[root_file] = 0

        files = [f for f in Path(test_case).rglob('*') if not f.is_dir() and not f.is_symlink()]
        files.sort(key=lambda f: (path_to_depth.get(f.resolve(), 1E9) if strategy == 'topo' else 0, f.suffix != '.cc', f))

        return files, path_to_depth
