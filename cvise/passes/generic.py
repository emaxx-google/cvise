import copy
import json
import logging
import os
from pathlib import Path
import pickle
import re
import subprocess
import tempfile
import types

from cvise.passes.abstract import AbstractPass, BinaryState, FuzzyBinaryState, PassResult


INCLUDE_DEPTH_TOOL = Path(__file__).resolve().parent.parent / 'calc-include-depth/calc-include-depth'
INCLUSION_GRAPH_TOOL = Path(__file__).resolve().parent.parent / 'inclusion-graph/inclusion-graph'

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
        max_depth = max(path_to_depth.values()) if path_to_depth else 0

        hints = []
        hints += self.generate_makefile_hints(test_case)
        hints += self.generate_cppmaps_hints(test_case)
        hints += self.generate_inclusion_directive_hints(test_case)
        hints += self.generate_clang_pcm_lazy_load_hints(test_case)
        hints += self.generate_line_hints(test_case)

        def hint_main_file(h):
            return test_case / (h['name'] if h['type'] == 'fileref' else h['locations'][0]['file'])
        def hint_comparison_key(h):
            file = hint_main_file(h)
            d = path_to_depth.get(hint_main_file(h), max_depth + 1) if strategy == 'topo' else 0
            return d, file

        hints = self.regroup_hints(hints)
        self.append_unused_file_removal_hints(test_case, hints)
        for h in hints:
            if h['type'] == 'fileref':
                assert not Path(h['name']).is_absolute()
            for l in h['locations']:
                assert not Path(l['file']).is_absolute()
        hints.sort(key=hint_comparison_key)
        instances = len(hints)

        for h in hints:
            if h['type'] == 'fileref':
                path = test_case / h['name']
                assert path.exists(), 'path={path} hint={h}'

        depth_to_instances = [0] * (max_depth + 2)
        for h in hints:
            d = path_to_depth.get(hint_main_file(h), max_depth + 1)
            depth_to_instances[d] += 1
        logging.info(f'{self}.new: len(hints)={instances} depth_to_instances={depth_to_instances}')

        if logging.getLogger().isEnabledFor(logging.DEBUG):
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
        # if state.end() - state.begin() > 1:  # TEMP!!
        #     return (PassResult.INVALID, state)

        state_list = state if isinstance(state, list) else [state]

        test_case = Path(test_case)

        hints_to_load = set()
        for s in state_list:
            hints_to_load |= set(range(s.begin(), s.end()))

        hints = {}
        min_hint_id = min(hints_to_load)
        max_hint_id = max(hints_to_load)
        with open(self.extra_file_path(test_case)) as f:
            path_to_depth = json.loads(next(f))
            path_to_depth = dict((test_case / s, v) for s, v in path_to_depth.items())
            for i, line in enumerate(f):
                if i < min_hint_id:
                    continue
                if i > max_hint_id:
                    break
                if i in hints_to_load:
                    hints[i] = json.loads(line)
        max_depth = max(path_to_depth.values()) if path_to_depth else 0

        files_for_deletion = set(h['name'] for h in hints.values() if h['type'] == 'fileref')
        file_to_edit_hints = {}
        for i, h in hints.items():
            for l in h['locations']:
                if l['file'] not in files_for_deletion:
                    file_to_edit_hints.setdefault(l['file'], set()).add(i)

        # if isinstance(state, list) or state.end() - state.begin() > 1 or not files_for_deletion:
        #     return (PassResult.INVALID, state)  # TEMP!!
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'{self}.transform: state={state}')

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'files_for_deletion={files_for_deletion} file_to_edit_hints={file_to_edit_hints}')

        dbg = []
        if file_to_edit_hints:
            file = list(file_to_edit_hints.keys())[0]
            with open(test_case / file) as f:
                contents = f.read()
            hint_ids = file_to_edit_hints[file]
            chunks = []
            for hint_id in hint_ids:
                for l in hints[hint_id]['locations']:
                    if l['file'] == file:
                        chunks += l['chunks']
            for c in self.merge_chunks(chunks):
                dbg.append('`' + contents[c['begin']:c['end']] + '`')

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

        # Sanity-check we don't start including files outside of bundle.
        try:
            self.get_ordered_files_list(test_case, self.strategy)
        except AssertionError as e:
            raise RuntimeError(f'Sanity check failed:\nfiles_for_deletion={files_for_deletion}\nfile_to_edit_hints={file_to_edit_hints.keys()}\nedits={"; ".join(dbg)}\nstate={state}\n{e}')

        for s in state_list:
            s.improv_per_depth = []
            s.dbg_file = None
        state_list[0].improv_per_depth = improv_per_depth
        if files_for_deletion:
            state_list[0].dbg_file = ','.join(files_for_deletion)

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'{self}.transform: END: state={state}')
        return (PassResult.OK, state)

    def append_unused_file_removal_hints(self, test_case, hints):
        mentioned_files = set(Path(h['name']) for h in hints if h['type'] == 'fileref')
        for file in test_case.rglob('*'):
            rel_path = file.relative_to(test_case)
            if not file.is_dir() and not file.is_symlink() and rel_path not in mentioned_files:
                if file.suffix in ('.makefile'):
                    hints.append({
                        'type': 'fileref',
                        'name': str(rel_path),
                        'locations': [],
                    })

    def edit_file(self, file, chunks_to_delete):
        with open(file) as f:
            data = f.read()
        merged = self.merge_chunks(chunks_to_delete)
        new_data = ''
        ptr = 0
        for c in merged:
            new_data += data[ptr:c['begin']]
            ptr = c['end']
        new_data += data[ptr:]
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'file={file} before:\n{data}\nafter:\n{new_data}')
        with open(file, 'w') as f:
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
        file_to_generating_targets = {}
        file_mentions = {}
        token_to_locs = {}

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
                    is_special_target = cur_target_name in ('clean', '.ALWAYS')
                    if not is_special_target:
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
                elif line.startswith('\t') and not is_special_target:
                    assert cur_target
                    cur_target.append({
                        'begin': line_start_pos,
                        'end': line_end_pos,
                    })
                    token_search_pos = 0
                    option_with_untouchable_arg = False
                    for i, token in enumerate(line.split()):
                        if token == '||':  # hack
                            break
                        token_pos = line.find(token, token_search_pos)
                        assert token_pos != -1, f'token "{token}" not found in line "{line}"'
                        token_search_pos = token_pos + len(token)
                        mention_pos = line_start_pos + token_pos
                        assert line[mention_pos-line_start_pos: mention_pos-line_start_pos+len(token)] == token
                        match = re.match(r'([^=]*=)*([^=]+\.(txt|cppmap|pcm|o|cc))$', token)
                        if match:
                            mentioned_file = match.group(2)
                            is_main_file_for_rule = not match.group(1) and not option_with_untouchable_arg
                            if is_main_file_for_rule:
                                file_to_generating_targets.setdefault(mentioned_file, []).append(cur_target_name)
                            else:
                                file_mentions.setdefault(mentioned_file, []).append({
                                    'begin': mention_pos,
                                    'end': mention_pos + len(token),
                                })
                            option_with_untouchable_arg = False
                        elif i > 0 and not option_with_untouchable_arg and \
                                token not in ('-c', '-nostdinc++', '-nostdlib++', '-fno-crash-diagnostics', '-ferror-limit=0', '-w', '-Wno-error', '-Xclang=-emit-module', '-xc++', '-fmodules', '-fno-implicit-modules', '-fno-implicit-module-maps', '-Xclang=-fno-cxx-modules', '-Xclang=-fmodule-map-file-home-is-cwd') and \
                                not token.startswith('-fmodule-name=') and not token.startswith('-std=') and not token.startswith('--sysroot='):
                            if token in ('-o', '-iquote', '-isystem', '-I'):
                                option_with_untouchable_arg = True
                            if token == '-Xclang':
                                next_tokens = line[token_search_pos:].split(maxsplit=2)
                                if next_tokens and next_tokens[0] == '-fallow-pcm-with-compiler-errors':
                                    option_with_untouchable_arg = True
                            if not option_with_untouchable_arg:
                                token_to_locs.setdefault(token, []).append({
                                    'begin': mention_pos,
                                    'end': mention_pos + len(token),
                                })
                        else:
                            option_with_untouchable_arg = False
                else:
                    cur_target = None
                    cur_target_name = None

                line_start_pos = line_end_pos

        hints = []
        deletion_candidates = set(list(file_mentions.keys()) + list(file_to_generating_targets.keys()))
        for path in deletion_candidates:
            full_path = test_case / path
            if not full_path.exists() or full_path.suffix in ('.cc'):
                continue
            chunks = []
            chunks += file_mentions.get(path, [])
            for target_name in file_to_generating_targets.get(path, []):
                chunks += targets[target_name]
                chunks += file_mentions.get(target_name, [])
            assert chunks, f'path={path}'
            hints.append({
                'type': 'fileref',
                'name': path,
                'locations': [{
                    'file': str(makefile_rel_path),
                    'chunks': chunks,
                }],
            })
        for token, locs in sorted(token_to_locs.items()):
            for chunk in locs:
                hints.append({
                    'type': 'edit',
                    'locations': [{
                        'file': str(makefile_rel_path),
                        'chunks': [{
                            'begin': chunk['begin'],
                            'end': chunk['end'],
                        }],
                    }],
                })
        return hints

    def generate_cppmaps_hints(self, test_case):
        hints = []
        name_to_cppmaps = {}
        cppmap_to_uses = {}
        for cppmap_path in test_case.rglob('*.cppmap'):
            cppmap_hints, top_level_names, headers, uses = self.parse_cppmap(test_case, cppmap_path)
            hints += cppmap_hints
            for name in top_level_names:
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
        cppmap_rel_path = cppmap_path.relative_to(test_case)
        hints = []
        top_level_names = []
        headers = {}
        uses = {}
        nested_module_start_pos = None
        nested_module_start_line_end_pos = None
        nested_module_empty = None
        module_depth = 0
        with open(cppmap_path) as f:
            line_start_pos = 0
            for line in f:
                line_end_pos = line_start_pos + len(line)

                module_match = re.match(r'.*module\s+(\S+).*', line)
                if module_match:
                    if module_depth == 0:
                        top_level_names.append(module_match.group(1).strip('"'))
                    else:
                        nested_module_start_pos = line_start_pos
                        nested_module_start_line_end_pos = line_end_pos
                        nested_module_empty = True
                    module_depth += 1

                header_match = re.match(r'.*header\s+"(.*)".*', line)
                if not module_match and header_match:
                    mentioned_file = header_match.group(1)
                    if (test_case / mentioned_file).exists():
                        headers.setdefault(mentioned_file, []).append({
                            'begin': line_start_pos,
                            'end': line_end_pos,
                        })
                        hints.append({
                            'type': 'edit',
                            'locations': [{
                                'file': str(cppmap_rel_path),
                                'chunks': [{
                                    'begin': line_start_pos,
                                    'end': line_end_pos,
                                }],
                            }]
                        })
                    nested_module_empty = False

                use_match = re.match(r'\s*use\s+(\S+)\s*', line)
                if not module_match and not header_match and use_match:
                    mentioned_module = use_match.group(1).strip('"')
                    uses.setdefault(mentioned_module, []).append({
                        'begin': line_start_pos,
                        'end': line_end_pos,
                    })
                    hints.append({
                        'type': 'edit',
                        'locations': [{
                            'file': str(cppmap_rel_path),
                            'chunks': [{
                                'begin': line_start_pos,
                                'end': line_end_pos,
                            }],
                        }]
                    })
                    nested_module_empty = False

                closing_brace = line.strip() == '}'
                if closing_brace:
                    module_depth -= 1
                    assert module_depth >= 0, f'cppmap_path={cppmap_path}'
                    if nested_module_start_pos is not None:
                        if nested_module_empty:
                            # Attempt to delete the empty submodule.
                            hints.append({
                                'type': 'edit',
                                'locations': [{
                                    'file': str(cppmap_rel_path),
                                    'chunks': [{
                                        'begin': nested_module_start_pos,
                                        'end': line_end_pos,
                                    }],
                                }]
                            })
                        # Attempt to inline the submodule's contents into the outer module.
                        hints.append({
                            'type': 'edit',
                            'locations': [{
                                'file': str(cppmap_rel_path),
                                'chunks': [{
                                    'begin': nested_module_start_pos,
                                    'end': nested_module_start_line_end_pos,
                                }, {
                                    'begin': line_start_pos,
                                    'end': line_end_pos,
                                }],
                            }]
                        })
                        nested_module_start_pos = None

                if not module_match and not header_match and not use_match and not closing_brace:
                    hints.append({
                        'type': 'edit',
                        'locations': [{
                            'file': str(cppmap_rel_path),
                            'chunks': [{
                                'begin': line_start_pos,
                                'end': line_end_pos,
                            }],
                        }]
                    })

                line_start_pos = line_end_pos

        assert module_depth == 0, f'cppmap_path={cppmap_path}'
        return hints, top_level_names, headers, uses

    def generate_line_hints(self, test_case):
        hints = []
        for file in test_case.rglob('*'):
            if not file.is_dir() and not file.is_symlink() and file.suffix not in ('.makefile', '.cppmap'):
                file_rel_path = str(file.relative_to(test_case))
                with open(file) as f:
                    line_start_pos = 0
                    try:
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
                    except UnicodeDecodeError as e:
                        raise RuntimeError(f'Failure while parsing {file}: {e}')
        return hints

    def generate_inclusion_directive_hints(self, test_case):
        if not test_case.is_dir():
            return []

        with open(test_case / 'target.makefile') as f:
            lines = f.readlines()
            for i, l in enumerate(lines):
                if '.o:' in l and i+1 < len(lines):
                    orig_command = lines[i+1].strip()
                    break
            else:
                return []

        root_file_candidates = list(test_case.rglob('*.cc'))
        if not root_file_candidates:
            return []
        root_file = root_file_candidates[0]

        file_to_line_pos = {}
        def get_line_pos_in_file(file, idx):
            if file not in file_to_line_pos:
                lines_pos = []
                with open(file) as f:
                    line_start_pos = 0
                    for line in f:
                        line_end_pos = line_start_pos + len(line)
                        lines_pos.append((line_start_pos, line_end_pos))
                        line_start_pos = line_end_pos
                file_to_line_pos[file] = lines_pos
            return file_to_line_pos[file][idx]

        orig_command = re.sub(r'\S*-fmodule\S*', '', orig_command).split()
        command = [
            INCLUSION_GRAPH_TOOL,
            root_file,
            '--',
            '-resource-dir=third_party/crosstool/v18/stable/toolchain/lib/clang/google3-trunk'] + orig_command
        path_and_depth = []
        out = subprocess.check_output(command, cwd=test_case, stderr=subprocess.DEVNULL, encoding='utf-8')
        hints = []
        for line in out.splitlines():
            if not line.strip():
                continue
            from_file, from_line, to_file = line.split(' ')
            from_file = Path(from_file)
            if not from_file.is_absolute():
                from_file = test_case / from_file
            from_line = int(from_line) - 1
            to_file = Path(to_file)
            if not to_file.is_absolute():
                to_file = test_case / to_file
            start_pos, end_pos = get_line_pos_in_file(from_file, from_line)
            hints.append({
                'type': 'fileref',
                'name': str(to_file.relative_to(test_case)),
                'locations': [{
                    'file': str(from_file.relative_to(test_case)),
                    'chunks': [{
                        'begin': start_pos,
                        'end': end_pos,
                    }],
                }],
            })
        return hints

    def generate_clang_pcm_lazy_load_hints(self, test_case):
        orig_command = self.get_root_compile_command(test_case)
        if not orig_command:
            return []

        subprocess.run(['make', '-f', 'target.makefile', '-j64'], cwd=Path(test_case), stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

        with tempfile.NamedTemporaryFile() as tmp:
            orig_command = re.sub(r'\s-o\s\S+', ' ', orig_command).split()
            command = copy.copy(orig_command)
            command.append('-resource-dir=third_party/crosstool/v18/stable/toolchain/lib/clang/google3-trunk')
            command.append('-Xclang')
            command.append(f'-print-deserialized-declarations-to-file={tmp.name}')
            proc = subprocess.run(command, cwd=test_case, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
            logging.debug(f'stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}')
            subprocess.run(['make', '-f', 'target.makefile', 'clean'], cwd=Path(test_case), stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
            path_to_used = {}
            if not Path(tmp.name).exists():
                # Likely an old Clang version, before the switch was introduced.
                return None
            with open(tmp.name) as f:
                file = None
                seg_from = None
                for line in f:
                    logging.debug(f'LINE {line.strip()}')
                    match = re.match(r'required lines in file: (.*)', line)
                    if match:
                        file = Path(test_case) / match[1]
                        seg_from = None
                        seg_to = None
                    match = re.match(r'\s*from: (\d+)', line)
                    if match:
                        seg_from = int(match[1])
                        seg_to = None
                    match = re.match(r'\s*to: (\d+)', line)
                    if match:
                        seg_to = int(match[1])
                    if file and seg_from and seg_to:
                        path_to_used.setdefault(file, []).append((seg_from-1, seg_to-1))

        hints = []
        for file, segs in path_to_used.items():
            file_rel_path = file.relative_to(test_case)
            segptr = 0
            with open(file) as f:
                line_start_pos = 0
                for i, line in enumerate(f):
                    line_end_pos = line_start_pos + len(line)
                    while segptr < len(segs) and segs[segptr][1] < i:
                        segptr += 1
                    if segptr >= len(segs) or not (segs[segptr][0] <= i <= segs[segptr][1]):
                        hints.append({
                            'type': 'edit',
                            'locations': [{
                                'file': str(file_rel_path),
                                'chunks': [{
                                    'begin': line_start_pos,
                                    'end': line_end_pos,
                                }],
                            }],
                        })
                    line_start_pos = line_end_pos
        return hints

    def get_root_compile_command(self, test_case):
        with open(Path(test_case) / 'target.makefile') as f:
            lines = f.readlines()
            for i, l in enumerate(lines):
                if '.o:' in l:
                    return lines[i+1].strip().lstrip('@').lstrip()
            else:
                return None

    def get_ordered_files_list(self, test_case, strategy):
        if not test_case.is_dir():
            return [test_case], {test_case: 0}

        with open(test_case / 'target.makefile') as f:
            lines = f.readlines()
            for i, l in enumerate(lines):
                if '.o:' in l and i+1 < len(lines):
                    orig_command = lines[i+1].strip()
                    break
            else:
                orig_command = None

        root_file_candidates = list(test_case.rglob('*.cc'))
        root_file = root_file_candidates[0] if root_file_candidates else None

        path_to_depth = {}
        if root_file and orig_command:
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
                    path = test_case / path
                assert path.is_relative_to(test_case), f'{path} doesnt belong to {test_case}'
                assert path.exists(), f'doesnt exist: {path}'
                path_and_depth.append((path.resolve(), int(depth)))
            path_to_depth = dict(path_and_depth)
        if root_file and not path_to_depth:
            path_to_depth[root_file] = 0

        files = [f for f in Path(test_case).rglob('*') if not f.is_dir() and not f.is_symlink()]
        files.sort(key=lambda f: (path_to_depth.get(f.resolve(), 1E9) if strategy == 'topo' else 0, f.suffix != '.cc', f))

        return files, path_to_depth
