import atexit
import copy
import functools
import json
import logging
import math
import os
from pathlib import Path
import pyzstd
import random
import re
import shlex
import shutil
import string
import subprocess
import tempfile
import types

from cvise.passes.abstract import AbstractPass, BinaryState, FuzzyBinaryState, PassResult


EXTERNAL_PROGRAMS = ['calc-include-depth', 'clang_delta', 'clex', 'hint_tool', 'inclusion-graph', 'topformflat', 'tree-sitter-delta']

success_histories = {}
type_to_attempted = {}


def get_type_to_attempted(pass_repr, generation):
    d = type_to_attempted.setdefault(pass_repr, {})
    if d.get('gen') != generation:
        d['gen'] = generation
        d['value'] = {}
    return d['value']

def relative_path(path, test_case):
    if test_case.is_dir():
        return path.relative_to(test_case)
    return path.name

def join_to_test_case(test_case, path):
    if test_case.is_dir():
        return test_case / path
    assert test_case.name == Path(path).name
    return test_case

def is_utf8_file(path):
    try:
        with open(path) as f:
            f.read()
        return True
    except UnicodeDecodeError:
        return False

def is_hint_meta_type(t):
    return t.startswith('#')


class PolyState(dict):
    def __init__(self):
        self.generation = random.randint(0, 10 ** 9)

    @staticmethod
    def start_small(type):
        return 'rm-toks-' in type or 'rm-tok-pattern-' in type

    @staticmethod
    def create(instances, pass_repr):
        self = PolyState()
        self.pass_repr = pass_repr
        self.types = list(sorted(instances.keys()))
        self.instances = instances
        for k, i in instances.items():
            self[k] = FuzzyBinaryState.create(i, pass_repr + ' :: ' + k, start_small=self.start_small(k))
        self.ptr = -1
        return self

    @staticmethod
    def create_from_hint(instances, last_state_hint, pass_repr):
        self = PolyState()
        self.pass_repr = pass_repr
        self.types = list(instances.keys())
        self.instances = instances
        for k, i in instances.items():
            if (k in last_state_hint) and last_state_hint[k]:
                self[k] = FuzzyBinaryState.create_from_hint(i, last_state_hint[k])
            else:
                self[k] = FuzzyBinaryState.create(i, pass_repr + ' :: ' + k, start_small=self.start_small(k))
        self.ptr = -1
        return self

    def advance(self, success_histories):
        new = copy.copy(self)

        wrapped_around = False
        while True:
            new.ptr += 1
            while new.ptr < len(new.types) and (is_hint_meta_type(new.get_type()) or not new[new.get_type()]):
                new.ptr += 1
            if new.ptr == len(new.types):
                if wrapped_around:
                    return None
                wrapped_around = True
                new.ptr = -1
                continue

            tp = new.get_type()
            type_to_attempted = get_type_to_attempted(new.pass_repr, new.generation)
            previous_attempts = type_to_attempted.get(tp + '::attempted', [])
            previous_successes = type_to_attempted.get(tp + '::success', [])

            while True:
                new[tp] = new[tp].advance(success_histories)
                if not new[tp]:
                    break
                already_attempted = any(le == new[tp].begin() and new[tp].end() == ri
                                        for le, ri in previous_attempts)
                subset_of_success = any(le <= new[tp].begin() and new[tp].end() <= ri
                                        for le, ri in previous_successes)
                if not already_attempted and not subset_of_success:
                    break
            if new[tp]:
                break

        new.mark_attempted('attempted')
        return new

    def mark_attempted(self, tag):
        get_type_to_attempted(self.pass_repr, self.generation).setdefault(self.get_type() + f'::{tag}', []).append((
            self[self.get_type()].begin(),
            self[self.get_type()].end()))

    def on_success_observed(self, improv):
        history = self[self.get_type()].get_success_history(success_histories)
        history.append(self.end() - self.begin())
        self.mark_attempted('success')

    def begin(self):
        return self.shift() + self[self.get_type()].begin()

    def end(self):
        return self.shift() + self[self.get_type()].end()

    def shift(self):
        return sum(self.instances[self.types[i]] for i in range(self.ptr))

    def __repr__(self):
        if self.ptr == -1:
            return 'PolyState[zygote]'
        t = self.get_type()
        return t + '::' + repr(self[t])

    def set_dbg(self, data):
        self[self.get_type()].dbg_file = data

    def get_type(self):
        return self.types[self.ptr]

    def get_improv(self):
        return self.improv


class GenericPass(AbstractPass):
    def __init__(self, arg=None, external_programs=None):
        super().__init__(arg, external_programs)
        self.extra_file_path = tempfile.NamedTemporaryFile(prefix='cvise-hints-', suffix='.jsonl.zstd', delete=False).name
        atexit.register(functools.partial(os.unlink, self.extra_file_path))

    def check_prerequisites(self):
        return all(self.check_external_program(p) for p in EXTERNAL_PROGRAMS)

    def supports_merging(self):
        return True

    def lazy_input_copying(self):
        return True

    def get_output_hint_types(self):
        if self.arg == 'makefile':
            return ['makeincldir', 'makemkdir', 'maketok', '#fileref', '#symbol']
        elif self.arg == 'cppmaps':
            return ['#fileref', '#symbol', 'cppmapheader', 'cppmapmissingheader', 'cppmapuse', 'cppmapmod', 'cppmapmodinl', 'cppmapline']
        elif self.arg == 'inclusion_directives':
            return ['#fileref']
        elif self.arg == 'clang_pcm_lazy_load':
            return ['lazypcm', 'lazypcmwhole']
        elif self.arg == 'clang_pcm_minimization_hints':
            return ['clang_pcm_hints_filewise', 'clang_pcm_hints_rowwise', 'clang_pcm_hints_unused_file_filewise', 'clang_pcm_hints_unused_file_rowwise']
        elif self.arg.startswith('clang_pcm_minimization_hints_splice_topformflat'):
            return ['clang_pcm_minimization_hints_splice_topformflat' + self.arg.partition('::')[2]]
        elif self.arg == 'line_markers':
            return ['linemarker']
        elif self.arg == 'blank':
            return ['blankline', 'blankconseq', 'blanktrail', 'blanklead', 'preprcond']
        elif self.arg == 'lines':
            return ['line']
        elif self.arg.startswith('clex::'):
            sub_arg = self.arg.partition('::')[2]
            if 'rm-tok-pattern' in sub_arg:
                bits = int(sub_arg.rsplit('-')[-1])
                return [f'rm-tok-pattern-{i}' for i in range(2, bits+1)]
            else:
                return [sub_arg]
        elif self.arg.startswith('topformflat::'):
            return ['topformflat' + self.arg.partition('::')[2], 'comment']
        elif self.arg.startswith('clang_delta::'):
            return [self.arg.partition('::')[2]]
        elif self.arg == 'tree_sitter_delta':
            return ['treesitfunc']
        elif self.arg == 'delete-file':
            return ['delfile::0', 'delfile::1', 'delfile::2', 'rmdir']
        elif self.arg == 'inline-file':
            return ['inlinefile']
        elif self.arg == 'rename-file':
            return ['renamefile']
        elif self.arg == 'rename-symbol':
            return ['renamesymbol']
        assert False, f'Unexpected arg={self.arg}'

    def get_input_hint_types(self):
        if self.arg == 'delete-file':
            return ['#fileref']
        elif self.arg == 'inline-file':
            return ['#fileref']
        elif self.arg == 'rename-file':
            return ['#fileref']
        elif self.arg == 'rename-symbol':
            return ['#fileref', '#symbol']
        elif self.arg.startswith('clang_pcm_minimization_hints_splice_topformflat'):
            return ['clang_pcm_hints_rowwise', 'clang_pcm_hints_unused_file_rowwise', 'topformflat' + self.arg.partition('::')[2]]
        else:
            return []

    def new(self, test_case, check_sanity=None, last_state_hint=None, other_init_states=None):
        test_case = Path(test_case)

        files = get_ordered_files_list(test_case)
        if not files:
            return None
        file_to_id = dict((f, i) for i, f in enumerate(files))

        path_to_depth = get_path_to_depth(test_case, self.external_programs)
        max_depth = max(path_to_depth.values()) if path_to_depth else 0
        depth_per_file = [path_to_depth.get(path.resolve(), max_depth + 1) for path in files]
        for p, d in path_to_depth.items():
            pa = join_to_test_case(test_case, relative_path(p, test_case.resolve()))
            assert depth_per_file[file_to_id[pa]] == d

        if self.arg == 'makefile':
            hints = generate_makefile_hints(test_case, files, file_to_id)
        elif self.arg == 'cppmaps':
            hints = generate_cppmaps_hints(test_case, files, file_to_id)
        elif self.arg == 'inclusion_directives':
            hints = generate_inclusion_directive_hints(test_case, files, file_to_id, self.external_programs)
        elif self.arg == 'clang_pcm_lazy_load':
            hints = generate_clang_pcm_lazy_load_hints(test_case, files, file_to_id)
        elif self.arg == 'clang_pcm_minimization_hints':
            hints = generate_clang_pcm_minimization_hints(test_case, files, file_to_id)
        elif self.arg.startswith('clang_pcm_minimization_hints_splice_topformflat'):
            hints = generate_splice_hints(test_case, files, file_to_id,
                                          ['clang_pcm_hints_rowwise', 'clang_pcm_hints_unused_file_rowwise'],
                                          ['topformflat' + self.arg.partition('::')[2]],
                                          'clang_pcm_minimization_hints_splice_topformflat' + self.arg.partition('::')[2],
                                          other_init_states)
        elif self.arg == 'line_markers':
            hints = generate_line_markers_hints(test_case, files, file_to_id)
        elif self.arg == 'blank':
            hints = generate_blank_hints(test_case, files, file_to_id)
        elif self.arg == 'lines':
            hints = generate_line_hints(test_case, files, file_to_id)
        elif self.arg.startswith('clex::'):
            hints = generate_clex_hints(test_case, files, file_to_id, self.arg.partition('::')[2], self.external_programs)
        elif self.arg.startswith('topformflat::'):
            hints = generate_topformflat_hints(test_case, files, file_to_id, int(self.arg.partition('::')[2]), self.external_programs)
        elif self.arg.startswith('clang_delta::'):
            hints = generate_clang_delta_hints(test_case, files, file_to_id, self.arg.partition('::')[2], self.external_programs)
        elif self.arg == 'tree_sitter_delta':
            hints = generate_tree_sitter_delta_hints(test_case, files, file_to_id, self.external_programs)
        elif self.arg == 'delete-file':
            hints = generate_delete_file_hints(test_case, files, file_to_id, other_init_states)
        elif self.arg == 'inline-file':
            hints = generate_inline_file_hints(test_case, files, file_to_id, other_init_states)
        elif self.arg == 'rename-file':
            hints = generate_rename_file_hints(test_case, files, file_to_id, other_init_states)
        elif self.arg == 'rename-symbol':
            hints = generate_rename_symbol_hints(test_case, files, file_to_id, other_init_states)
        else:
            raise RuntimeError(f'Unknown hint source: arg={self.arg}')

        if hints is None:
            # Bail out silently if the pass doesn't support the input format at all.
            return None

        def hint_main_file(h):
            if 'n' in h:
                return h['n'], files[h['n']]
            if 'ns' in h:
                return len(files), test_case / h['ns']
            if 'f' in h:
                return h['f'], files[h['f']]
            if not h['multi']:
                return -1, ''
            return h['multi'][0]['f'], files[h['multi'][0]['f']]
        def hint_comparison_key(h):
            return h['t'], hint_main_file(h), sorted([(l.get('f'), l.get('l'), l.get('r')) for l in get_hint_locs(h)])

        declared_types = self.get_output_hint_types()
        for h in hints:
            assert_valid_hint(h, files, declared_types)
        hints.sort(key=hint_comparison_key)

        instances = {}
        for h in hints:
            instances[h['t']] = instances.get(h['t'], 0) + 1

        for h in hints:
            if h['t'].startswith('delfile::'):
                path = files[h['n']]
                assert path.exists(), 'path={path} hint={h}'

        instances_per_file = {}
        instances_per_depth = {}
        for h in hints:
            file_id, _path = hint_main_file(h)
            if file_id >= len(files):
                continue  # the hint points to a temporary file
            t = h['t']
            if t not in instances_per_file:
                instances_per_file[t] = [0] * len(files)
            instances_per_file[t][file_id] += 1
            if t not in instances_per_depth:
                instances_per_depth[t] = []
            if depth_per_file[file_id] >= len(instances_per_depth[t]):
                instances_per_depth[t] += [0] * (depth_per_file[file_id] - len(instances_per_depth[t]) + 1)
                assert depth_per_file[file_id] < len(instances_per_depth[t])
            instances_per_depth[t][depth_per_file[file_id]] += 1

        with pyzstd.open(self.extra_file_path, 'wt') as f:
            f.write(dump_json([str(relative_path(f, test_case)) for f in files]))
            f.write('\n')
            f.write(dump_json(depth_per_file))
            f.write('\n')
            f.write(dump_json(instances_per_file))
            f.write('\n')
            for hint in hints:
                f.write(dump_json(hint))
                f.write('\n')

        if not hints:
            logging.info(f'Generated hints for arg={self.arg}: 0')
            return None

        if last_state_hint:
            state = PolyState.create_from_hint(instances, last_state_hint, repr(self))
        else:
            state = PolyState.create(instances, repr(self))
        if state:
            state.extra_file_path = self.extra_file_path
            for tp in state.types:
                state[tp].instances_per_depth = instances_per_depth.get(tp, [])
            state.has_meta_hints = any(is_hint_meta_type(h['t']) for h in hints)

        logging.info(f'Generated hints for arg={self.arg}: {instances}')

        return state

    def advance(self, test_case, state):
        return state.advance(success_histories)

    def on_success_observed(self, state, improv):
        if not isinstance(state, list):
            state.on_success_observed(improv)

    def get_state_hint_sources(self, test_case, state_list):
        depth_cache = {}
        sources = []
        for s in state_list:
            begin = s.begin()
            end = s.end()
            if s[s.get_type()].tp == 2:
                if s.get_type() not in depth_cache:
                    void_state = copy.copy(s[s.get_type()])
                    void_state.tp = 0
                    void_state.index = 0
                    void_state.chunk = 0
                    void_state.extra_file_path = s.extra_file_path
                    _files, depth_per_file, instances_per_file, _hints = load_hints([void_state], test_case)
                    depth_cache[s.get_type()] = depth_per_file, instances_per_file
                depth_per_file, instances_per_file = depth_cache[s.get_type()]
                if s.get_type() not in instances_per_file:
                    return None
                global_shift = s.shift()
                assert s.get_type() in instances_per_file, f'type={s.get_type()} begin={begin} end={end} global_shift={global_shift} instances_per_file_keys={list(instances_per_file.keys())}'
                begin, end = recalc_per_depth_ordering(begin-global_shift, end-global_shift, depth_per_file, instances_per_file[s.get_type()], s[s.get_type()].instances_per_depth)
                begin += global_shift
                end += global_shift
                # logging.info(f'RECALC: old_begin={s.begin()} old_end={s.end()} begin={begin} end={end}')
            sources.append((s.extra_file_path, begin, end))
        return sources

    def transform(self, test_case, state, process_event_notifier, original_test_case):
        test_case = Path(test_case)

        if not isinstance(state, list) and state.ptr == -1:
            return (PassResult.INVALID, state)

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'{self}.transform: state={state}')
        state_list = state if isinstance(state, list) else [state]
        for s in state_list:
            s.improv = 0
            s.set_dbg(None)

        command = [
            self.external_programs['hint_tool'],
            'transform',
            str(original_test_case),
            str(test_case),
        ]
        sources = self.get_state_hint_sources(test_case, state_list)
        if sources is None:
            return (PassResult.INVALID, state)
        for extra_file_path, begin, end in sources:
            command += [str(extra_file_path), str(begin), str(end)]
        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        if proc.returncode == 1:
            # Graceful error, e.g., a hint spanning beyond the end-of-file.
            return (PassResult.INVALID, state)
        if proc.returncode != 0:
            raise RuntimeError(f'hint_tool failed: command:\n{shlex.join(command)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}')
        improv, _improv_file_count = proc.stdout.strip().split()

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'hint_tool stderr:\n{proc.stderr}')
        state_list[0].improv = int(improv)
        return (PassResult.OK, state)

    def predict_improv(self, test_case, state):
        test_case = Path(test_case)
        state_list = state if isinstance(state, list) else [state]
        command = [
            self.external_programs['hint_tool'],
            'dry-run',
            str(test_case),
        ]
        for extra_file_path, begin, end in self.get_state_hint_sources(test_case, state_list):
            command += [str(extra_file_path), str(begin), str(end)]
        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        if proc.returncode == 1:
            # Graceful error, e.g., a hint spanning beyond the end-of-file.
            return 0, 0
        if proc.returncode != 0:
            raise RuntimeError(f'hint_tool failed: command:\n{shlex.join(command)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}')
        improv, improv_file_count = proc.stdout.strip().split()
        return int(improv), int(improv_file_count)

def load_hints(state_list, test_case, load_all=False):
    hints_to_load = {}
    for s in state_list:
        set_for_file = hints_to_load.setdefault(s.extra_file_path, set())
        if not load_all:
            set_for_file |= set(range(s.begin(), s.end()))

    hints = []
    for extra_file_path, hint_ids in hints_to_load.items():
        min_hint_id = None if load_all or not hint_ids else min(hint_ids)
        max_hint_id = None if load_all or not hint_ids else max(hint_ids)
        with pyzstd.open(extra_file_path, 'rt') as f:
            files = json.loads(next(f))
            files = [join_to_test_case(test_case, f) for f in files]
            depth_per_file = json.loads(next(f))
            instances_per_file = json.loads(next(f))
            if load_all or hint_ids:
                for i, line in enumerate(f):
                    should_load = False
                    if load_all:
                        should_load = True
                    else:
                        if i < min_hint_id:
                            continue
                        if i > max_hint_id:
                            break
                        should_load = i in hint_ids
                    if should_load:
                        hints.append(json.loads(line))
    return files, depth_per_file, instances_per_file, hints

def recalc_per_depth_ordering(begin, end, depth_per_file, instances_per_file, instances_per_depth):
    for file_id, _ in sorted(enumerate(depth_per_file), key=lambda i_d: (i_d[1], i_d[0])):
        instances_here = instances_per_file[file_id]
        if begin >= instances_here:
            begin -= instances_here
            end -= instances_here
            continue
        end = min(end, instances_here)
        shift = sum(instances_per_file[:file_id])
        return begin + shift, end + shift
    assert False

def generate_makefile_hints(test_case, files, file_to_id):
    if not test_case.is_dir():
        return None

    targets = {}
    file_to_generating_targets = {}
    file_mentions = {}
    token_to_locs = {}
    phony_targets = []

    makefile_path = test_case / 'Makefile'
    if not makefile_path.exists():
        return []
    makefile_file_id = file_to_id[makefile_path]
    hints = []
    with open(makefile_path) as f:
        line_start_pos = 0
        cur_target = None
        for line in f:
            line_end_pos = line_start_pos + len(line)

            if line.strip() and not line.startswith('\t') and ':' in line:
                cur_target_name = line.split(':')[0].strip()
                assert cur_target_name
                target_name_pos = line.find(cur_target_name)
                assert target_name_pos != -1
                if cur_target_name == '.PHONY':
                    phony_targets = line.split(':', maxsplit=2)[1].split()
                is_special_target = cur_target_name in ['.PHONY', 'clean', '.ALWAYS'] + phony_targets
                if not is_special_target:
                    cur_target = targets.setdefault(cur_target_name, [])
                    cur_target.append({
                        'f': makefile_file_id,
                        'l': line_start_pos,
                        'r': line_end_pos,
                    })
                    file_mentions.setdefault(cur_target_name, []).append({
                        'f': makefile_file_id,
                        'l': line_start_pos + target_name_pos,
                        'r': line_start_pos + target_name_pos + len(cur_target_name),
                    })
                    for dep_name in line.split(':', maxsplit=2)[1].split():
                        mention_pos = line_start_pos + line.find(dep_name)
                        targets.setdefault(dep_name, []).append({
                            'f': makefile_file_id,
                            'l': mention_pos,
                            'r': mention_pos + len(dep_name),
                        })
                        file_mentions.setdefault(dep_name, []).append({
                            'f': makefile_file_id,
                            'l': mention_pos,
                            'r': mention_pos + len(dep_name),
                        })
            elif line.startswith('\t') and not is_special_target:
                assert cur_target
                cur_target.append({
                    'f': makefile_file_id,
                    'l': line_start_pos,
                    'r': line_end_pos,
                })
                tokens = line.split()
                if tokens and tokens[0] == 'mkdir':
                    path = test_case / tokens[-1]
                    if not path.exists() or not list(path.iterdir()):
                        hints.append({
                            't': 'makemkdir',
                            'f': makefile_file_id,
                            'l': line_start_pos,
                            'r': line_end_pos,
                        })
                elif tokens and tokens[0] == '$(CLANG)':
                    token_search_pos = 0
                    arg_of_option = None
                    for i, token in enumerate(tokens):
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
                            is_main_file_for_rule = not match.group(1) and not arg_of_option
                            if is_main_file_for_rule:
                                file_to_generating_targets.setdefault(mentioned_file, []).append(cur_target_name)
                            else:
                                file_mentions.setdefault(mentioned_file, []).append({
                                    'f': makefile_file_id,
                                    'l': mention_pos,
                                    'r': mention_pos + len(token),
                                })
                            arg_of_option = None
                        elif token == '$(EXTRA_CFLAGS)':
                            pass
                        elif i > 0 and token.startswith('-fmodule-name='):
                            name = token.removeprefix('-fmodule-name=')
                            hints.append({
                                'f': makefile_file_id,
                                't': '#symbol',
                                'ns': f'module::{name}',
                                'l': mention_pos + len('-fmodule-name='),
                                'r': mention_pos + len(token),
                            })
                        elif i > 0 and not arg_of_option and token.startswith('--sysroot='):
                            dir_path = test_case / token.removeprefix('--sysroot=')
                            if not dir_path.exists() or not list(dir_path.iterdir()):
                                hints.append({
                                    'f': makefile_file_id,
                                    't': 'makeincldir',
                                    'l': mention_pos,
                                    'r': mention_pos + len(token),
                                })
                        elif i > 0 and not arg_of_option and \
                                token not in ('-c', '-cc1', '-nostdinc++', '-nostdlib++', '-fno-crash-diagnostics', '-ferror-limit=0', '-w', '-Wno-error', '-Xclang=-emit-module', '-xc++', '-fmodules', '-fno-implicit-modules', '-fno-implicit-module-maps', '-Xclang=-fno-cxx-modules', '-Xclang=-fmodule-map-file-home-is-cwd') and \
                                not token.startswith('-std='):
                            if token in ('-o', '-iquote', '-isystem', '-I'):
                                arg_of_option = (token, mention_pos)
                            if token == '-Xclang':
                                next_tokens = line[token_search_pos:].split(maxsplit=2)
                                if next_tokens and next_tokens[0] == '-fallow-pcm-with-compiler-errors':
                                    arg_of_option = (token, mention_pos)
                            if not arg_of_option:
                                token_to_locs.setdefault(token, []).append({
                                    'f': makefile_file_id,
                                    'l': mention_pos,
                                    'r': mention_pos + len(token),
                                })
                        elif arg_of_option and arg_of_option[0] in ('-iquote', '-isystem', '-I'):
                            incl_path = test_case / token
                            if not incl_path.exists() or incl_path.is_dir() and not list(incl_path.iterdir()):
                                hints.append({
                                    'f': makefile_file_id,
                                    't': 'makeincldir',
                                    'l': arg_of_option[1],
                                    'r': mention_pos + len(token),
                                })
                            elif not incl_path.is_dir() and incl_path in file_to_id:
                                # Sometimes the flags are (mis)used to point to specific files.
                                hints.append({
                                    'f': makefile_file_id,
                                    't': '#fileref',
                                    'n': file_to_id[incl_path],
                                    'l': arg_of_option[1],
                                    'r': mention_pos + len(token),
                                })
                            arg_of_option = None
                        else:
                            arg_of_option = None
            else:
                cur_target = None
                cur_target_name = None

            line_start_pos = line_end_pos

    all_mentioned_files = set(list(file_mentions.keys()) + list(file_to_generating_targets.keys()))
    for path in all_mentioned_files:
        full_path = test_case / path
        if full_path.exists():
            assert full_path.suffix not in ('.pcm', '.o', '.tmp', '.ALWAYS')
            chunks = []
            chunks += file_mentions.get(path, [])
            for target_name in file_to_generating_targets.get(path, []):
                chunks += targets[target_name]
                chunks += file_mentions.get(target_name, [])
            assert chunks, f'path={path}'
            h = {
                't': '#fileref',
                'n': file_to_id[full_path],
            }
            set_hint_locs(h, chunks)
            hints.append(h)
        elif path not in ('.ALWAYS',) and Path(path).suffix not in ('.cppmap',):
            # assert full_path.suffix in ('.pcm', '.o', ''), f'File "{full_path}" referenced in the Makefile doesn\'t exist'
            for loc in file_mentions.get(path, []):
                hints.append({
                    't': '#fileref',
                    'ns': path,
                    'f': makefile_file_id,
                    'l': loc['l'],
                    'r': loc['r'],
                })
    for token, locs in sorted(token_to_locs.items()):
        for chunk in locs:
            h = {'t': 'maketok'}
            set_hint_locs(h, [chunk])
            hints.append(h)
    return hints

def generate_cppmaps_hints(test_case, files, file_to_id):
    if not test_case.is_dir():
        return None

    hints = []
    name_to_cppmaps = {}
    cppmap_to_uses = {}
    for cppmap_path in test_case.rglob('*.cppmap'):
        cppmap_hints, top_level_names, headers, uses = parse_cppmap(test_case, cppmap_path, files, file_to_id)
        hints += cppmap_hints
        for name in top_level_names:
            name_to_cppmaps.setdefault(name, []).append(cppmap_path)
        cppmap_to_uses[cppmap_path] = uses
        for header_path, chunks in headers.items():
            h = {
                't': '#fileref',
                'n': file_to_id[test_case / header_path],
                'w': 0.1,
            }
            set_hint_locs(h, chunks)
            hints.append(h)

    for cppmap_path, uses in cppmap_to_uses.items():
        for use_name, chunks in uses.items():
            for other_cppmap_path in name_to_cppmaps.get(use_name, []):
                h = {
                    't': '#fileref',
                    'n': file_to_id[other_cppmap_path],
                }
                set_hint_locs(h, chunks)
                hints.append(h)

    return hints

def parse_cppmap(test_case, cppmap_path, files, file_to_id):
    cppmap_file_id = file_to_id[cppmap_path]
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
                    name = module_match.group(1).strip('"')
                    name_pos = line.find(name)
                    assert name_pos != -1
                    hints.append({
                        't': '#symbol',
                        'ns': f'module::{name}',
                        'f': cppmap_file_id,
                        'l': line_start_pos + name_pos,
                        'r': line_start_pos + name_pos + len(name),
                    })
                    top_level_names.append(name)
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
                        'f': cppmap_file_id,
                        'l': line_start_pos,
                        'r': line_end_pos,
                    })
                    hints.append({
                        't': 'cppmapheader',
                        'f': cppmap_file_id,
                        'l': line_start_pos,
                        'r': line_end_pos,
                    })
                else:
                    hints.append({
                        't': 'cppmapmissingheader',
                        'f': cppmap_file_id,
                        'l': line_start_pos,
                        'r': line_end_pos,
                    })
                nested_module_empty = False

            use_match = re.match(r'\s*use\s+(\S+)\s*', line)
            if not module_match and not header_match and use_match:
                mentioned_module = use_match.group(1).strip('"')
                uses.setdefault(mentioned_module, []).append({
                    'f': cppmap_file_id,
                    'l': line_start_pos,
                    'r': line_end_pos,
                })
                hints.append({
                    't': 'cppmapuse',
                    'f': cppmap_file_id,
                    'l': line_start_pos,
                    'r': line_end_pos,
                })
                nested_module_empty = False

            closing_brace = line.strip() == '}'
            if closing_brace:
                module_depth -= 1
                # assert module_depth >= 0, f'cppmap_path={cppmap_path}'
                if nested_module_start_pos is not None:
                    if nested_module_empty:
                        # Attempt to delete the empty submodule.
                        hints.append({
                            't': 'cppmapmod',
                            'f': cppmap_file_id,
                            'l': nested_module_start_pos,
                            'r': line_end_pos,
                        })
                    # Attempt to inline the submodule's contents into the outer module.
                    hints.append({
                        't': 'cppmapmodinl',
                        'multi': [{
                            'f': cppmap_file_id,
                            'l': nested_module_start_pos,
                            'r': nested_module_start_line_end_pos,
                        }, {
                            'f': cppmap_file_id,
                            'l': line_start_pos,
                            'r': line_end_pos,
                        }]
                    })
                    nested_module_start_pos = None

            if not module_match and not header_match and not use_match and not closing_brace:
                hints.append({
                    't': 'cppmapline',
                    'f': cppmap_file_id,
                    'l': line_start_pos,
                    'r': line_end_pos,
                })

            line_start_pos = line_end_pos

    # assert module_depth == 0, f'cppmap_path={cppmap_path}'
    return hints, top_level_names, headers, uses

def generate_line_hints(test_case, files, file_to_id):
    hints = []
    for file_id, file in enumerate(files):
        if not file.is_symlink() and file.suffix not in ('.makefile', '.cppmap') and file.name not in ('Makefile',):
            with open(file) as f:
                line_start_pos = 0
                try:
                    for line in f:
                        line_end_pos = line_start_pos + len(line)
                        hints.append({
                            't': 'line',
                            'f': file_id,
                            'l': line_start_pos,
                            'r': line_end_pos,
                        })
                        line_start_pos = line_end_pos
                except UnicodeDecodeError as e:
                    logging.info(f'Non-UTF8 file found: {file}')
                    # Skip non-UTF files in this heuristic.
    return hints

def generate_clex_hints(test_case, files, file_to_id, clex_arg, external_programs):
    hints = []
    for file_id, file in enumerate(files):
        if not file.is_symlink() and file.suffix not in ('.makefile', '.cppmap') and file.name not in ('Makefile',):
            idx = -1
            while True:
                command = [str(external_programs['clex']), clex_arg, str(idx), str(file)]
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f'generate_clex_hints: running: {shlex.join(command)}')
                proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
                if proc.returncode == 71:
                    break
                if proc.returncode != 51:
                    raise RuntimeError(f'generate_clex_hints failed: command:\n{shlex.join(command)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}')
                for line in proc.stdout.splitlines():
                    if line.strip():
                        try:
                            h = json.loads(line)
                        except json.decoder.JSONDecodeError as e:
                            raise RuntimeError(f'Error while processing {file}: JSON line "{line}": {e}')
                        if 'multi' in h:
                            for l in h['multi']:
                                l['f'] = file_id
                        else:
                            h['f'] = file_id
                        hints.append(h)
                idx -= 1
    return hints

def generate_topformflat_hints(test_case, files, file_to_id, depth, external_programs):
    hints = []
    for file_id, file in enumerate(files):
        if not file.is_symlink() and file.suffix not in ('.makefile', '.cppmap') and file.name not in ('Makefile',):
            command = [str(external_programs['topformflat']), str(depth), str(file)]
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'generate_topformflat_hints: running: {shlex.join(command)}')
            try:
                out = subprocess.check_output(command, stderr=subprocess.DEVNULL, encoding='utf-8')
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f'generate_topformflat_hints failed: command:\n{shlex.join(command)}\nstdout:\n{e.stdout}\nstderr:\n{e.stderr}')

            with open(file, 'rb') as f:
                file_contents = f.read()

            for line in out.splitlines():
                if line.strip():
                    try:
                        h = json.loads(line)
                        chunk = file_contents[h['l']:h['r']].lstrip()
                        if not chunk:
                            continue
                        h['f'] = file_id
                        hints.append(h)
                    except json.decoder.JSONDecodeError as e:
                        raise RuntimeError(f'Error while processing {file}: JSON line "{line}": {e}')
    return hints

def generate_clang_delta_hints(test_case, files, file_to_id, transformation, external_programs):
    if test_case.is_dir():
        return None

    command = [
        str(external_programs['clang_delta']),
        f'--generate-hints={transformation}',
        str(test_case),
        '--warn-on-counter-out-of-bounds',
        '--std=c++2b',
    ]
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug(f'generate_clang_delta_hints: running: {shlex.join(command)}')
    try:
        out = subprocess.check_output(command, stderr=subprocess.PIPE, encoding='utf-8')
    except subprocess.CalledProcessError as e:
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'generate_clang_delta_hints failed: command:\n{shlex.join(command)}\nstdout:\n{e.stdout}\nstderr:\n{e.stderr}')
        return []
    hints = []
    file_id = file_to_id[test_case]
    for line in out.splitlines():
        if not line.strip():
            continue
        h = json.loads(line)
        h['t'] = transformation
        if 'multi' in h:
            for l in h['multi']:
                l['f'] = file_id
        else:
            h['f'] = file_id
        hints.append(h)
    return hints

def generate_inclusion_directive_hints(test_case, files, file_to_id, external_programs):
    if not test_case.is_dir():
        return None

    resource_dir = get_clang_resource_dir()
    hints = []
    seen_lines = set()
    for orig_command in get_all_compile_commands(test_case):
        main_file_candidates = list(filter(
            lambda s: '=' not in s and (s.endswith('.cc') or s.endswith('.cppmap')),
            orig_command.split()))
        assert len(main_file_candidates) == 1, f'main_file_candidates={main_file_candidates} orig_command={orig_command}'
        main_file = main_file_candidates[0]

        file_to_line_pos = {}
        def get_line_pos_in_file(file, idx):
            if file not in file_to_line_pos:
                lines_pos = []
                with open(file) as f:
                    line_start_pos = 0
                    for line in f:
                        line_end_pos = line_start_pos + len(line.rstrip())
                        lines_pos.append((line_start_pos, line_end_pos))
                        line_start_pos += len(line)
                file_to_line_pos[file] = lines_pos
            return file_to_line_pos[file][idx]

        orig_command = orig_command.replace('$(CLANG)', '')
        orig_command = re.sub(r'\S*-fmodule-map-file=\S*', '', orig_command)
        orig_command = re.sub(r'\S*-fmodule-file\S*', '', orig_command)
        command = [
            str(external_programs['inclusion-graph']),
            str(main_file),
            '--',
            f'-resource-dir={resource_dir}'] + orig_command.split()
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'generate_inclusion_directive_hints: running: {shlex.join(command)}')
        proc = subprocess.run(command, cwd=test_case, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        if proc.returncode:
            logging.debug(f'generate_inclusion_directive_hints: stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}')
            continue
        abs_test_case = test_case.resolve()
        for line in proc.stdout.splitlines():
            if not line.strip():
                continue
            if line in seen_lines:
                continue
            seen_lines.add(line)
            from_file, from_line, to_file = line.split(' ')
            if not from_file:
                continue  # links from .cppmap have the empty "from_file"
            from_file = Path(from_file)
            to_file = Path(to_file)

            if to_file.is_relative_to(resource_dir):
                # Ignore #includes to/inside the compiler's resource directory - we never try deleting those headers.
                continue
            if not to_file.is_absolute():
                to_file = abs_test_case / to_file
            # assert to_file.is_relative_to(abs_test_case), f'Error: discovered #include from {from_file} (line {from_line}) to the file {to_file} that is outside both the test case {test_case} and the resource dir {resource_dir}; the command was: {shlex.join(command)}'
            if not to_file.is_relative_to(abs_test_case):
                logging.info(f'Error: discovered #include from "{from_file}" (line {from_line}) to the file "{to_file}" that is outside both the test case {test_case} and the resource dir {resource_dir}; the command was: {shlex.join(command)}')
                assert False
                continue
            to_file = test_case / to_file.resolve().relative_to(abs_test_case)
            assert to_file in file_to_id, f'to_file={to_file} file_to_id={file_to_id}'

            if not from_file.is_absolute():
                from_file = abs_test_case / from_file
            if from_file.is_relative_to(resource_dir):
                hints.append({
                    't': '#fileref',
                    'n': file_to_id[to_file],
                    'f': -1,
                })
                continue
            if not from_file.is_relative_to(abs_test_case):
                logging.info(f'Discovered #include from file "{from_file}" (line {from_line} to file "{to_file}") that is outside both the test case {test_case} and the resource dir {resource_dir}; the command was: {shlex.join(command)}')
                assert False
                continue
            from_file = test_case / from_file.resolve().relative_to(abs_test_case)
            assert from_file in file_to_id, f'from_file={from_file} file_to_id={file_to_id} line="{line}"'

            from_line = int(from_line) - 1
            start_pos, end_pos = get_line_pos_in_file(from_file, from_line)
            hints.append({
                't': '#fileref',
                'n': file_to_id[to_file],
                'f': file_to_id[from_file],
                'l': start_pos,
                'r': end_pos,
            })
    return hints

def get_clang_resource_dir():
    clang_path = Path(os.environ['CLANG'])
    return f'{clang_path.parent.parent}/lib/clang/google3-trunk'

def generate_clang_pcm_lazy_load_hints(test_case, files, file_to_id):
    orig_command = get_root_compile_command(test_case)
    if not orig_command:
        return []
    resource_dir = get_clang_resource_dir()

    with tempfile.NamedTemporaryFile() as tmp_dump:
        with tempfile.TemporaryDirectory(prefix='cvise-clanglazypcm') as tmp_for_copy:
            tmp_copy = Path(tmp_for_copy) / test_case.name
            shutil.copytree(test_case, tmp_copy, symlinks=True)

            # Hack to avoid rebuilding PCMs
            (tmp_copy / '.ALWAYS').touch()

            command = ['make', '-j64']
            extra_env = {
                'EXTRA_CFLAGS': f'-resource-dir={resource_dir} -fno-crash-diagnostics -Xclang -fallow-pcm-with-compiler-errors -ferror-limit=0',
                'CLANG': str(Path.home() / 'clang-toys/clang-fprint-deserialized-declarations'), # TODO: this should land to upstream Clang
            }
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'generate_clang_pcm_lazy_load_hints: running: {shlex.join(command)}\nenv: {extra_env}')
            proc = subprocess.run(command, cwd=tmp_copy, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8', env=os.environ.copy() | extra_env)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'generate_clang_pcm_lazy_load_hints: stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}')

            for f in tmp_copy.rglob('*.o'):
                f.unlink()

            extra_env['EXTRA_CFLAGS'] += f' -Xclang -print-deserialized-declarations-to-file={tmp_dump.name}'
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'generate_clang_pcm_lazy_load_hints: running: {shlex.join(command)}\nenv: {extra_env}')
            proc = subprocess.run(command, cwd=tmp_copy, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8', env=os.environ.copy() | extra_env)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'generate_clang_pcm_lazy_load_hints: stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}')

            if not Path(tmp_dump.name).exists() or not Path(tmp_dump.name).stat().st_size:
                # Likely an old version of Clang, before the switch was introduced.
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f'generate_clang_pcm_lazy_load_hints: no out file created, exiting')
                return []
            path_to_used = {}
            with open(tmp_dump.name) as f:
                file = None
                seg_from = None
                for line in f:
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.debug(f'LINE {line.strip()}')
                    match = re.match(r'required lines in file: (.*)', line)
                    if match:
                        file = Path(match[1])
                        if file.is_relative_to(resource_dir):
                            # Ignore #includes inside the compiler's resource directory - we never try modifying those headers.
                            continue
                        file = tmp_copy / file
                        # assert file.is_relative_to(tmp_copy), f'Error - the file {file} is outside the test case {tmp_copy} and outside the resource dir {resource_dir}; env was: {extra_env}'
                        if not file.is_relative_to(tmp_copy):
                            logging.info(f'generate_clang_pcm_lazy_load_hints: File "{file}" is outside the test case {tmp_copy} and outside the resource dir {resource_dir}; env was: {extra_env}')
                            assert False
                            continue
                        file_rel = file.relative_to(tmp_copy)
                        file = test_case / file_rel
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
    for file in files:
        if file.is_symlink() or file.suffix in ('.makefile', '.cppmap') or file.name in ('Makefile',):
            continue
        segs = path_to_used.get(file, [])
        file_id = file_to_id[file]
        segptr = 0
        hint_type = 'lazypcm' if segs else 'lazypcmwhole'
        with open(file) as f:
            line_start_pos = 0
            try:
                for i, line in enumerate(f):
                    line_end_pos = line_start_pos + len(line)
                    while segptr < len(segs) and segs[segptr][1] < i:
                        segptr += 1
                    if segptr >= len(segs) or not (segs[segptr][0] <= i <= segs[segptr][1]):
                        hints.append({
                            't': hint_type,
                            'f': file_id,
                            'l': line_start_pos,
                            'r': line_end_pos,
                        })
                    line_start_pos = line_end_pos
            except UnicodeDecodeError as e:
                if segs:
                    logging.info(f'Non-UTF8 file found: {file}')
                    # Skip non-UTF files in this heuristic.
    return hints

def generate_clang_pcm_minimization_hints(test_case, files, file_to_id):
    if not test_case.is_dir():
        return None
    if not os.environ.get('CLANG'):
        return []
    orig_command = get_root_compile_command(test_case)
    if not orig_command:
        return []
    resource_dir = get_clang_resource_dir()

    with tempfile.NamedTemporaryFile() as tmp_dump:
        with tempfile.TemporaryDirectory(prefix='cvise-clanglazypcm') as tmp_for_copy:
            tmp_copy = Path(tmp_for_copy) / test_case.name
            shutil.copytree(test_case, tmp_copy, symlinks=True)

            # Hack to avoid rebuilding PCMs
            (tmp_copy / '.ALWAYS').touch()

            command = ['make', '-j64']
            extra_env = {
                'EXTRA_CFLAGS': f'-fno-crash-diagnostics -Xclang -fallow-pcm-with-compiler-errors -ferror-limit=0 -w',
            }
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'generate_clang_pcm_minimization_hints: running: {shlex.join(command)}\nenv: {extra_env}')
            proc = subprocess.run(command, cwd=tmp_copy, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8', env=os.environ.copy() | extra_env)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'generate_clang_pcm_minimization_hints: stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}')

            for f in tmp_copy.rglob('*.o'):
                f.unlink()

            extra_env['EXTRA_CFLAGS'] += f' -Xclang=-dump-minimization-hints={tmp_dump.name}'
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'generate_clang_pcm_minimization_hints: running: {shlex.join(command)}\nenv: {extra_env}')
            proc = subprocess.run(command, cwd=tmp_copy, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8', env=os.environ.copy() | extra_env)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f'generate_clang_pcm_minimization_hints: stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}')

            if not Path(tmp_dump.name).exists() or not Path(tmp_dump.name).stat().st_size:
                # Likely an old version of Clang, before the switch was introduced.
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f'generate_clang_pcm_minimization_hints: no out file created, exiting')
                return []
            with open(tmp_dump.name) as df:
                clang_hints = json.load(df)

    hints = []
    required_ranges = {}
    for item in clang_hints['required_ranges']:
        file = Path(item['file'])
        if file.is_relative_to(resource_dir):
            # Ignore #includes inside the compiler's resource directory - we never try modifying those headers.
            continue
        file = tmp_copy / file
        if not file.is_relative_to(tmp_copy):
            logging.info(f'generate_clang_pcm_minimization_hints: File "{file}" is outside the test case {tmp_copy} and outside the resource dir {resource_dir}; env was: {extra_env}')
            continue
        file_rel = file.resolve().relative_to(tmp_copy)
        file = test_case / file_rel
        assert file in file_to_id, f'Unexpected file "{file}"'
        file_id = file_to_id[file]
        required_ranges[file_id] = item['range']

    hints = []
    for file_id, file in enumerate(files):
        if file.is_symlink() or file.suffix in ('.makefile', '.cppmap') or file.name in ('Makefile',):
            continue

        lines = []
        line_blocklisted = []
        with open(file, 'rb') as f:
            line_start_pos = 0
            in_preprocessor = False
            for line in f:
                line_end_pos = line_start_pos + len(line)
                lines.append((line_start_pos, line_end_pos))

                if line.lstrip().startswith(b'#'):
                    in_preprocessor = True
                line_blocklisted.append(in_preprocessor)
                if not line.rstrip().endswith(b'\\'):
                    in_preprocessor = False

                line_start_pos = line_end_pos

        ranges = required_ranges.get(file_id, [])
        cut_line = 0
        cut_col = 0
        lines_to_remove = []
        for range in ranges:
            from_line = range['from']['line'] - 1
            from_col = range['from']['column'] - 1
            to_line = range['to']['line'] - 1
            to_col = range['to']['column'] - 1
            # assert (from_line, from_col) < (to_line, to_col), f'Hint with negative size: {range}'
            if (from_line, from_col) >= (to_line, to_col):
                logging.info(f'Ignoring hint with negative size: {range}')
                continue
            assert (to_line, to_col) <= (len(lines), 0), f'Hint beyond end of file: {range}, len(lines)={len(lines)}'
            assert from_col < lines[from_line][1] - lines[from_line][0], f'Hint column number beyond end of line: {range}, line={lines[from_line]}'
            assert to_col <= lines[to_line][1] - lines[to_line][0], f'Hint column number beyond end of line: {range}, line={lines[to_line]}'
            assert (cut_line, cut_col) <= (from_line, from_col), f'Hint violating order: range={range}, cut_line={cut_line}, cut_col={cut_col}, file={file}'
            while cut_line < from_line:
                if not line_blocklisted[cut_line]:
                    lines_to_remove.append((lines[cut_line][0] + cut_col, lines[cut_line][1]))
                cut_line += 1
                cut_col = 0
            assert cut_line == from_line
            assert cut_col <= from_col
            if not line_blocklisted[cut_line]:
                lines_to_remove.append((lines[cut_line][0] + cut_col, lines[cut_line][0] + from_col))
            cut_line = to_line
            cut_col = to_col
        while cut_line < len(lines):
            if not line_blocklisted[cut_line]:
                lines_to_remove.append((lines[cut_line][0] + cut_col, lines[cut_line][1]))
            cut_line += 1
            cut_col = 0
        for l, r in lines_to_remove:
            assert l <= r
        lines_to_remove = [(l, r) for l, r in lines_to_remove if l < r]
        if not lines_to_remove:
            continue

        hint_type_prefix = 'clang_pcm_hints' if ranges else 'clang_pcm_hints_unused_file'
        for pos_l, pos_r in lines_to_remove:
            hints.append({
                't': f'{hint_type_prefix}_rowwise',
                'f': file_id,
                'l': pos_l,
                'r': pos_r,
            })
        hints.append({
            't': f'{hint_type_prefix}_filewise',
            'multi': [
                {'f': file_id, 'l': pos_l, 'r': pos_r} for pos_l, pos_r in lines_to_remove
            ],
        })

    return hints

def generate_line_markers_hints(test_case, files, file_to_id):
    if test_case.is_dir():
        return None

    line_regex = re.compile('^\\s*#\\s*[0-9]+')
    path = files[0]
    file_id = file_to_id[path]
    hints = []
    with open(path) as f:
        line_start_pos = 0
        for line in f:
            line_end_pos = line_start_pos + len(line)
            if line_regex.search(line):
                hints.append({
                    't': 'linemarker',
                    'f': file_id,
                    'l': line_start_pos,
                    'r': line_end_pos,
                })
            line_start_pos = line_end_pos
    return hints

def generate_blank_hints(test_case, files, file_to_id):
    generic_patterns = {
        'blankline': r'^\s*\n$',
        'blankconseq': r'[ \t]+(?=[ \t])',
        'blanktrail': r'[ \t]+(?=\n)',
    }
    non_makefile_patterns = {
        'blanklead': r'^[ \t]+',
        'preprcond': r'^[ \t]*\#[ \t]*(if|else|end).*',
    }
    hints = []
    for file_id, path in enumerate(files):
        patterns = copy.copy(generic_patterns)
        if path.name != 'Makefile':
            patterns.update(non_makefile_patterns)
        with open(path) as f:
            line_start_pos = 0
            for line in f:
                line_end_pos = line_start_pos + len(line)
                for type, pattern in patterns.items():
                    for match in re.finditer(pattern, line):
                        hints.append({
                            't': type,
                            'f': file_id,
                            'l': line_start_pos + match.start(),
                            'r': line_start_pos + match.end(),
                        })
                line_start_pos = line_end_pos
    return hints

def generate_tree_sitter_delta_hints(test_case, files, file_to_id, external_programs):
    command = [str(external_programs['tree-sitter-delta'])]
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug(f'generate_tree_sitter_delta_hints: running: {shlex.join(command)}')
    paths = '\n'.join(str(f) for f in files if is_utf8_file(f))
    try:
        out = subprocess.check_output(command, input=paths, stderr=subprocess.PIPE, encoding='utf-8')
    except subprocess.CalledProcessError as e:
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'generate_tree_sitter_delta_hints failed: command:\n{shlex.join(command)}\nstdout:\n{e.stdout}\nstderr:\n{e.stderr}')
        return []
    hints = []
    for line in out.splitlines():
        if not line.strip():
            continue
        h = json.loads(line)
        hints.append(h)
    return hints

def generate_delete_file_hints(test_case, files, file_to_id, other_init_states):
    if not test_case.is_dir():
        return None

    states_to_load = [s for s in other_init_states if s]
    if not states_to_load:
        return []
    files, depth_per_file, instances_per_file, old_hints = load_hints(states_to_load, test_case, load_all=True)

    imperishable_files = set()
    file_to_locs = {}
    file_to_weight = {}
    for h in old_hints:
        if h['t'] == '#fileref' and 'n' in h:
            file_id = h['n']
            if h.get('f') == -1:
                imperishable_files.add(file_id)
            else:
                file_to_locs.setdefault(file_id, []).extend(get_hint_locs(h))
                file_to_weight[file_id] = file_to_weight.get(file_id, 0) + h.get('w', 1)

    hints = []
    for file_id, locs in file_to_locs.items():
        if file_id in imperishable_files:
            continue
        w = file_to_weight[file_id]
        if 0 < w < 1 and not w.is_integer():
            w = 1
        else:
            w = 2 if int(w) > 0 else 0
        h = {
            't': f'delfile::{w}',
            'n': file_id,
        }
        set_hint_locs(h, locs)
        hints.append(h)

    for file_id, file in enumerate(files):
        if file_id not in file_to_locs and file_id not in imperishable_files:
            if file.suffix not in ('.makefile',) and file.name not in ('Makefile',):  # Hack
                hints.append({
                    't': 'delfile::0',
                    'n': file_id,
                    'multi': [],
                })

    for path in test_case.rglob('*'):
        if path.is_dir() and not path.is_symlink() and not list(path.iterdir()):
            hints.append({
                't': 'rmdir',
                'ns': str(relative_path(path, test_case)),
                'multi': [],
            })

    return hints

def generate_inline_file_hints(test_case, files, file_to_id, other_init_states):
    if not test_case.is_dir():
        return None

    states_to_load = [s for s in other_init_states if s]
    if not states_to_load:
        return []
    files, depth_per_file, instances_per_file, old_hints = load_hints(states_to_load, test_case, load_all=True)

    file_to_locs = {}
    for h in old_hints:
        if h['t'] == '#fileref' and 'n' in h and h.get('w', 1) == 1:
            file_id = h['n']
            file_to_locs.setdefault(file_id, []).extend(get_hint_locs(h))

    hints = []
    for file_id, locs in file_to_locs.items():
        if len(locs) == 1:
            loc = locs[0]
            hints.append({
                't': 'inlinefile',
                'multi': [
                    {'f': loc['f'], 'l': loc['l'], 'r': loc['r'], 'vf': file_id},
                    {'f': file_id, 'l': 0, 'r': files[file_id].stat().st_size},
                ],
            })
    return hints

def load_text(path, index_l, index_r):
    with open(path, 'rt') as f:
        f.seek(index_l)
        return f.read(index_r - index_l)

def generate_rename_file_hints(test_case, files, file_to_id, other_init_states):
    if not test_case.is_dir():
        return None

    RND_VOCAB = string.ascii_uppercase + string.digits
    RND_LEN = 3

    states_to_load = [s for s in other_init_states if s]
    if not states_to_load:
        return []
    files, depth_per_file, instances_per_file, old_hints = load_hints(states_to_load, test_case, load_all=True)

    file_to_locs = {}
    for h in old_hints:
        if h['t'] == '#fileref':
            path = files[h['n']] if 'n' in h else test_case / h['ns']
            file_to_locs.setdefault(path, []).extend(get_hint_locs(h))

    hints = []
    for path, locs in file_to_locs.items():
        if len(path.stem) != RND_LEN or any(c not in RND_VOCAB for c in path.stem):
            rel_path = relative_path(path, test_case)
            new_name = ''.join(random.choices(RND_VOCAB, k=RND_LEN)) + path.suffix
            edits = []
            if path in file_to_id:
                edits.append({
                    'f': file_to_id[path],
                    't': 'mv',
                    'v': new_name,
                })
            for loc in locs:
                loc_path = files[loc['f']]
                orig_text = load_text(loc_path, loc['l'], loc['r'])
                replacement_text = orig_text.replace(str(rel_path), new_name)
                if loc_path.name not in ('Makefile',) and loc_path.suffix not in ('.cppmap',) and '#include' in orig_text:
                    replacement_text = f'#include "{new_name}"'
                elif orig_text == replacement_text:
                    continue
                edits.append({
                    'f': loc['f'],
                    'l': loc['l'],
                    'r': loc['r'],
                    'v': replacement_text,
                })
            hints.append({'t': 'renamefile', 'multi': edits})
    return hints

def generate_rename_symbol_hints(test_case, files, file_to_id, other_init_states):
    RND_VOCAB = string.ascii_lowercase
    RND_MIN_LEN = 1
    RND_MAX_LEN = 3

    states_to_load = [s for s in other_init_states if s]
    if not states_to_load:
        return []
    files, depth_per_file, instances_per_file, old_hints = load_hints(states_to_load, test_case, load_all=True)

    file_to_ref_locs = {}
    for h in old_hints:
        if h['t'] == '#fileref':
            for loc in get_hint_locs(h):
                file_to_ref_locs.setdefault(loc['f'], []).append(loc)

    for file_id, path in enumerate(files):
        if path.name not in ('Makefile',) and path.suffix not in ('.cppmap',):
            file_symbols = parse_file_symbols(file_id, path, file_to_ref_locs.get(file_id, []))
            old_hints += file_symbols

    symbol_to_locs = {}
    for h in old_hints:
        if h['t'] == '#symbol':
            symbol = h['ns']
            symbol_to_locs.setdefault(symbol, []).extend(get_hint_locs(h))

    occupied_names = set(symbol_to_locs.keys())

    def generate_next_free_name():
        while True:
            len = random.randint(RND_MIN_LEN, RND_MAX_LEN)
            name = ''.join(random.choices(RND_VOCAB, k=len))
            if name not in occupied_names:
                occupied_names.add(name)
                return name

    hints = []
    for symbol, locs in sorted(symbol_to_locs.items(), key=lambda s: (len(s), s)):
        if not (RND_MIN_LEN <= len(symbol) <= RND_MAX_LEN) or any(c not in RND_VOCAB for c in symbol):
            new_name = generate_next_free_name()
            orig_len = locs[0]['r'] - locs[0]['l']
            if len(new_name) >= orig_len:
                occupied_names.remove(new_name)
                continue
            hints.append({
                't': 'renamesymbol',
                'multi': [
                    {
                        'f': loc['f'],
                        'l': loc['l'],
                        'r': loc['r'],
                        'v': new_name,
                    } for loc in locs
                ]
            })
    return hints

def parse_file_symbols(file_id, path, filerefs):
    def inside_fileref(pos):
        for loc in filerefs:
            if loc['l'] <= pos < loc['r']:
                return True
        return False

    hints = []
    with open(path, 'rt') as f:
        line_start_pos = 0
        for line in f:
            tok_start_pos = 0
            for i, c in enumerate(line + ' '):
                if not c.isalnum() and c != '_':
                    if i > tok_start_pos and not inside_fileref(line_start_pos + tok_start_pos) and \
                        '#include' not in line:  # HACK
                        hints.append({
                            'f': file_id,
                            't': '#symbol',
                            'ns': line[tok_start_pos:i],
                            'l': line_start_pos + tok_start_pos,
                            'r': line_start_pos + i,
                        })
                    tok_start_pos = i + 1
            line_start_pos += len(line)
    return hints

def generate_splice_hints(test_case, files, file_to_id, types_left, types_right, result_type, other_init_states):
    if not test_case.is_dir():
        return None

    states_to_load = [s for s in other_init_states if s]
    if not states_to_load:
        return []
    files, depth_per_file, instances_per_file, old_hints = load_hints(states_to_load, test_case, load_all=True)

    left_hints = []
    right_hints = []
    for h in old_hints:
        if h['t'] in types_left:
            if left_hints and left_hints[-1]['multi'][-1]['f'] == h['f'] and left_hints[-1]['multi'][-1]['r'] == h['l']:
                left_hints[-1]['multi'] += get_hint_locs(h)
            else:
                left_hints.append({
                    't': h['t'],
                    'multi': get_hint_locs(h),
                })
        elif h['t'] in types_right:
            right_hints.append(h)

    def starts_before(hint, base_hint):
        hint_loc = get_hint_locs(hint)[0]
        base_loc = get_hint_locs(base_hint)[0]
        return (hint_loc['f'], hint_loc['l']) < (base_loc['f'], base_loc['l'])

    def ends_not_later(hint, base_hint):
        hint_loc = get_hint_locs(hint)[-1]
        base_loc = get_hint_locs(base_hint)[-1]
        return (hint_loc['f'], hint_loc['r']) <= (base_loc['f'], base_loc['r'])

    cur_file = None
    cur_file_data = None
    def trivial_chunk(hint):
        nonlocal cur_file
        nonlocal cur_file_data
        if cur_file != hint['f']:
            cur_file = hint['f']
            with open(files[cur_file], 'rb') as f:
                cur_file_data = f.read()
        for loc in get_hint_locs(hint):
            if cur_file_data[loc['l']:loc['r']].lstrip() != b'':
                return False
        return True

    new_hints = []
    right_hint_ptr = 0
    for h in left_hints:
        while right_hint_ptr < len(right_hints) and starts_before(right_hints[right_hint_ptr], h):
            right_hint_ptr += 1
        while right_hint_ptr < len(right_hints) and ends_not_later(right_hints[right_hint_ptr], h):
            if not trivial_chunk(right_hints[right_hint_ptr]):
                new_hint = copy.copy(right_hints[right_hint_ptr])
                new_hint['t'] = result_type
                new_hints.append(new_hint)
            right_hint_ptr += 1
    return new_hints

def get_root_compile_command(test_case):
    makefile_path = test_case / 'Makefile'
    if not makefile_path.exists():
        return None
    with open(makefile_path) as f:
        lines = f.readlines()
        for i, l in enumerate(lines):
            if '.o:' in l:
                p = i + 1
                while p < len(lines) and lines[p].strip().lstrip('@').lstrip().split()[0] != '$(CLANG)':
                    p += 1
                if p >= len(lines):
                    return None
                return lines[p].strip().lstrip('@').lstrip()
        else:
            return None

def get_all_compile_commands(test_case):
    makefile_path = test_case / 'Makefile'
    if not makefile_path.exists():
        return None
    commands = []
    with open(makefile_path) as f:
        for l in f:
            if l.strip().split(maxsplit=2)[0] == '$(CLANG)':
                commands.append(l)
    return commands

def get_ordered_files_list(test_case):
    if not test_case.is_dir():
        return [test_case]

    files = [f for f in Path(test_case).rglob('*') if not f.is_dir() and not f.is_symlink()]

    def is_unexpected_file(f):
        return f.suffix in ('.pcm', '.o', '.tmp') or f.name == '.ALWAYS'
    assert all(not is_unexpected_file(f) for f in files), f'Unexpected files found in the input: {", ".join(str(f) for f in files if is_unexpected_file(f))}'

    files.sort()
    return files

def get_path_to_depth(test_case, external_programs):
    orig_command = get_root_compile_command(test_case)
    if not orig_command:
        return {}

    resource_dir = get_clang_resource_dir()
    root_file_candidates = list(test_case.rglob('*.cc'))
    root_file = root_file_candidates[0] if root_file_candidates else None

    path_to_depth = {}
    if root_file and orig_command:
        orig_command = re.sub(r'\S*-fmodule\S*', '', orig_command).split()
        command = [
            str(external_programs['calc-include-depth']),
            str(root_file.relative_to(test_case)),
            '--',
            f'-resource-dir={resource_dir}'] + orig_command
        path_and_depth = []
        stderr = subprocess.PIPE if logging.getLogger().isEnabledFor(logging.DEBUG) else subprocess.DEVNULL
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'get_path_to_depth: running: {shlex.join(command)}')
        proc = subprocess.run(command, cwd=test_case, stdout=subprocess.PIPE, stderr=stderr, encoding='utf-8')
        for line in proc.stdout.splitlines():
            if not line.strip():
                continue
            path, depth = line.rsplit(maxsplit=1)
            path = Path(path)
            if not path.is_absolute():
                path = test_case.resolve() / path
            if path.is_relative_to(resource_dir):
                # Ignore #includes inside the compiler's resource directory - we never try modifying those headers.
                continue
            # assert path.is_relative_to(test_case.resolve()), f'{path} doesnt belong to {test_case}'
            if not path.is_relative_to(test_case.resolve()):
                logging.info(f'get_path_to_depth: {path} doesnt belong to {test_case}')
                assert False, f'{path} doesnt belong to {test_case}'
                continue
            assert path.exists(), f'doesnt exist: {path}'
            path_and_depth.append((path.resolve(), int(depth)))
        if not path_and_depth and logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f'get_path_to_depth: stderr:\n{proc.stderr}')
        path_to_depth = dict(path_and_depth)

    return path_to_depth

def dump_json(o):
    return json.dumps(o, separators=(',', ':'), check_circular=False)

def assert_valid_hint(h, files, declared_types):
    try:
        assert isinstance(h, dict)
        assert 't' in h
        assert isinstance(h['t'], str)
        assert h['t'] in declared_types, f'type not among declared types ({declared_types})'
        if h['t'].startswith('delfile::') or h['t'] == '#fileref':
            assert 'n' in h or 'ns' in h
            if 'n' in h:
                assert isinstance(h['n'], int)
                assert 0 <= h['n'] < len(files)
            if 'ns' in h:
                assert isinstance(h['ns'], str)
        if h['t'] == '#symbol':
            assert 'ns' in h
            assert isinstance(h['ns'], str)
            assert h['ns']
        if 'f' in h and h['f'] == -1 and h['t'] == '#fileref':
            pass  # an imperishable fileref
        elif 'l' in h or 'r' in h or 'v' in h:
            assert 'multi' not in h
            assert 'f' in h
            assert isinstance(h['f'], int)
            assert 0 <= h['f'] < len(files)
            assert 'l' in h
            assert isinstance(h['l'], int)
            assert 0 <= h['l']
            assert 'r' in h
            assert isinstance(h['r'], int)
            assert h['l'] <= h['r']
            if 'v' in h:
                assert isinstance(h['v'], str)
        elif 'multi' in h:
            for l in h['multi']:
                assert isinstance(l, dict)
                assert 'multi' not in l
                assert 'f' in l
                assert isinstance(l['f'], int)
                assert 0 <= l['f'] < len(files)
                if 't' in l:
                    assert isinstance(l['t'], str)
                if l.get('t') not in ('mv',):
                    assert 'l' in l
                    assert isinstance(l['l'], int)
                    assert 0 <= l['l']
                    assert 'r' in l
                    assert isinstance(l['r'], int)
                    assert l['l'] <= l['r']
                if 'v' in l:
                    assert isinstance(l['v'], str)
                if 'vf' in l:
                    assert isinstance(l['vf'], int)
                    assert 0 <= l['vf'] < len(files)
        else:
            assert False
    except AssertionError as e:
        raise RuntimeError(f'Invalid hint: {h}')

def get_hint_locs(hint):
    if 'multi' in hint:
        return hint['multi']
    if hint['f'] == -1:
        return []
    l = {
        'f': hint['f'],
        'l': hint['l'],
        'r': hint['r'],
    }
    if 'v' in hint:
        l['v'] = hint['v']
    return [l]

def set_hint_locs(hint, locs):
    if len(locs) == 1:
        l = locs[0]
        hint['f'] = l['f']
        hint['l'] = l['l']
        hint['r'] = l['r']
    else:
        hint['multi'] = locs

def merge_chunks(chunks):
    result = []
    for c in sorted(chunks, key=lambda c: (c['l'], 'v' in c)):
        if result and result[-1]['r'] > c['l']:
            result[-1]['r'] = max(result[-1]['r'], c['r'])
        else:
            result.append(c)
    return result

def debug_dump_hints(hints, files):
    dump = ''
    for hint in hints:
        dump += f'*** {hint["t"]}\n'
        for c in get_hint_locs(hint):
            dump += f'    in {files[c["f"]]}:'
            with open(files[c["f"]]) as f:
                contents = f.read()
            if 'v' in c:
                dbg1 = contents[max(0, c['l'] - 10): c['l']].replace('\n', '\\n')
                dbg2 = contents[c['l']:c['r']].replace('\n', '\\n')
                dbg3 = contents[c['r']: min(len(contents), c['r'] + 10)].replace('\n', '\\n')
                dump += f'       REP {c["l"]}..{c["r"]} with "{c["v"]}": "{dbg1}" >>> "{dbg2}" <<< "{dbg3}"\n'
            else:
                dbg = contents[c['l']:c['r']].replace('\n', '\\n')
                dump += f'       DEL {c["l"]}..{c["r"]}: "{dbg}"\n'
    logging.info(dump)
