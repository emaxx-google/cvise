import logging
from pathlib import Path
import json
import pyzstd

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

def relative_path(path, test_case):
    if test_case.is_dir():
        return path.relative_to(test_case)
    return path.name

def join_to_test_case(test_case, path):
    if test_case.is_dir():
        return test_case / path
    assert test_case.name == Path(path).name
    return test_case

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

def apply_hints(hints, files, src_path, dest_path, dry_run=False):
    # Group edits by path.
    files_for_deletion = set(h['n'] for h in hints if h['t'].startswith('delfile::'))
    dirs_for_deletion = set(src_path / h['ns'] for h in hints if h['t'].startswith('rmdir'))
    file_to_edits = {}
    for h in hints:
        for l in get_hint_locs(h):
            if l['f'] not in files_for_deletion:
                file_to_edits.setdefault(l['f'], []).append(l)

    # Replicate the directory structure.
    reduction_file_count = 0
    for f in src_path.rglob('*'):
        if f.is_dir():
            if f in dirs_for_deletion:
                # logging.debug(f'skipping deleted dir {f}')
                reduction_file_count += 1
                continue
            dir_dest = dest_path / f.relative_to(src_path)
            if not dry_run:
                # logging.debug(f'creating dir {dir_dest}')
                dir_dest.mkdir()

    # Write modified files and symlink the unmodified ones.
    total_reduction = 0
    for file_id, path_to in enumerate(files):
        path_from = join_to_test_case(src_path, relative_path(path_to, dest_path)).resolve()
        if file_id in files_for_deletion:
            # logging.debug(f'skipping deleted file {path_to}')
            total_reduction += path_from.stat().st_size
            reduction_file_count += 1
            continue
        if file_id not in file_to_edits:
            # logging.debug(f'creating symlink at {path_to} pointing to {path_from}')
            if not dry_run:
                path_to.symlink_to(path_from)
            continue
        edit_reduction = write_edited_file(path_from, path_to, file_to_edits[file_id], files, dest_path, dry_run)
        if edit_reduction is None:
            return None, None
        total_reduction += edit_reduction

    return total_reduction, reduction_file_count

def write_edited_file(path_from, path_to, edits, files, dest_path, dry_run):
    with open(path_from, 'rb') as f:
        data = f.read()
    global_edits, merged_edits = merge_edits(edits)

    new_data = b''
    ptr = 0
    for e in merged_edits:
        if e['l'] >= len(data):
            logging.warning(f'error: hint {e} spans beyond end of file {path_from}; file size was {len(data)}')
            return None
        new_data += data[ptr:e['l']]
        if 'v' in e:
            new_data += e['v'].encode()
        elif 'vf' in e:
            with open(files[e['vf']], 'rb') as f:
                new_data += f.read()
        ptr = e['r']
        if ptr > len(data):
            logging.warning(f'error: end of hint {e} spans beyond end of file {path_from}; file size was {len(data)}')
            return None
    new_data += data[ptr:]

    write_path = path_to
    for e in global_edits:
        if e.get('t') == 'mv':
            write_path = dest_path / e['v']
        else:
            raise RuntimeError(f'Unexpected hint loc type {e}')
    # logging.debug(f'writing {len(new_data)} bytes (old size {len(data)}) to {"" if path_to == write_path else "moved "} file {write_path}')
    if not dry_run:
        with open(write_path, 'wb') as f:
            f.write(new_data)
    return len(data) - len(new_data)

def merge_edits(edits):
    merged = []
    global_edits = []
    for e in sorted(edits, key=lambda e: (e.get('l', -1), 'v' in e)):
        if 'l' in e:
            if merged and merged[-1]['r'] > e['l']:
                merged[-1]['r'] = max(merged[-1]['r'], e['r'])
            else:
                merged.append(e)
        else:
            global_edits.append(e)
    return global_edits, merged
