use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use walkdir::WalkDir;

#[derive(Debug, PartialEq)]
enum Command {
    Transform,
    DryRun,
}

#[derive(Clone, Debug, Deserialize)]
struct HintLoc {
    f: u32,
    t: Option<String>,
    l: Option<u32>,
    r: Option<u32>,
    v: Option<String>,
    vf: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
struct Hint {
    t: String,
    n: Option<u32>,
    ns: Option<String>,
    f: Option<u32>,
    l: Option<u32>,
    r: Option<u32>,
    v: Option<String>,
    vf: Option<u32>,
    multi: Option<Vec<HintLoc>>,
}

fn load_hints(path: &str, hint_ids: &HashSet<u32>) -> (Vec<PathBuf>, Vec<Hint>) {
    let file = fs::File::open(path).unwrap();
    let reader = BufReader::new(GzDecoder::new(file));
    let mut lines = reader.lines();
    let files: Vec<String> = serde_json::from_str(&lines.next().unwrap().unwrap()).unwrap();
    lines.next(); // depth_per_file
    lines.next(); // instances_per_file
    let mut hints = vec![];
    for (idx, line) in lines.enumerate() {
        if hint_ids.contains(&(idx as u32)) {
            let h = serde_json::from_str(&line.unwrap()).unwrap();
            hints.push(h);
        }
    }
    (files.into_iter().map(|f| PathBuf::from(f)).collect(), hints)
}

fn get_hint_locs(hint: &Hint) -> Vec<HintLoc> {
    match &hint.multi {
        Some(multi) => multi.clone(),
        None => vec![HintLoc {
            f: hint.f.unwrap(),
            t: None,
            l: hint.l,
            r: hint.r,
            v: hint.v.clone(),
            vf: hint.vf,
        }],
    }
}

fn merge_edits(mut edits: Vec<HintLoc>) -> (Vec<HintLoc>, Vec<HintLoc>) {
    let mut merged: Vec<HintLoc> = vec![];
    edits.sort_by_key(|e| (e.l, e.v.is_some()));
    let mut global_edits = vec![];
    for e in edits.into_iter() {
        if !e.l.is_some() || !e.r.is_some() {
            global_edits.push(e);
            continue;
        }
        if merged.len() > 0 && merged.last().unwrap().r.unwrap() > e.l.unwrap() {
            merged.last_mut().unwrap().r = Some(max(merged.last().unwrap().r.unwrap(), e.r.unwrap()));
        } else {
            merged.push(e);
        }
    }
    (global_edits, merged)
}

fn write_edited_file(command: &Command, file_id: usize, edits: Vec<HintLoc>, dest_path: &Path, src_file_paths: &Vec<PathBuf>, dest_file_paths: &Vec<PathBuf>) -> Option<i64> {
    let read_path = &src_file_paths[file_id];
    let data = fs::read(read_path).unwrap();

    let mut new_data = vec![];
    let mut ptr = 0;
    let (global_edits, merged_edits) = merge_edits(edits);
    for e in merged_edits {
        if e.l.unwrap() as usize >= data.len() {
            eprintln!("error: hint {:?} spans beyond end of file {:?}; file size was {}", e, read_path, data.len());
            return None;
        }
        new_data.extend(&data[ptr..e.l.unwrap() as usize]);
        if let Some(v) = e.v {
            new_data.extend(v.as_bytes());
        } else if let Some(vf) = e.vf {
            let inlined_data = fs::read(&src_file_paths[vf as usize]).unwrap();
            new_data.extend(inlined_data);
        }
        ptr = e.r.unwrap() as usize;
    }
    new_data.extend(&data[ptr..]);

    let mut write_path_override = None;
    for e in global_edits {
        if e.t == Some("mv".to_string()) {
            write_path_override = Some(dest_path.join(e.v.unwrap()));
        } else {
            panic!("unexpected hint loc type {:?}", e.t);
        }
    }
    let write_path = if let Some(overridden) = &write_path_override {
        eprintln!("writing {} bytes (old size {}) to moved file {:?}", new_data.len(), data.len(), overridden);
        overridden
    } else {
        eprintln!("writing {} bytes (old size {}) to file {:?}", new_data.len(), data.len(), dest_file_paths[file_id]);
        &dest_file_paths[file_id]
    };

    if command == &Command::Transform {
        fs::write(write_path, &new_data).unwrap();
    }
    let reduction = data.len() as i64 - new_data.len() as i64;
    Some(reduction)
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let command = match args[1].as_str() {
        "transform" => Command::Transform,
        "dry-run" => Command::DryRun,
        _ => panic!(),
    };
    let src_path = Path::new(&args[2]);
    let dest_path = if command == Command::Transform { Path::new(&args[3]) } else { Path::new("") };
    let mut file_to_hint_ids = HashMap::new();
    let hint_sources_arg = if command == Command::Transform { 4 } else { 3 };
    for i in (hint_sources_arg..args.len()).step_by(3) {
        let path = &args[i];
        let hint_from: u32 = args[i + 1].parse().unwrap();
        let hint_to: u32 = args[i + 2].parse().unwrap();
        let ids = file_to_hint_ids
            .entry(path)
            .or_insert_with(|| HashSet::new());
        ids.extend(hint_from..hint_to);
    }

    let mut files = vec![];
    let mut hints = vec![];
    for (path, hint_ids) in &file_to_hint_ids {
        let (fs, hs) = load_hints(path, hint_ids);
        files = fs;
        hints.extend(hs);
    }
    // eprintln!("files={:?}", files);
    // for h in hints.iter() {
    //     eprintln!("{:?}", h);
    // }

    // Determine files/dirs to be deleted.
    let files_for_deletion: HashSet<u32> = hints
        .iter()
        .filter_map(|h| {
            if h.t.starts_with("delfile::") {
                h.n
            } else {
                None
            }
        })
        .collect();
    let dirs_for_deletion: HashSet<PathBuf> = hints
        .iter()
        .filter_map(|h| {
            if h.t.starts_with("rmdir") {
                Some(PathBuf::from(h.ns.clone().unwrap()))
            } else {
                None
            }
        })
        .collect();
    // eprintln!("files_for_deletion={:?}", files_for_deletion);

    // Determine edits for each file.
    let mut file_to_edits = HashMap::new();
    for h in hints.iter() {
        for l in get_hint_locs(&h) {
            if !files_for_deletion.contains(&l.f) {
                file_to_edits.entry(l.f).or_insert_with(|| vec![]).push(l);
            }
        }
    }
    // eprintln!("file_to_edits={:?}", file_to_edits);

    // Replicate the directory structure.
    let mut reduction_file_count = 0;
    for entry in WalkDir::new(src_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_dir())
    {
        let dir_rel = entry.path().strip_prefix(src_path).unwrap();
        if dirs_for_deletion.contains(dir_rel) {
            eprintln!("skipping deleted dir {:?}", dir_rel);
            reduction_file_count += 1;
            continue;
        }
        let dir_dest = dest_path.join(dir_rel);
        eprintln!("creating dir {:?}", dir_dest);
        if command == Command::Transform {
            fs::create_dir_all(dir_dest).unwrap();
        }
    }

    let nest = fs::metadata(src_path).unwrap().is_dir();
    let src_file_paths: Vec<PathBuf> = files.iter().map(|f| if nest { src_path.join(&f) } else { src_path.to_path_buf() }).collect();
    let dest_file_paths: Vec<PathBuf> = files.iter().map(|f| if nest { dest_path.join(&f) } else { dest_path.to_path_buf() }).collect();

    let mut total_reduction = 0;
    for file_id in 0..files.len() {
        let path_from = &src_file_paths[file_id];
        if files_for_deletion.contains(&(file_id as u32)) {
            total_reduction += fs::metadata(path_from).unwrap().len() as i64;
            reduction_file_count += 1;
            eprintln!("skipping deleted file {:?}", path_from);
            continue;
        }
        match file_to_edits.remove(&(file_id as u32)) {
            None => {
                let canon_path_from = fs::canonicalize(path_from).unwrap();
                let path_to = &dest_file_paths[file_id];
                eprintln!("creating symlink at {:?} pointing to {:?}", path_to, canon_path_from);
                if command == Command::Transform {
                    symlink(canon_path_from, path_to).unwrap();
                }
            }
            Some(edits) => {
                if let Some(reduction) = write_edited_file(&command, file_id, edits, dest_path, &src_file_paths, &dest_file_paths) {
                    total_reduction += reduction;
                } else {
                    return ExitCode::from(1);
                }
            }
        }
    }
    println!("{} {}", total_reduction, reduction_file_count);
    ExitCode::from(0)
}
