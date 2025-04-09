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
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize)]
struct HintLoc {
    f: u32,
    l: u32,
    r: u32,
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
    lines.next();
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
            l: hint.l.unwrap(),
            r: hint.r.unwrap(),
            v: hint.v.clone(),
            vf: hint.vf,
        }],
    }
}

fn merge_edits(mut edits: Vec<HintLoc>) -> Vec<HintLoc> {
    let mut merged: Vec<HintLoc> = vec![];
    edits.sort_by_key(|e| (e.l, e.v.is_some()));
    for e in edits.into_iter() {
        if merged.len() > 0 && merged.last().unwrap().r > e.l {
            merged.last_mut().unwrap().r = max(merged.last().unwrap().r, e.r);
        } else {
            merged.push(e);
        }
    }
    merged
}

fn write_edited_file(file_dest: &Path, file_src: &Path, edits: Vec<HintLoc>, src_paths: &Vec<PathBuf>) -> i64 {
    let data = fs::read(file_src).unwrap();
    let mut new_data = vec![];
    let mut ptr = 0;
    for e in merge_edits(edits) {
        new_data.extend(&data[ptr..e.l as usize]);
        if let Some(v) = e.v {
            new_data.extend(v.as_bytes());
        } else if let Some(vf) = e.vf {
            let inlined_data = fs::read(&src_paths[vf as usize]).unwrap();
            new_data.extend(inlined_data);
        }
        ptr = e.r as usize;
    }
    new_data.extend(&data[ptr..]);
    let reduction = data.len() as i64 - new_data.len() as i64;
    fs::write(file_dest, new_data).unwrap();
    reduction
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let dest_path = Path::new(&args[1]);
    let src_path = Path::new(&args[2]);
    let mut file_to_hint_ids = HashMap::new();
    for i in (3..args.len()).step_by(3) {
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
    for entry in WalkDir::new(src_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_dir())
    {
        let dir_rel = entry.path().strip_prefix(src_path).unwrap();
        if dirs_for_deletion.contains(dir_rel) {
            continue;
        }
        let dir_dest = dest_path.join(dir_rel);
        fs::create_dir_all(dir_dest).unwrap();
    }

    let nest = fs::metadata(src_path).unwrap().is_dir();
    let src_paths: Vec<PathBuf> = files.iter().map(|f| if nest { src_path.join(&f) } else { src_path.to_path_buf() }).collect();
    let dest_paths: Vec<PathBuf> = files.iter().map(|f| if nest { dest_path.join(&f) } else { dest_path.to_path_buf() }).collect();

    let mut reduction = 0;
    for file_id in 0..files.len() {
        let file_src = &src_paths[file_id];
        let file_dest = &dest_paths[file_id];
        if files_for_deletion.contains(&(file_id as u32)) {
            reduction += fs::metadata(file_src).unwrap().len() as i64;
            continue;
        }
        // eprintln!("file={:?} file_src={:?} file_dest={:?}", file, file_src, file_dest);
        match file_to_edits.remove(&(file_id as u32)) {
            None => {
                symlink(fs::canonicalize(file_src).unwrap(), file_dest).unwrap();
            }
            Some(edits) => {
                reduction += write_edited_file(&file_dest, &file_src, edits, &src_paths);
            }
        }
    }
    println!("{}", reduction);
}
