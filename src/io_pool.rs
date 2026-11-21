use std::env;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

pub fn worker_count() -> usize {
    if let Ok(val) = env::var("NCPU") {
        if let Ok(n) = val.trim().parse::<usize>() {
            if n > 0 {
                return n.min(64);
            }
        }
    }
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .clamp(1, 64)
}

#[cfg(unix)]
fn read_range_into(file: &File, _path: &Path, mut offset: u64, mut len: usize, out: &mut Vec<u8>) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    let base = out.len();
    out.resize(base + len, 0);
    let mut dst = &mut out[base..];
    while !dst.is_empty() {
        let n = file.read_at(dst, offset)?;
        if n == 0 {
            let read_so_far = len - dst.len();
            out.truncate(base + read_so_far);
            break;
        }
        offset += n as u64;
        len -= n;
        let _ = len;
        dst = &mut dst[n..];
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_range_into(_file: &File, path: &Path, offset: u64, len: usize, out: &mut Vec<u8>) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(offset))?;
    let base = out.len();
    out.resize(base + len, 0);
    let mut filled = 0usize;
    while filled < len {
        let n = f.read(&mut out[base + filled..base + len])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    out.truncate(base + filled);
    Ok(())
}

fn find_data_start(file: &File, path: &Path, file_len: u64) -> io::Result<Option<u64>> {
    let mut probe = Vec::with_capacity(4096);
    let mut offset = 0u64;
    while offset < file_len {
        probe.clear();
        let step = 4096usize.min((file_len - offset) as usize);
        read_range_into(file, path, offset, step, &mut probe)?;
        if probe.is_empty() {
            return Ok(None);
        }
        if let Some(pos) = memchr::memchr(b'\n', &probe) {
            let start = offset + pos as u64 + 1;
            return Ok((start < file_len).then_some(start));
        }
        offset += probe.len() as u64;
    }
    Ok(None)
}

pub fn parallel_for<F>(count: usize, threads: usize, f: F)
where
    F: Fn(usize) + Sync + Send,
{
    if count == 0 {
        return;
    }
    let next = AtomicUsize::new(0);
    let next_ref = &next;
    let f = &f;
    thread::scope(|scope| {
        for _ in 0..threads.min(count) {
            scope.spawn(move || loop {
                let i = next_ref.fetch_add(1, Ordering::Relaxed);
                if i >= count {
                    break;
                }
                f(i);
            });
        }
    });
}

struct ChunkTask {
    file: Arc<File>,
    path: Arc<PathBuf>,
    file_len: u64,
    data_start: u64,
    c_start: u64,
    c_end: u64,
}

pub fn process_csv_files_parallel<S, InitFn, LineFn>(
    paths: &[PathBuf],
    block_size: u64,
    threads: usize,
    init_worker: InitFn,
    process_line: LineFn,
) -> io::Result<Vec<S>>
where
    S: Send,
    InitFn: Fn(usize) -> S + Sync,
    LineFn: Fn(&mut S, &[u8]) + Sync,
{
    let mut sized_paths: Vec<(u64, &PathBuf)> = Vec::with_capacity(paths.len());
    for p in paths {
        let len = p.metadata()?.len();
        if len > 0 {
            sized_paths.push((len, p));
        }
    }
    sized_paths.sort_unstable_by(|a, b| b.0.cmp(&a.0));

    let mut tasks: Vec<ChunkTask> = Vec::with_capacity(sized_paths.len() * 2);
    for (file_len, p) in sized_paths {
        let file = Arc::new(File::open(p)?);
        let Some(data_start) = find_data_start(&file, p, file_len)? else {
            continue;
        };
        let path_arc = Arc::new(p.clone());
        let mut c_start = data_start;
        while c_start < file_len {
            let c_end = (c_start + block_size).min(file_len);
            tasks.push(ChunkTask {
                file: Arc::clone(&file),
                path: Arc::clone(&path_arc),
                file_len,
                data_start,
                c_start,
                c_end,
            });
            c_start = c_end;
        }
    }

    let actual_threads = threads.min(tasks.len().max(1));
    if tasks.is_empty() {
        return Ok((0..actual_threads).map(&init_worker).collect());
    }

    let next_task = AtomicUsize::new(0);
    let err_slot: Mutex<Option<io::Error>> = Mutex::new(None);
    let results: Vec<Mutex<Option<S>>> = (0..actual_threads).map(|_| Mutex::new(None)).collect();

    thread::scope(|scope| {
        for worker_id in 0..actual_threads {
            let next_ref = &next_task;
            let tasks_ref = &tasks;
            let err_ref = &err_slot;
            let res_slot = &results[worker_id];
            let init_ref = &init_worker;
            let line_ref = &process_line;

            scope.spawn(move || {
                let mut state = init_ref(worker_id);
                let mut buf = Vec::with_capacity(block_size as usize + 8192);

                loop {
                    let idx = next_ref.fetch_add(1, Ordering::Relaxed);
                    let Some(task) = tasks_ref.get(idx) else {
                        break;
                    };
                    if let Err(e) = execute_chunk(task, &mut buf, &mut state, line_ref) {
                        let mut guard = err_ref.lock().unwrap();
                        if guard.is_none() {
                            *guard = Some(e);
                        }
                        break;
                    }
                }
                *res_slot.lock().unwrap() = Some(state);
            });
        }
    });

    if let Some(e) = err_slot.into_inner().unwrap() {
        return Err(e);
    }

    Ok(results
        .into_iter()
        .filter_map(|m| m.into_inner().unwrap())
        .collect())
}

fn execute_chunk<S, LineFn>(
    task: &ChunkTask,
    buf: &mut Vec<u8>,
    state: &mut S,
    process_line: &LineFn,
) -> io::Result<()>
where
    LineFn: Fn(&mut S, &[u8]),
{
    let read_from = if task.c_start == task.data_start {
        task.c_start
    } else {
        task.c_start - 1
    };
    let mut read_to = (task.c_end + 4096).min(task.file_len);
    buf.clear();
    read_range_into(
        &task.file,
        &task.path,
        read_from,
        (read_to - read_from) as usize,
        buf,
    )?;

    let limit_in_buf = (task.c_end - read_from) as usize;
    while read_to < task.file_len && memchr::memchr(b'\n', &buf[limit_in_buf..]).is_none() {
        let next_to = (read_to + 65_536).min(task.file_len);
        read_range_into(
            &task.file,
            &task.path,
            read_to,
            (next_to - read_to) as usize,
            buf,
        )?;
        read_to = next_to;
    }

    let mut pos = if task.c_start == task.data_start {
        0usize
    } else if buf.first() == Some(&b'\n') {
        1usize
    } else {
        match memchr::memchr(b'\n', &buf[1..]) {
            Some(rel) => 1 + rel + 1,
            None => return Ok(()),
        }
    };

    while pos < limit_in_buf && pos < buf.len() {
        match memchr::memchr(b'\n', &buf[pos..]) {
            Some(rel) => {
                process_line(state, &buf[pos..pos + rel]);
                pos += rel + 1;
            }
            None => {
                process_line(state, &buf[pos..]);
                break;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv_dialect::iter_data_lines;
    use std::fs;

    #[test]
    fn parallel_tiny_chunks_match_sequential_on_every_alignment() {
        let dir = std::env::temp_dir().join(format!("helex_io_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let p1 = dir.join("a.csv");
        let p2 = dir.join("b_header_only.csv");
        let p3 = dir.join("c_no_trailing_nl.csv");

        let mut content1 = b"col1,col2\n".to_vec();
        for i in 0..500 {
            if i % 37 == 0 {
                content1.extend_from_slice(b"\n");
            }
            content1.extend_from_slice(format!("row_{i},value_number_{i}\n").as_bytes());
        }
        fs::write(&p1, &content1).unwrap();
        fs::write(&p2, b"col1,col2\n").unwrap();
        fs::write(&p3, b"col1,col2\nalpha,1\nbeta,2").unwrap();

        let paths = vec![p1, p2, p3];
        let mut expected: Vec<Vec<u8>> = Vec::new();
        for p in &paths {
            let b = fs::read(p).unwrap();
            iter_data_lines(&b, |l| expected.push(l.to_vec()));
        }
        expected.sort();

        for chunk_sz in [7u64, 13, 17, 31, 64, 128, 4096] {
            let states = process_csv_files_parallel(
                &paths,
                chunk_sz,
                4,
                |_| Vec::<Vec<u8>>::new(),
                |st, line| st.push(line.to_vec()),
            )
            .unwrap();
            let mut got: Vec<Vec<u8>> = states.into_iter().flatten().collect();
            got.sort();
            assert_eq!(got, expected, "mismatch at chunk_sz={chunk_sz}");
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
