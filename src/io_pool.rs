use std::env;
use std::fs::File;
use std::io;
use std::path::PathBuf;
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



fn find_data_start(map: &[u8], file_len: u64) -> Option<u64> {
    let mut offset = 0usize;
    while offset < map.len() {
        let step = 4096usize.min(map.len() - offset);
        if let Some(pos) = memchr::memchr(b'\n', &map[offset..offset + step]) {
            let start = offset as u64 + pos as u64 + 1;
            return (start < file_len).then_some(start);
        }
        offset += step;
    }
    None
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
    map: Arc<memmap2::Mmap>,
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
    LineFn: Fn(&mut S, &[u8], bool) + Sync,
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
        let file = File::open(p)?;
        let map = Arc::new(unsafe { memmap2::Mmap::map(&file)? });
        let Some(data_start) = find_data_start(&map, file_len) else {
            continue;
        };
        let mut c_start = data_start;
        while c_start < file_len {
            let c_end = (c_start + block_size).min(file_len);
            tasks.push(ChunkTask {
                map: Arc::clone(&map),
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

                loop {
                    let idx = next_ref.fetch_add(1, Ordering::Relaxed);
                    let Some(task) = tasks_ref.get(idx) else {
                        break;
                    };
                    if let Err(e) = execute_chunk(task, &mut state, line_ref) {
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
    state: &mut S,
    process_line: &LineFn,
) -> io::Result<()>
where
    LineFn: Fn(&mut S, &[u8], bool),
{
    let map = &task.map[..];
    let read_from = if task.c_start == task.data_start {
        task.c_start
    } else {
        task.c_start - 1
    };
    let mut read_to = (task.c_end + 4096).min(task.file_len);
    let limit_in_buf = (task.c_end - read_from) as usize;
    while read_to < task.file_len
        && memchr::memchr(b'\n', &map[(read_from as usize + limit_in_buf)..read_to as usize])
            .is_none()
    {
        read_to = (read_to + 65_536).min(task.file_len);
    }
    let buf = &map[read_from as usize..read_to as usize];

    let chunk_ascii = buf.is_ascii();

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
                process_line(state, &buf[pos..pos + rel], chunk_ascii);
                pos += rel + 1;
            }
            None => {
                process_line(state, &buf[pos..], chunk_ascii);
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
                |st, line, _ascii| st.push(line.to_vec()),
            )
            .unwrap();
            let mut got: Vec<Vec<u8>> = states.into_iter().flatten().collect();
            got.sort();
            assert_eq!(got, expected, "mismatch at chunk_sz={chunk_sz}");
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
