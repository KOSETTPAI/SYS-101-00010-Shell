use std::ffi::{CStr, CString};
use std::io::{self, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::PathBuf;
use std::{env, fs::OpenOptions};

use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{dup2, execvp, fork, pipe, ForkResult, Pid};

#[derive(Debug, Clone)]
struct Stage {
    argv: Vec<String>,
    input: Option<PathBuf>,
    output: Option<PathBuf>,
}

fn print_prompt() {
    match env::current_dir() {
        Ok(p) => {
            print!("{}$ ", p.display());
            let _ = io::stdout().flush();
        }
        Err(e) => {
            print!("[cwd-error:{}]$ ", e);
            let _ = io::stdout().flush();
        }
    }
}

fn read_line() -> io::Result<Option<String>> {
    let mut line = String::new();
    let n = io::stdin().read_line(&mut line)?;
    if n == 0 {
        return Ok(None);
    }
    // Trim trailing newline and carriage return
    while line.ends_with(['\n', '\r']) {
        line.pop();
    }
    Ok(Some(line))
}

fn externalize_from_vec(args: &[String]) -> Result<Vec<CString>, String> {
    let mut v = Vec::with_capacity(args.len());
    for s in args {
        match CString::new(s.as_str()) {
            Ok(cs) => v.push(cs),
            Err(_) => return Err("argument contains NUL byte".to_string()),
        }
    }
    Ok(v)
}

fn parse_command_line(line: &str) -> Result<(Vec<Stage>, bool), String> {
    let mut s = line.trim().to_string();
    let mut background = false;
    if s.ends_with('&') {
        background = true;
        s.pop();
        s = s.trim_end().to_string();
    }

    if s.is_empty() {
        return Err("empty".to_string());
    }

    let mut stages: Vec<Stage> = Vec::new();
    for raw_seg in s.split('|') {
        let seg = raw_seg.trim();
        if seg.is_empty() {
            return Err("syntax error near '|'".to_string());
        }
        let mut argv: Vec<String> = Vec::new();
        let mut input: Option<PathBuf> = None;
        let mut output: Option<PathBuf> = None;
        let mut it = seg.split_whitespace().peekable();
        while let Some(tok) = it.next() {
            match tok {
                "<" => {
                    if input.is_some() {
                        return Err("duplicate input redirection".into());
                    }
                    let Some(fname) = it.next() else { return Err("missing filename after '<'".into()) };
                    input = Some(PathBuf::from(fname));
                }
                ">" => {
                    if output.is_some() {
                        return Err("duplicate output redirection".into());
                    }
                    let Some(fname) = it.next() else { return Err("missing filename after '>'".into()) };
                    output = Some(PathBuf::from(fname));
                }
                _ => argv.push(tok.to_string()),
            }
        }
        if argv.is_empty() {
            return Err("empty command in pipeline".into());
        }
        stages.push(Stage { argv, input, output });
    }

    // Enforce redirection only on ends of pipeline
    if stages.len() > 1 {
        for (i, st) in stages.iter().enumerate() {
            if i != 0 && st.input.is_some() {
                return Err("input redirection only allowed for first command".into());
            }
            if i != stages.len() - 1 && st.output.is_some() {
                return Err("output redirection only allowed for last command".into());
            }
        }
    }

    Ok((stages, background))
}

fn run_builtin(line: &str) -> Result<bool, String> {
    // returns Ok(true) if handled as builtin
    let mut parts = line.split_whitespace();
    let Some(cmd) = parts.next() else { return Ok(true) }; // blank

    match cmd {
        "exit" => std::process::exit(0),
        "cd" => {
            let target = match parts.next() {
                Some(p) => PathBuf::from(p),
                None => {
                    match env::var("HOME") {
                        Ok(h) => PathBuf::from(h),
                        Err(_) => {
                            return Err("cd: HOME not set".into());
                        }
                    }
                }
            };
            match env::set_current_dir(&target) {
                Ok(()) => Ok(true),
                Err(e) => Err(format!("cd: {}: {}", target.display(), e)),
            }
        }
        _ => Ok(false),
    }
}

fn spawn_pipeline(stages: Vec<Stage>, background: bool) -> Result<(), String> {
    // returns after waiting unless background
    let mut pids: Vec<Pid> = Vec::new();
    let mut prev_read: Option<OwnedFd> = None;

    for i in 0..stages.len() {
        let is_last = i == stages.len() - 1;
        let st = &stages[i];

        // Create pipe for this stage if not last
        let (next_read, this_write) = if !is_last {
            match pipe() {
                Ok((r, w)) => (Some(r), Some(w)),
                Err(e) => return Err(format!("pipe: {}", e)),
            }
        } else {
            (None, None)
        };

        // Open redirection files in parent so errors are reported before fork
        let mut in_file = None;
        if st.input.is_some() && i == 0 {
            let path = st.input.as_ref().unwrap();
            match OpenOptions::new().read(true).open(path) {
                Ok(f) => in_file = Some(f),
                Err(e) => return Err(format!("< {}: {}", path.display(), e)),
            }
        }
        let mut out_file = None;
        if st.output.is_some() && is_last {
            let path = st.output.as_ref().unwrap();
            match OpenOptions::new().create(true).truncate(true).write(true).open(path) {
                Ok(f) => out_file = Some(f),
                Err(e) => return Err(format!("> {}: {}", path.display(), e)),
            }
        }

        // Prepare argv
        let cargs = externalize_from_vec(&st.argv)?;
        if cargs.is_empty() { return Err("empty argv".into()); }
        let prog: &CStr = &cargs[0];

        // Fork
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // Child: set up fds
                // If there is previous read end, dup to stdin
                if let Some(ref rfd) = prev_read {
                    if let Err(e) = dup2(rfd.as_raw_fd(), 0) { eprintln!("dup2 prev->stdin: {}", e); std::process::exit(1); }
                }
                // If there is a write end for this stage, dup to stdout
                if let Some(ref wfd) = this_write {
                    if let Err(e) = dup2(wfd.as_raw_fd(), 1) { eprintln!("dup2 pipe->stdout: {}", e); std::process::exit(1); }
                }
                // Redirections
                if let Some(f) = in_file.as_ref() {
                    if let Err(e) = dup2(f.as_raw_fd(), 0) { eprintln!("dup2 <file: {}", e); std::process::exit(1); }
                }
                if let Some(f) = out_file.as_ref() {
                    if let Err(e) = dup2(f.as_raw_fd(), 1) { eprintln!("dup2 >file: {}", e); std::process::exit(1); }
                }
                // OwnedFd will be closed on drop here
                // Exec
                match execvp(prog, &cargs) {
                    Ok(_) => unreachable!(),
                    Err(e) => {
                        eprintln!("execvp {}: {}", st.argv[0], e);
                        std::process::exit(127);
                    }
                }
            }
            Ok(ForkResult::Parent { child }) => {
                // Parent: close write end and previous read end (drop OwnedFd)
                if this_write.is_some() { drop(this_write); }
                if let Some(fd) = prev_read.take() { drop(fd); }
                prev_read = next_read;
                pids.push(child);
            }
            Err(e) => return Err(format!("fork: {}", e)),
        }
    }

    // Close last prev_read in parent
    if let Some(fd) = prev_read { drop(fd); }

    if background {
        if let Some(&last) = pids.last() {
            println!("{}", last);
        }
        return Ok(());
    }

    // Wait for all children in order
    for pid in pids {
        match waitpid(pid, None) {
            Ok(WaitStatus::Exited(_, _)) | Ok(WaitStatus::Signaled(_, _, _)) | Ok(WaitStatus::Stopped(_, _)) | Ok(WaitStatus::Continued(_)) => {}
            Ok(_) => {}
            Err(e) => eprintln!("waitpid {}: {}", pid, e),
        }
    }
    Ok(())
}

fn main() {
    loop {
        print_prompt();
        let line_opt = match read_line() { Ok(l) => l, Err(e) => { eprintln!("read error: {}", e); break; } };
        let Some(line) = line_opt else { break };
        // ignore blank
        if line.trim().is_empty() { continue; }

        // Builtins: handle raw line to allow e.g. "cd .."
        match run_builtin(&line) {
            Ok(true) => continue,
            Ok(false) => {},
            Err(e) => { eprintln!("{}", e); continue; }
        }

        // Parse and execute external commands (including pipelines/redirs)
        match parse_command_line(&line) {
            Ok((stages, background)) => {
                if let Err(e) = spawn_pipeline(stages, background) {
                    eprintln!("{}", e);
                }
            }
            Err(e) if e == "empty" => continue,
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }
}
