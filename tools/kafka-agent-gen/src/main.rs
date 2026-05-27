use std::fs;
use std::io::{self, BufRead, Write};

const DEFAULT_TAG: &str = "ww";
const DEFAULT_OUT: &str = "kafka.agent.yaml";

fn random_appname() -> String {
    let id = uuid::Uuid::new_v4();
    format!("app_{}", &id.to_string()[..8])
}

fn prompt(p: &str) -> String {
    let mut stdout = io::stdout().lock();
    write!(stdout, "{p}").ok();
    stdout.flush().ok();
    let stdin = io::stdin().lock();
    stdin.lines().next().map_or_else(String::new, |l| l.unwrap_or_default())
}

fn prompt_default(p: &str, def: &str) -> String {
    let mut stdout = io::stdout().lock();
    write!(stdout, "{p} [{def}]: ").ok();
    stdout.flush().ok();
    let stdin = io::stdin().lock();
    let v = stdin.lines().next().map_or_else(String::new, |l| l.unwrap_or_default());
    if v.trim().is_empty() { def.to_string() } else { v }
}

fn render(source_id: &str, appname: &str, tag: &str) -> String {
    format!(
        "sink:\n  type: kafka\n  kafka:\n    mode: agent\n    agent:\n      source_id: {source_id}\n      appname: {appname}\n      tag: {tag}\n"
    )
}

fn main() {
    let source_id = prompt("source_id: ");
    let source_id = source_id.trim();
    if source_id.is_empty() {
        eprintln!("kafka-agent-gen: source_id is required");
        std::process::exit(1);
    }

    let def_name = random_appname();
    let appname = prompt_default("appname", &def_name);

    let tag = prompt_default("tag", DEFAULT_TAG);

    let outfile = prompt_default("filename", DEFAULT_OUT);

    let yaml = render(source_id, appname.trim(), tag.trim());

    fs::write(&outfile, yaml).unwrap_or_else(|e| {
        eprintln!("kafka-agent-gen: write {}: {e}", outfile);
        std::process::exit(1);
    });
    eprintln!("kafka-agent-gen: written to {outfile}");
}
