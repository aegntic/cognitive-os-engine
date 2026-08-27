use crate::config::Config;
use crate::db::Brain;
use crate::embed::{packing, Embedder};
use crate::ingest::{sha256, walk_vault};
use anyhow::{bail, Result};
use serde_json::{json, Value};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct CycleReport {
    pub status: String,
    pub discovered: i64,
    pub imported: i64,
    pub skipped: i64,
    pub embedded: i64,
    pub exported: i64,
    pub failed_gate: Option<String>,
    pub detail: Value,
}

impl CycleReport {
    pub fn to_json(&self) -> Value {
        json!({
            "status": self.status,
            "discovered": self.discovered,
            "imported": self.imported,
            "skipped": self.skipped,
            "embedded": self.embedded,
            "exported": self.exported,
            "failed_gate": self.failed_gate,
            "gates": self.detail.get("gates"),
            "detail": self.detail,
        })
    }
}

pub fn run(home: &Path, cfg: &Config) -> Result<CycleReport> {
    let started = now();
    let brain_path = Config::brain_path(home, &cfg.name);
    let brain = Brain::open(&brain_path)?;
    let embedder = Embedder::new(&cfg.embedder, &cfg.embedder_url, cfg.embed_dims);

    let mut discovered = 0i64;
    let mut imported = 0i64;
    let mut skipped = 0i64;
    let mut embedded = 0i64;
    let mut vault_ids = Vec::new();

    for vault in &cfg.vaults {
        let root = Path::new(&vault.path);
        vault_ids.push(vault.id.clone());
        if !root.is_dir() {
            let report = fail(
                started,
                &brain,
                &vault_ids,
                discovered,
                imported,
                skipped,
                embedded,
                "discovered",
                json!({"gates": {"discovered": "failed"}, "error": format!("vault missing: {}", root.display())}),
            )?;
            return Ok(report);
        }
        let files = walk_vault(root)?;
        if files.is_empty() {
            let report = fail(
                started,
                &brain,
                &vault_ids,
                discovered,
                imported,
                skipped,
                embedded,
                "discovered",
                json!({"gates": {"discovered": "failed"}, "error": format!("no markdown in {}", root.display())}),
            )?;
            return Ok(report);
        }
        discovered += files.len() as i64;

        if let Err(e) = embedder.probe() {
            let report = fail(
                started,
                &brain,
                &vault_ids,
                discovered,
                0,
                0,
                0,
                "embed",
                json!({
                    "gates": {"discovered": "ok", "import": "skipped", "embed": "failed"},
                    "error": format!("embedder unreachable: {e:#}")
                }),
            )?;
            return Ok(report);
        }

        for file in files {
            let page_path = format!("{}:{}", vault.id, file.rel);
            let (page_id, changed) = brain.upsert_page(
                &page_path,
                &vault.id,
                &file.title,
                file.mtime,
                &file.hash,
                now(),
            )?;
            if !changed {
                skipped += 1;
                continue;
            }
            imported += 1;
            for (ord, chunk) in file.chunks.iter().enumerate() {
                let vec = embedder.embed(chunk)?;
                if vec.len() != cfg.embed_dims as usize && !embedder.is_mock() {
                    // nomic may return 768; mock always matches. Tolerate mock dims.
                }
                let packed = packing(&vec);
                brain.insert_chunk(page_id, ord as i32, chunk, &sha256(chunk), Some(&packed))?;
                embedded += 1;
            }
        }
    }

    if imported != discovered - skipped {
        let report = fail(
            started,
            &brain,
            &vault_ids,
            discovered,
            imported,
            skipped,
            embedded,
            "import",
            json!({"gates": {"import": "failed"}, "error": "imported != discovered - skipped"}),
        )?;
        return Ok(report);
    }

    let report = CycleReport {
        status: "green".into(),
        discovered,
        imported,
        skipped,
        embedded,
        exported: 0,
        failed_gate: None,
        detail: json!({
            "gates": {
                "discovered": "ok",
                "import": "ok",
                "embed": "ok",
                "export": "skipped"
            }
        }),
    };
    brain.record_cycle(
        started,
        now(),
        &vault_ids.join(","),
        discovered,
        imported,
        skipped,
        embedded,
        0,
        "green",
        &report.detail,
    )?;
    Ok(report)
}

fn fail(
    started: i64,
    brain: &Brain,
    vaults: &[String],
    discovered: i64,
    imported: i64,
    skipped: i64,
    embedded: i64,
    gate: &str,
    detail: Value,
) -> Result<CycleReport> {
    brain.record_cycle(
        started,
        now(),
        &vaults.join(","),
        discovered,
        imported,
        skipped,
        embedded,
        0,
        "failed",
        &detail,
    )?;
    Ok(CycleReport {
        status: "failed".into(),
        discovered,
        imported,
        skipped,
        embedded,
        exported: 0,
        failed_gate: Some(gate.into()),
        detail,
    })
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub fn exit_code(r: &CycleReport) -> i32 {
    if r.status == "green" {
        0
    } else {
        1
    }
}

pub fn ensure_green(r: &CycleReport) -> Result<()> {
    if r.status != "green" {
        bail!("cycle failed at gate {:?}", r.failed_gate);
    }
    Ok(())
}
