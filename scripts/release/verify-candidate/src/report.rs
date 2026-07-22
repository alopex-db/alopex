use crate::gate::{ReadinessReport, ReadinessVerdict};
use crate::{io_error, Result};
use std::fs::File;
use std::io::Write;
use std::path::Path;

pub fn write_reports(output_dir: &Path, report: &ReadinessReport) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .map_err(|error| io_error("create report directory", error))?;
    let json_path = output_dir.join("readiness-report.json");
    let json = serde_json::to_vec_pretty(report)
        .map_err(|error| crate::json_error("serialize readiness report", error))?;
    std::fs::write(&json_path, json).map_err(|error| io_error("write JSON report", error))?;

    let mut markdown = File::create(output_dir.join("support-matrix.md"))
        .map_err(|error| io_error("create Markdown report", error))?;
    let verdict = match report.verdict {
        ReadinessVerdict::Ready => "Ready",
        ReadinessVerdict::Blocked => "Blocked",
    };
    writeln!(markdown, "# Alopex v0.8 candidate support matrix\n")
        .map_err(|error| io_error("write Markdown report", error))?;
    writeln!(
        markdown,
        "Verdict: **{verdict}**. This report performs no publication action.\n"
    )
    .map_err(|error| io_error("write Markdown report", error))?;
    writeln!(markdown, "| ID | Phase | Surface | Support | Prerequisite | Artifact | Normal outcome | Failure outcome |")
        .map_err(|error| io_error("write Markdown report", error))?;
    writeln!(
        markdown,
        "| --- | --- | --- | --- | --- | --- | --- | --- |"
    )
    .map_err(|error| io_error("write Markdown report", error))?;
    for row in &report.rows {
        writeln!(
            markdown,
            "| {} | {} | {} | {:?} | {} | {} | {} | {} |",
            row.id,
            row.phase,
            escape_cell(&row.public_surface),
            row.support,
            escape_cell(row.prerequisite.as_deref().unwrap_or("")),
            escape_cell(&row.artifacts.join(", ")),
            escape_cell(&row.normal_outcome),
            escape_cell(&row.failure_outcome),
        )
        .map_err(|error| io_error("write Markdown report", error))?;
    }
    writeln!(markdown, "\n## Blockers\n")
        .map_err(|error| io_error("write Markdown report", error))?;
    if report.blockers.is_empty() {
        writeln!(markdown, "None.").map_err(|error| io_error("write Markdown report", error))?;
    } else {
        for blocker in &report.blockers {
            writeln!(markdown, "- `{}`: {}", blocker.code, blocker.detail)
                .map_err(|error| io_error("write Markdown report", error))?;
        }
    }
    Ok(())
}

fn escape_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}
