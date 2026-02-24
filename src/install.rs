use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Context};

const SKILL_CONTENT: &str = include_str!("../skills/otel-cli/SKILL.md");

pub fn run(global: bool, force: bool) -> anyhow::Result<()> {
    let dest = if global {
        let home = dirs::home_dir().context("Could not determine home directory")?;
        home.join(".claude")
            .join("skills")
            .join("otel-cli")
            .join("SKILL.md")
    } else {
        PathBuf::from("./.claude/skills/otel-cli/SKILL.md")
    };

    if dest.exists() && !force {
        bail!(
            "File already exists: {}\nUse --force to overwrite.",
            dest.display()
        );
    }

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    fs::write(&dest, SKILL_CONTENT)
        .with_context(|| format!("Failed to write file: {}", dest.display()))?;

    let display_path = if global {
        dest.display().to_string()
    } else {
        format!("./.claude/skills/otel-cli/SKILL.md")
    };

    let line1 = "otel-cli skill installed successfully!";
    let line_installed = format!("Installed to: {display_path}");
    let line_available = "The skill is now available for AI coding agents.";
    let line_try = r#"Try asking: "Use otel-cli to inspect my traces""#;

    let content_width = [
        line1.len(),
        line_installed.len(),
        line_available.len(),
        line_try.len(),
    ]
    .into_iter()
    .max()
    .unwrap();
    let box_width = content_width + 4; // 2 spaces padding each side

    let border = "═".repeat(box_width);
    let empty_line = format!("║  {:width$}  ║", "", width = content_width);

    println!("╔{border}╗");
    println!("║  {:width$}  ║", line1, width = content_width);
    println!("{empty_line}");
    println!("║  {:width$}  ║", line_installed, width = content_width);
    println!("{empty_line}");
    println!("║  {:width$}  ║", line_available, width = content_width);
    println!("║  {:width$}  ║", line_try, width = content_width);
    println!("╚{border}╝");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn install_creates_file_in_target_directory() {
        let tmp = TempDir::new().unwrap();
        let dest = tmp.path().join(".claude/skills/otel-cli/SKILL.md");

        // Simulate the install logic with a custom path
        let parent = dest.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        fs::write(&dest, SKILL_CONTENT).unwrap();

        assert!(dest.exists());
        let content = fs::read_to_string(&dest).unwrap();
        assert_eq!(content, SKILL_CONTENT);
    }

    #[test]
    fn skill_content_is_embedded() {
        assert!(!SKILL_CONTENT.is_empty());
        assert!(SKILL_CONTENT.contains("otel-cli"));
    }
}
