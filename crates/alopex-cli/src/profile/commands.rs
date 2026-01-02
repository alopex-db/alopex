use std::io::{self, Write};

use serde::Serialize;

use crate::cli::{OutputFormat, ProfileCommand};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::{create_formatter, Formatter};

use super::config::{Profile, ProfileManager};

#[derive(Debug, Serialize)]
pub struct ProfileListOutput {
    pub profiles: Vec<ProfileListItem>,
}

#[derive(Debug, Serialize)]
pub struct ProfileListItem {
    pub name: String,
    pub data_dir: String,
    pub is_default: bool,
}

#[derive(Debug, Serialize)]
pub struct ProfileShowOutput {
    pub name: String,
    pub data_dir: String,
    pub is_default: bool,
}

pub fn execute_profile_command(cmd: ProfileCommand, output: OutputFormat) -> Result<()> {
    match cmd {
        ProfileCommand::Create { name, data_dir } => {
            let mut manager = ProfileManager::load()?;
            manager.create(&name, Profile { data_dir })?;
            manager.save()
        }
        ProfileCommand::List => {
            let manager = ProfileManager::load()?;
            let items = build_list_items(&manager);
            output_profile_list(&items, output)
        }
        ProfileCommand::Show { name } => {
            let manager = ProfileManager::load()?;
            let show = build_show_output(&manager, &name)?;
            output_profile_show(&show, output)
        }
        ProfileCommand::Delete { name } => {
            let mut manager = ProfileManager::load()?;
            manager.delete(&name)?;
            manager.save()
        }
        ProfileCommand::SetDefault { name } => {
            let mut manager = ProfileManager::load()?;
            manager.set_default(&name)?;
            manager.save()
        }
    }
}

fn build_list_items(manager: &ProfileManager) -> Vec<ProfileListItem> {
    let default_name = manager.default_profile();
    manager
        .list()
        .into_iter()
        .filter_map(|name| {
            manager.get(name).map(|profile| ProfileListItem {
                name: name.to_string(),
                data_dir: profile.data_dir.clone(),
                is_default: default_name == Some(name),
            })
        })
        .collect()
}

fn build_show_output(manager: &ProfileManager, name: &str) -> Result<ProfileShowOutput> {
    let profile = manager
        .get(name)
        .ok_or_else(|| CliError::ProfileNotFound(name.to_string()))?;
    Ok(ProfileShowOutput {
        name: name.to_string(),
        data_dir: profile.data_dir.clone(),
        is_default: manager.default_profile() == Some(name),
    })
}

fn output_profile_list(items: &[ProfileListItem], output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Table => write_list_table(items),
        OutputFormat::Json => write_list_json(items),
        _ => Err(CliError::InvalidArgument(format!(
            "Unsupported output format for profile list: {:?}",
            output
        ))),
    }
}

fn output_profile_show(show: &ProfileShowOutput, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Table => write_show_table(show),
        OutputFormat::Json => write_show_json(show),
        _ => Err(CliError::InvalidArgument(format!(
            "Unsupported output format for profile show: {:?}",
            output
        ))),
    }
}

fn write_list_table(items: &[ProfileListItem]) -> Result<()> {
    let mut writer = io::stdout().lock();
    write_list_table_to(&mut writer, items)
}

fn write_list_table_to(writer: &mut dyn Write, items: &[ProfileListItem]) -> Result<()> {
    let columns = vec![
        Column::new("Name", DataType::Text),
        Column::new("Data Dir", DataType::Text),
        Column::new("Default", DataType::Text),
    ];
    let rows: Vec<Row> = items
        .iter()
        .map(|item| {
            Row::new(vec![
                Value::Text(item.name.clone()),
                Value::Text(item.data_dir.clone()),
                Value::Text(if item.is_default { "*" } else { "" }.to_string()),
            ])
        })
        .collect();
    write_rows_with_formatter_to(writer, OutputFormat::Table, &columns, &rows)
}

fn write_list_json(items: &[ProfileListItem]) -> Result<()> {
    let mut writer = io::stdout().lock();
    write_list_json_to(&mut writer, items)
}

fn write_list_json_to(writer: &mut dyn Write, items: &[ProfileListItem]) -> Result<()> {
    let columns = vec![
        Column::new("name", DataType::Text),
        Column::new("data_dir", DataType::Text),
        Column::new("is_default", DataType::Bool),
    ];
    let rows: Vec<Row> = items
        .iter()
        .map(|item| {
            Row::new(vec![
                Value::Text(item.name.clone()),
                Value::Text(item.data_dir.clone()),
                Value::Bool(item.is_default),
            ])
        })
        .collect();
    let array = rows_to_json_array(&columns, &rows)?;
    let value = serde_json::json!({ "profiles": array });
    write_json_value_to(writer, &value)
}

fn write_show_table(show: &ProfileShowOutput) -> Result<()> {
    let mut writer = io::stdout().lock();
    write_show_table_to(&mut writer, show)
}

fn write_show_table_to(writer: &mut dyn Write, show: &ProfileShowOutput) -> Result<()> {
    let columns = vec![
        Column::new("Name", DataType::Text),
        Column::new("Data Dir", DataType::Text),
        Column::new("Default", DataType::Text),
    ];
    let rows = vec![Row::new(vec![
        Value::Text(show.name.clone()),
        Value::Text(show.data_dir.clone()),
        Value::Text(if show.is_default { "Yes" } else { "No" }.to_string()),
    ])];
    let mut formatter = KeyValueFormatter::new();
    formatter.write_header(writer, &columns)?;
    for row in &rows {
        formatter.write_row(writer, row)?;
    }
    formatter.write_footer(writer)
}

fn write_show_json(show: &ProfileShowOutput) -> Result<()> {
    let mut writer = io::stdout().lock();
    write_show_json_to(&mut writer, show)
}

fn write_show_json_to(writer: &mut dyn Write, show: &ProfileShowOutput) -> Result<()> {
    let columns = vec![
        Column::new("name", DataType::Text),
        Column::new("data_dir", DataType::Text),
        Column::new("is_default", DataType::Bool),
    ];
    let rows = vec![Row::new(vec![
        Value::Text(show.name.clone()),
        Value::Text(show.data_dir.clone()),
        Value::Bool(show.is_default),
    ])];
    let array = rows_to_json_array(&columns, &rows)?;
    let obj = array
        .as_array()
        .and_then(|items| items.first())
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    write_json_value_to(writer, &obj)
}

fn write_rows_with_formatter_to(
    writer: &mut dyn Write,
    output: OutputFormat,
    columns: &[Column],
    rows: &[Row],
) -> Result<()> {
    let mut formatter = create_formatter(output);
    formatter.write_header(writer, columns)?;
    for row in rows {
        formatter.write_row(writer, row)?;
    }
    formatter.write_footer(writer)
}

fn rows_to_json_array(columns: &[Column], rows: &[Row]) -> Result<serde_json::Value> {
    let mut buffer = Vec::new();
    let mut formatter = create_formatter(OutputFormat::Json);
    formatter.write_header(&mut buffer, columns)?;
    for row in rows {
        formatter.write_row(&mut buffer, row)?;
    }
    formatter.write_footer(&mut buffer)?;
    let value: serde_json::Value = serde_json::from_slice(&buffer)?;
    Ok(value)
}

fn write_json_value_to(writer: &mut dyn Write, value: &serde_json::Value) -> Result<()> {
    serde_json::to_writer_pretty(&mut *writer, value)?;
    writeln!(writer)?;
    Ok(())
}

struct KeyValueFormatter {
    columns: Vec<String>,
    rows: Vec<Row>,
}

impl KeyValueFormatter {
    fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

impl Formatter for KeyValueFormatter {
    fn write_header(&mut self, _writer: &mut dyn Write, columns: &[Column]) -> Result<()> {
        self.columns = columns.iter().map(|column| column.name.clone()).collect();
        Ok(())
    }

    fn write_row(&mut self, _writer: &mut dyn Write, row: &Row) -> Result<()> {
        self.rows.push(row.clone());
        Ok(())
    }

    fn write_footer(&mut self, writer: &mut dyn Write) -> Result<()> {
        const LABEL_WIDTH: usize = 9;
        if let Some(row) = self.rows.first() {
            for (column, value) in self.columns.iter().zip(row.columns.iter()) {
                let label = format!("{}:", column);
                let value_str = format_value(value);
                writeln!(
                    writer,
                    "{:<width$} {}",
                    label,
                    value_str,
                    width = LABEL_WIDTH
                )?;
            }
        }
        Ok(())
    }

    fn supports_streaming(&self) -> bool {
        false
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => {
            if *b {
                "Yes".to_string()
            } else {
                "No".to_string()
            }
        }
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{:.6}", f),
        Value::Text(s) => s.clone(),
        Value::Bytes(bytes) => format!("{:?}", bytes),
        Value::Vector(values) => format!("{:?}", values),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_items() -> Vec<ProfileListItem> {
        vec![
            ProfileListItem {
                name: "dev".to_string(),
                data_dir: "/path/dev".to_string(),
                is_default: true,
            },
            ProfileListItem {
                name: "prod".to_string(),
                data_dir: "/path/prod".to_string(),
                is_default: false,
            },
        ]
    }

    fn sample_show() -> ProfileShowOutput {
        ProfileShowOutput {
            name: "dev".to_string(),
            data_dir: "/path/dev".to_string(),
            is_default: true,
        }
    }

    #[test]
    fn test_list_table_output() {
        let items = sample_items();
        let mut buffer = Vec::new();
        write_list_table_to(&mut buffer, &items).unwrap();
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("Name"));
        assert!(output.contains("Data Dir"));
        assert!(output.contains("Default"));
        assert!(output.contains("dev"));
        assert!(output.contains("/path/dev"));
        assert!(output.contains("*"));
    }

    #[test]
    fn test_list_json_output() {
        let items = sample_items();
        let mut buffer = Vec::new();
        write_list_json_to(&mut buffer, &items).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&buffer).unwrap();
        let profiles = value["profiles"].as_array().unwrap();
        assert_eq!(profiles.len(), 2);
        assert!(profiles.iter().any(|profile| {
            profile["name"] == "dev"
                && profile["data_dir"] == "/path/dev"
                && profile["is_default"] == true
        }));
        assert!(profiles.iter().any(|profile| {
            profile["name"] == "prod"
                && profile["data_dir"] == "/path/prod"
                && profile["is_default"] == false
        }));
    }

    #[test]
    fn test_show_table_output() {
        let show = sample_show();
        let mut buffer = Vec::new();
        write_show_table_to(&mut buffer, &show).unwrap();
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("Name:"));
        assert!(output.contains("Data Dir:"));
        assert!(output.contains("Default:"));
        assert!(output.contains("dev"));
        assert!(output.contains("/path/dev"));
        assert!(output.contains("Yes"));
    }

    #[test]
    fn test_show_json_output() {
        let show = sample_show();
        let mut buffer = Vec::new();
        write_show_json_to(&mut buffer, &show).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&buffer).unwrap();
        assert_eq!(value["name"], "dev");
        assert_eq!(value["data_dir"], "/path/dev");
        assert_eq!(value["is_default"], true);
    }
}
