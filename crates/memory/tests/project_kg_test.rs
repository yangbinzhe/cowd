//! Integration tests for project knowledge graph building and entity extraction.

use std::fs;
use std::path::{Path, PathBuf};

use cowd_memory::entity::EntityType;
use cowd_memory::project_scope::build_project_kg;

fn write_file(dir: &Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).ok();
    }
    fs::write(&path, content).expect("write temp file");
    path
}

// ---------------------------------------------------------------------------
// build_project_kg — basic extraction
// ---------------------------------------------------------------------------

#[test]
fn extracts_rust_functions() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/main.rs",
        r#"
fn main() {}

fn helper_one() -> bool { true }

fn process_data(input: &str) -> String {
    input.to_string()
}

struct Config {
    port: u16,
}

trait Handler {
    fn handle(&self);
}

impl Handler for Config {
    fn handle(&self) {}
}
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    // Should find at least main, helper_one, process_data
    let names: Vec<&str> = entities.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"main"),
        "should find fn main(), got: {names:?}"
    );
    assert!(
        names.contains(&"helper_one"),
        "should find fn helper_one(), got: {names:?}"
    );
    assert!(
        names.contains(&"process_data"),
        "should find fn process_data(), got: {names:?}"
    );
}

#[test]
fn extracts_rust_structs_and_traits() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/lib.rs",
        r#"
struct User { id: u64 }
struct Order { total: f64 }
trait Serializable {}
trait Database {}
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    let structs: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::Concept)
        .collect();
    let struct_names: Vec<&str> = structs.iter().map(|e| e.name.as_str()).collect();

    assert!(
        struct_names.contains(&"User"),
        "should find struct User, got: {struct_names:?}"
    );
    assert!(
        struct_names.contains(&"Order"),
        "should find struct Order, got: {struct_names:?}"
    );
    assert!(
        struct_names.contains(&"Serializable"),
        "should find trait Serializable, got: {struct_names:?}"
    );
    assert!(
        struct_names.contains(&"Database"),
        "should find trait Database, got: {struct_names:?}"
    );
}

#[test]
fn extracts_rust_impl_blocks() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/lib.rs",
        r#"
impl MyStruct {
    fn new() -> Self { todo!() }
}
impl SomeTrait for MyStruct {
    fn do_thing() {}
}
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
    let names: Vec<_> = entities.iter().map(|e| e.name.as_str()).collect();

    assert!(
        names.contains(&"MyStruct"),
        "should find impl MyStruct, got: {names:?}"
    );
    assert!(
        names.contains(&"SomeTrait"),
        "should find impl SomeTrait, got: {names:?}"
    );
}

#[test]
fn extracts_rust_enums() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/lib.rs",
        r#"
enum Color { Red, Green, Blue }
enum Status { Active, Inactive }
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
    let names: Vec<_> = entities.iter().map(|e| e.name.as_str()).collect();

    assert!(names.contains(&"Color"), "should find enum Color");
    assert!(names.contains(&"Status"), "should find enum Status");
}

#[test]
fn extracts_typescript_functions_and_classes() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/utils.ts",
        r#"
function getUser(id: string): User { return {} as User; }
function formatDate(date: Date): string { return ""; }
class UserService {
  getUser(id: string): User { return {} as User; }
}
interface Config {
  port: number;
}
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
    let names: Vec<_> = entities.iter().map(|e| e.name.as_str()).collect();

    assert!(
        names.contains(&"getUser"),
        "should find function getUser, got: {names:?}"
    );
    assert!(
        names.contains(&"formatDate"),
        "should find function formatDate, got: {names:?}"
    );
    assert!(
        names.contains(&"UserService"),
        "should find class UserService, got: {names:?}"
    );
    assert!(
        names.contains(&"Config"),
        "should find interface Config, got: {names:?}"
    );
}

#[test]
fn extracts_typescript_tsx_files() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/App.tsx",
        r#"
function App() { return <div />; }
class MyComponent {}
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
    let names: Vec<_> = entities.iter().map(|e| e.name.as_str()).collect();

    assert!(
        names.contains(&"App"),
        "should find function App in tsx, got: {names:?}"
    );
    assert!(
        names.contains(&"MyComponent"),
        "should find class MyComponent in tsx, got: {names:?}"
    );
}

#[test]
fn extracts_python_defs_and_classes() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "app.py",
        r#"
def calculate_total(items):
    return sum(items)

def format_output(data):
    return str(data)

class DataProcessor:
    def process(self):
        pass

class ConfigLoader:
    def load(self):
        pass
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
    let names: Vec<_> = entities.iter().map(|e| e.name.as_str()).collect();

    assert!(
        names.contains(&"calculate_total"),
        "should find def calculate_total, got: {names:?}"
    );
    assert!(
        names.contains(&"format_output"),
        "should find def format_output, got: {names:?}"
    );
    assert!(
        names.contains(&"DataProcessor"),
        "should find class DataProcessor, got: {names:?}"
    );
    assert!(
        names.contains(&"ConfigLoader"),
        "should find class ConfigLoader, got: {names:?}"
    );
}

// ---------------------------------------------------------------------------
// New language extraction (universal scanner)
// ---------------------------------------------------------------------------

#[test]
fn extracts_go_functions_and_types() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "main.go",
        r#"
func main() {}

func processData(input string) string { return input }

type Config struct {
    Port int
}

type Handler interface {
    Serve() error
}
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
    let names: Vec<_> = entities.iter().map(|e| e.name.as_str()).collect();

    assert!(names.contains(&"main"), "should find func main, got: {names:?}");
    assert!(
        names.contains(&"processData"),
        "should find func processData, got: {names:?}"
    );
    assert!(
        names.contains(&"Config"),
        "should find type Config, got: {names:?}"
    );
    assert!(
        names.contains(&"Handler"),
        "should find type Handler, got: {names:?}"
    );
}

#[test]
fn extracts_java_classes_and_methods() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "Service.java",
        r#"
public class UserService {
    public User getUser(String id) { return null; }
    private void logAccess() {}
}

public interface Repository {
    void save(Object entity);
}
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
    let names: Vec<_> = entities.iter().map(|e| e.name.as_str()).collect();

    assert!(
        names.contains(&"UserService"),
        "should find class UserService, got: {names:?}"
    );
    assert!(
        names.contains(&"Repository"),
        "should find interface Repository, got: {names:?}"
    );
    assert!(
        names.contains(&"getUser"),
        "should find method getUser, got: {names:?}"
    );
}

// ---------------------------------------------------------------------------
// Doc file extraction
// ---------------------------------------------------------------------------

#[test]
fn markdown_code_blocks_extract_code_symbols() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "docs/guide.md",
        r#"# API Guide

Example usage:

```rust
fn authenticate(token: &str) -> bool {
    true
}
```

```python
def validate_input(data):
    return True
```
"#,
    );

    let kg = build_project_kg(tmp.path());
    let names: Vec<_> = kg.list_entities().iter().map(|e| e.name.as_str()).collect();

    assert!(
        names.contains(&"authenticate"),
        "should find Rust fn in code block, got: {names:?}"
    );
    assert!(
        names.contains(&"validate_input"),
        "should find Python def in code block, got: {names:?}"
    );
}

// ---------------------------------------------------------------------------
// Config file extraction
// ---------------------------------------------------------------------------

#[test]
fn extracts_yaml_top_level_keys() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "config.yaml",
        r#"
database:
  host: localhost
  port: 5432

server:
  port: 8080
  debug: true

logging:
  level: info
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    let config_keys: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::ConfigKey)
        .map(|e| e.name.as_str())
        .collect();

    assert!(
        config_keys.contains(&"database"),
        "should extract 'database' key, got: {config_keys:?}"
    );
    assert!(
        config_keys.contains(&"server"),
        "should extract 'server' key, got: {config_keys:?}"
    );
    assert!(
        config_keys.contains(&"logging"),
        "should extract 'logging' key, got: {config_keys:?}"
    );
}

#[test]
fn extracts_json_top_level_keys() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "package.json",
        r#"{
  "name": "my-app",
  "version": "1.0.0",
  "dependencies": {
    "react": "^18.0.0"
  }
}"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    let config_keys: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::ConfigKey)
        .map(|e| e.name.as_str())
        .collect();

    assert!(
        config_keys.contains(&"name"),
        "should extract 'name' key, got: {config_keys:?}"
    );
    assert!(
        config_keys.contains(&"version"),
        "should extract 'version' key, got: {config_keys:?}"
    );
    assert!(
        config_keys.contains(&"dependencies"),
        "should extract 'dependencies' key, got: {config_keys:?}"
    );
}

#[test]
fn extracts_toml_sections_and_keys() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "Cargo.toml",
        r#"
[package]
name = "my-crate"
version = "0.1.0"

[dependencies]
serde = "1.0"
"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
    let names: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::ConfigKey)
        .map(|e| e.name.as_str())
        .collect();

    assert!(
        names.contains(&"package"),
        "should find [package] section, got: {names:?}"
    );
    assert!(
        names.contains(&"dependencies"),
        "should find [dependencies] section, got: {names:?}"
    );
    assert!(names.contains(&"name"), "should find name key, got: {names:?}");
    assert!(
        names.contains(&"version"),
        "should find version key, got: {names:?}"
    );
}

// ---------------------------------------------------------------------------
// Web / data file extraction
// ---------------------------------------------------------------------------

#[test]
fn extracts_html_tags_as_data_fields() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "index.html",
        r#"<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>My Page</title></head>
<body>
  <div class="container">
    <h1>Welcome</h1>
    <custom-widget></custom-widget>
  </div>
</body>
</html>"#,
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    let tags: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::DataField)
        .map(|e| e.name.as_str())
        .collect();

    assert!(tags.contains(&"div"), "should find div tag, got: {tags:?}");
    assert!(tags.contains(&"h1"), "should find h1 tag, got: {tags:?}");
    assert!(
        tags.contains(&"custom-widget"),
        "should find custom-widget tag, got: {tags:?}"
    );
    // Structural tags should be excluded
    assert!(
        !tags.contains(&"html"),
        "html tag should be excluded, got: {tags:?}"
    );
    assert!(
        !tags.contains(&"head"),
        "head tag should be excluded, got: {tags:?}"
    );
}

// ---------------------------------------------------------------------------
// Unknown text fallback
// ---------------------------------------------------------------------------

#[test]
fn unknown_text_file_uses_first_line_as_concept() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "data.csv",
        "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n",
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    assert_eq!(
        entities.len(),
        1,
        "unknown text should produce exactly one entity"
    );
    assert_eq!(entities[0].entity_type, EntityType::Concept);
    assert_eq!(entities[0].name, "id,name,email");
}

// ---------------------------------------------------------------------------
// Source attribution
// ---------------------------------------------------------------------------

#[test]
fn source_type_is_set_correctly() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(tmp.path(), "src/lib.rs", "fn my_fn() {}");
    write_file(tmp.path(), "README.md", "# My Project");
    write_file(
        tmp.path(),
        "config.yaml",
        "app:\n  name: test\n",
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    let code_entity = entities.iter().find(|e| e.name == "my_fn").unwrap();
    assert_eq!(code_entity.source_type, "code");

    let doc_entity = entities
        .iter()
        .find(|e| e.name == "My Project")
        .unwrap();
    assert_eq!(doc_entity.source_type, "doc");

    let config_entity = entities.iter().find(|e| e.name == "app").unwrap();
    assert_eq!(config_entity.source_type, "config");
}

#[test]
fn source_ids_use_file_prefix() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(tmp.path(), "src/main.rs", "fn main() {}");

    let kg = build_project_kg(tmp.path());
    let entity = kg.get_entity_by_name("main").expect("main should exist");

    assert_eq!(entity.source_ids.len(), 1);
    assert!(
        entity.source_ids[0].starts_with("file:"),
        "source_id should use 'file:' prefix, got: {}",
        entity.source_ids[0]
    );
    assert!(
        entity.source_ids[0].contains("main.rs"),
        "source_id should reference origin file, got: {}",
        entity.source_ids[0]
    );
}

// ---------------------------------------------------------------------------
// Isolation / no contamination
// ---------------------------------------------------------------------------

#[test]
fn second_project_kg_does_not_contaminate_first() {
    let tmp1 = tempfile::TempDir::new().unwrap();
    let tmp2 = tempfile::TempDir::new().unwrap();

    write_file(tmp1.path(), "src/main.rs", "fn unique_project_one() {}");
    write_file(tmp2.path(), "src/main.rs", "fn unique_project_two() {}");

    let kg1 = build_project_kg(tmp1.path());
    let kg2 = build_project_kg(tmp2.path());

    assert!(
        kg1.get_entity_by_name("unique_project_one").is_some(),
        "kg1 should have unique_project_one"
    );
    assert!(
        kg1.get_entity_by_name("unique_project_two").is_none(),
        "kg1 should NOT have unique_project_two"
    );
    assert!(
        kg2.get_entity_by_name("unique_project_two").is_some(),
        "kg2 should have unique_project_two"
    );
    assert!(
        kg2.get_entity_by_name("unique_project_one").is_none(),
        "kg2 should NOT have unique_project_one"
    );
}

// ---------------------------------------------------------------------------
// Entity type classification
// ---------------------------------------------------------------------------

#[test]
fn functions_get_tool_entity_type() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/lib.rs",
        "fn compute() {}",
    );

    let kg = build_project_kg(tmp.path());
    let entity = kg.get_entity_by_name("compute").expect("compute should exist");
    assert_eq!(
        entity.entity_type,
        EntityType::Tool,
        "functions should be classified as Tool"
    );
}

#[test]
fn structs_and_traits_get_concept_entity_type() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/lib.rs",
        "struct Data {} trait Validator {}",
    );

    let kg = build_project_kg(tmp.path());
    let data_entity = kg.get_entity_by_name("Data").expect("Data should exist");
    let validator_entity = kg
        .get_entity_by_name("Validator")
        .expect("Validator should exist");

    assert_eq!(data_entity.entity_type, EntityType::Concept);
    assert_eq!(validator_entity.entity_type, EntityType::Concept);
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[test]
fn empty_directory_returns_empty_kg() {
    let tmp = tempfile::TempDir::new().unwrap();

    let kg = build_project_kg(tmp.path());
    assert!(
        kg.list_entities().is_empty(),
        "empty directory should produce empty KG"
    );
}

#[test]
fn extracts_markdown_headings_and_terms() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "readme.md",
        "# Project\n\nSome **important** concept.\n\n## Getting Started\n\nAnother _emphasized_ term.\n",
    );

    let kg = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    // Should find headings
    let headings: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::DocHeading)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        headings.contains(&"Project"),
        "should extract '# Project' heading, got: {headings:?}"
    );
    assert!(
        headings.contains(&"Getting Started"),
        "should extract '## Getting Started' heading, got: {headings:?}"
    );

    // Should find bold/strong terms
    let terms: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::DocTerm)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        terms.contains(&"important"),
        "should extract **important** term, got: {terms:?}"
    );
}

#[test]
fn case_sensitive_lookup_with_get_entity_by_name() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(tmp.path(), "main.rs", "fn MyFunc() {}");

    let kg = build_project_kg(tmp.path());

    assert!(kg.get_entity_by_name("MyFunc").is_some());
    assert!(
        kg.get_entity_by_name("myfunc").is_some(),
        "get_entity_by_name should be case-insensitive"
    );
    assert!(kg.get_entity_by_name("MYFUNC").is_some());
}

#[test]
fn entity_source_ids_track_origin_file() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(tmp.path(), "src/foo.rs", "fn foo_fn() {}");

    let kg = build_project_kg(tmp.path());
    let entity = kg.get_entity_by_name("foo_fn").expect("foo_fn should exist");

    assert_eq!(entity.source_ids.len(), 1);
    assert!(
        entity.source_ids[0].contains("foo.rs"),
        "source_id should reference origin file, got: {}",
        entity.source_ids[0]
    );
}
