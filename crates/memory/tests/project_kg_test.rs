//! Integration tests for project knowledge graph building and entity extraction.

use std::fs;
use std::path::{Path, PathBuf};

use memory::entity::EntityType;
use memory::project_scope::{build_project_kg, ProjectScopeManager};

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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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
struct MyStruct;
impl MyStruct {
    fn new() -> Self { Self }
}
impl SomeTrait for MyStruct {
    fn do_thing() {}
}
"#,
    );

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
    let names: Vec<_> = entities.iter().map(|e| e.name.as_str()).collect();

    assert!(
        names.contains(&"main"),
        "should find func main, got: {names:?}"
    );
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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
    assert!(
        names.contains(&"name"),
        "should find name key, got: {names:?}"
    );
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
  <main>
    <nav aria-label="Main">
      <custom-widget role="button"></custom-widget>
    </nav>
    <div class="container">
      <h1>Welcome</h1>
    </div>
  </main>
</body>
</html>"#,
    );

    let (kg, _mtimes) = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    // Regular tags → DataField
    let data_fields: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::DataField)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        data_fields.contains(&"div"),
        "should find div tag as DataField, got: {data_fields:?}"
    );
    assert!(
        data_fields.contains(&"h1"),
        "should find h1 tag as DataField, got: {data_fields:?}"
    );

    // Custom elements (dash-case) → Concept
    let concepts: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::Concept)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        concepts.contains(&"custom-widget"),
        "custom-widget should be Concept, got: {concepts:?}"
    );

    // Semantic tags → DocHeading
    let headings: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::DocHeading)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        headings.contains(&"main"),
        "main should be DocHeading, got: {headings:?}"
    );
    assert!(
        headings.contains(&"nav"),
        "nav should be DocHeading, got: {headings:?}"
    );

    // Aria roles → ConfigKey
    let config_keys: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::ConfigKey)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        config_keys.contains(&"button"),
        "role=button should be ConfigKey, got: {config_keys:?}"
    );

    // Structural tags should be excluded
    let all_names: Vec<_> = entities.iter().map(|e| e.name.as_str()).collect();
    assert!(
        !all_names.contains(&"html"),
        "html tag should be excluded, got: {all_names:?}"
    );
    assert!(
        !all_names.contains(&"head"),
        "head tag should be excluded, got: {all_names:?}"
    );
}

// ---------------------------------------------------------------------------
// CSS / SCSS / LESS extraction
// ---------------------------------------------------------------------------

#[test]
fn extracts_css_selectors_and_keyframes() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "styles.css",
        r#"
.container {
    display: flex;
}

#main-header {
    background: red;
}

@keyframes fadeIn {
    from { opacity: 0; }
    to { opacity: 1; }
}

@media (max-width: 768px) {
    .container { flex-direction: column; }
}

@media (min-width: 1024px) {
    .container { flex-direction: row; }
}

.card-title {
    font-size: 1.5rem;
}
#sidebar {
    width: 300px;
}
"#,
    );

    let (kg, _mtimes) = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    let data_fields: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::DataField)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        data_fields.contains(&"container"),
        "should extract .container class, got: {data_fields:?}"
    );
    assert!(
        data_fields.contains(&"card-title"),
        "should extract .card-title class, got: {data_fields:?}"
    );

    // ID selectors → DataField
    assert!(
        data_fields.contains(&"main-header"),
        "should extract #main-header id, got: {data_fields:?}"
    );
    assert!(
        data_fields.contains(&"sidebar"),
        "should extract #sidebar id, got: {data_fields:?}"
    );

    // @keyframes names → Concept
    let concepts: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::Concept)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        concepts.contains(&"fadeIn"),
        "should extract @keyframes fadeIn as Concept, got: {concepts:?}"
    );

    // @media queries counted
    let media_entity = entities.iter().find(|e| e.name.contains("media-queries"));
    assert!(
        media_entity.is_some(),
        "should have a media-queries count entity, got {count} entities",
        count = entities.len()
    );
}

#[test]
fn extracts_scss_and_less_selectors() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "theme.scss",
        r#"
$primary: #333;

.navbar {
    color: $primary;
    &.active {
        font-weight: bold;
    }
}
"#,
    );

    write_file(
        tmp.path(),
        "theme.less",
        r#"
@primary: #333;

.sidebar {
    color: @primary;
}
"#,
    );

    let (kg, _mtimes) = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    let data_fields: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::DataField)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        data_fields.contains(&"navbar"),
        "should extract .navbar from scss, got: {data_fields:?}"
    );
    assert!(
        data_fields.contains(&"sidebar"),
        "should extract .sidebar from less, got: {data_fields:?}"
    );
}

// ---------------------------------------------------------------------------
// Vue SFC extraction
// ---------------------------------------------------------------------------

#[test]
fn extracts_vue_component_name_and_template_elements() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/App.vue",
        r#"<script>
export default {
    name: 'AppLayout',
    methods: {
        handleClick() {
            console.log('clicked');
        }
    }
};
</script>

<template>
  <div class="app">
    <nav-bar></nav-bar>
    <main-content></main-content>
    <custom-footer :items="links"></custom-footer>
  </div>
</template>

<style scoped>
.app { display: flex; }
</style>"#,
    );

    let (kg, _mtimes) = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    // Component name → Concept
    let concepts: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::Concept)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        concepts.contains(&"AppLayout"),
        "should extract component name AppLayout as Concept, got: {concepts:?}"
    );

    // Script methods → Tool
    let tools: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::Tool)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        tools.contains(&"handleClick"),
        "should extract method handleClick as Tool, got: {tools:?}"
    );

    // Template elements → DataField
    let data_fields: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::DataField)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        data_fields.contains(&"div"),
        "should extract div from template, got: {data_fields:?}"
    );
    assert!(
        data_fields.contains(&"nav-bar"),
        "should extract nav-bar from template, got: {data_fields:?}"
    );
    assert!(
        data_fields.contains(&"main-content"),
        "should extract main-content from template, got: {data_fields:?}"
    );
    assert!(
        data_fields.contains(&"custom-footer"),
        "should extract custom-footer from template, got: {data_fields:?}"
    );

    // source_type should be "frontend"
    let app_entity = entities.iter().find(|e| e.name == "AppLayout").unwrap();
    assert_eq!(app_entity.source_type, "frontend");
}

#[test]
fn vue_fallback_component_name_from_filename() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/MyWidget.vue",
        r#"<script>
export default {
    props: ['title']
};
</script>

<template>
  <span>{{ title }}</span>
</template>"#,
    );

    let (kg, _mtimes) = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    let concepts: Vec<_> = entities
        .iter()
        .filter(|e| e.entity_type == EntityType::Concept)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        concepts.contains(&"MyWidget"),
        "should use filename as fallback component name, got: {concepts:?}"
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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
    write_file(tmp.path(), "config.yaml", "app:\n  name: test\n");

    let (kg, _mtimes) = build_project_kg(tmp.path());
    let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();

    let code_entity = entities.iter().find(|e| e.name == "my_fn").unwrap();
    assert_eq!(code_entity.source_type, "code");

    let doc_entity = entities.iter().find(|e| e.name == "My Project").unwrap();
    assert_eq!(doc_entity.source_type, "doc");

    let config_entity = entities.iter().find(|e| e.name == "app").unwrap();
    assert_eq!(config_entity.source_type, "config");
}

#[test]
fn source_ids_use_file_prefix() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(tmp.path(), "src/main.rs", "fn main() {}");

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg1, _mtimes1) = build_project_kg(tmp1.path());
    let (kg2, _mtimes2) = build_project_kg(tmp2.path());

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

    write_file(tmp.path(), "src/lib.rs", "fn compute() {}");

    let (kg, _mtimes) = build_project_kg(tmp.path());
    let entity = kg
        .get_entity_by_name("compute")
        .expect("compute should exist");
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());
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

    let (kg, _mtimes) = build_project_kg(tmp.path());

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

    let (kg, _mtimes) = build_project_kg(tmp.path());
    let entity = kg
        .get_entity_by_name("foo_fn")
        .expect("foo_fn should exist");

    assert_eq!(entity.source_ids.len(), 1);
    assert!(
        entity.source_ids[0].contains("foo.rs"),
        "source_id should reference origin file, got: {}",
        entity.source_ids[0]
    );
}

// ---------------------------------------------------------------------------
// T0: File change triggers KG rebuild (staleness detection)
// ---------------------------------------------------------------------------

#[test]
fn test_file_change_triggers_kg_rebuild() {
    let tmp = tempfile::TempDir::new().unwrap();

    // 1. Create a project with a .rs file containing "fn old_function()"
    let proj = tmp.path().join("my_proj");
    fs::create_dir(&proj).unwrap();
    fs::write(proj.join("lib.rs"), "fn old_function() {}\n").unwrap();

    // 2. Register project and build KG
    let mgr = ProjectScopeManager::new(tmp.path().join("global.db")).unwrap();
    let pid = mgr.register_project(&proj).unwrap();

    // 3. Assert: is_kg_stale() returns false immediately after registration
    assert!(
        !mgr.is_kg_stale(&pid).unwrap(),
        "KG should NOT be stale immediately after registration"
    );

    // 4. Modify the file: add "fn new_function()"
    // Small sleep to ensure mtime actually changes (some FS have 1s granularity)
    std::thread::sleep(std::time::Duration::from_secs(1));
    fs::write(
        proj.join("lib.rs"),
        "fn old_function() {}\nfn new_function() {}\n",
    )
    .unwrap();

    // 5. Assert: is_kg_stale() returns true
    assert!(
        mgr.is_kg_stale(&pid).unwrap(),
        "KG should be stale after file modification"
    );
}

#[test]
fn test_is_kg_stale_unknown_project() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mgr = ProjectScopeManager::new(tmp.path().join("global.db")).unwrap();

    let result = mgr.is_kg_stale("nonexistent_project_id");
    assert!(
        result.is_err(),
        "is_kg_stale should return error for unknown project"
    );
}

// ---------------------------------------------------------------------------
// TDD: unified_scan tests
// ---------------------------------------------------------------------------

#[cfg(feature = "code-index")]
#[test]
fn test_unified_scan_produces_both_entities_and_symbols() {
    use memory::code_indexer::CodeIndexer;
    use memory::project_scope::unified_scan;

    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "src/lib.rs",
        r#"
/// Authenticate a user with the given credentials.
pub fn authenticate_user(username: &str, password: &str) -> bool {
    validate_credentials(username, password)
}

fn validate_credentials(username: &str, password: &str) -> bool {
    !username.is_empty() && !password.is_empty()
}

pub struct AuthService {
    pub enabled: bool,
}

pub enum AuthError {
    InvalidCredentials,
    ExpiredToken,
}
"#,
    );

    let mut indexer = CodeIndexer::new(tmp.path()).expect("create code indexer");
    let result = unified_scan(tmp.path(), Some(&mut indexer));

    // Regex-based KG extraction should find entities
    let entities: Vec<_> = result.kg.list_entities().into_iter().cloned().collect();
    let names: Vec<&str> = entities.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"authenticate_user"),
        "regex should find fn authenticate_user, got: {names:?}"
    );
    assert!(
        names.contains(&"AuthService"),
        "regex should find struct AuthService, got: {names:?}"
    );
    assert!(
        names.contains(&"AuthError"),
        "regex should find enum AuthError, got: {names:?}"
    );

    // Tree-sitter extraction should find code symbols
    let sym_names: Vec<&str> = result.symbols.iter().map(|s| s.name.as_str()).collect();
    assert!(
        !sym_names.is_empty(),
        "tree-sitter should find symbols, got empty"
    );
    assert!(
        sym_names.contains(&"authenticate_user"),
        "tree-sitter should find authenticate_user, got: {sym_names:?}"
    );
    assert!(
        sym_names.contains(&"AuthService"),
        "tree-sitter should find AuthService, got: {sym_names:?}"
    );

    // mtimes should be tracked
    assert!(!result.mtimes.is_empty(), "should track file mtimes");

    // Edge edges should be found (authenticate_user calls validate_credentials)
    let has_calls = result
        .edges
        .iter()
        .any(|e| e.edge_type == memory::code_indexer::SymbolEdgeType::Calls);
    assert!(has_calls, "should find call edges between functions");
}

#[test]
fn test_regex_cached_on_second_call() {
    use memory::project_scope::unified_scan;
    use std::time::Instant;

    let tmp = tempfile::TempDir::new().unwrap();

    // Create several files for a non-trivial scan
    write_file(
        tmp.path(),
        "src/lib.rs",
        "fn foo() {}\nfn bar() {}\nstruct S {}\n",
    );
    write_file(tmp.path(), "src/main.rs", "fn main() {}\nfn helper() {}\n");
    write_file(
        tmp.path(),
        "README.md",
        "# Project\n\nSome **important** text.\n",
    );
    write_file(
        tmp.path(),
        "config.yaml",
        "app:\n  name: test\n  version: 1\n",
    );

    // First call: cold (regexes compiled and cached)
    let start1 = Instant::now();
    let _result1 = unified_scan(tmp.path(), None);
    let duration1 = start1.elapsed();

    // Second call: warm (regexes served from OnceLock cache)
    let start2 = Instant::now();
    let _result2 = unified_scan(tmp.path(), None);
    let duration2 = start2.elapsed();

    // The second call should NOT be significantly slower than the first.
    // With caching, it should be at least as fast (usually faster since
    // regex compilation is skipped).
    let ratio = duration2.as_nanos() as f64 / duration1.as_nanos().max(1) as f64;
    assert!(
        ratio <= 1.15,
        "second call ({duration2:?}) should not be >15% slower than first ({duration1:?}), got ratio {ratio:.2}"
    );
}
