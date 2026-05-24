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
fn no_matching_symbols_returns_empty_kg() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_file(
        tmp.path(),
        "readme.md",
        "# Project\n\nNo code symbols here.\n",
    );

    let kg = build_project_kg(tmp.path());
    assert!(
        kg.list_entities().is_empty(),
        "non-code files should produce no entities"
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
