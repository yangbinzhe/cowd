use std::{collections::BTreeSet, fs, path::Path, process::Command};

use serde::Deserialize;
use syn::{spanned::Spanned, visit::Visit};

use super::{has_flag, Roots};

#[derive(Debug, Deserialize)]
struct SourcePolicy {
    structural: StructuralPolicy,
}

#[derive(Debug, Deserialize)]
struct StructuralPolicy {
    core_base: String,
    edge_base: String,
    max_function_lines: usize,
    max_state_fields: usize,
    max_composition_handles: usize,
    max_constructor_arguments: usize,
}

struct RustLimits<'a> {
    path: &'a str,
    policy: &'a StructuralPolicy,
    violations: Vec<String>,
}

impl<'ast> Visit<'ast> for RustLimits<'_> {
    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        self.check_function(
            &node.sig.ident.to_string(),
            node.span(),
            node.sig.inputs.len(),
            matches!(node.vis, syn::Visibility::Public(_)),
        );
        syn::visit::visit_item_fn(self, node);
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        self.check_function(
            &node.sig.ident.to_string(),
            node.span(),
            node.sig.inputs.len(),
            matches!(node.vis, syn::Visibility::Public(_)),
        );
        syn::visit::visit_impl_item_fn(self, node);
    }

    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        let fields = node.fields.len();
        let name = node.ident.to_string();
        let limit = if matches!(
            name.as_str(),
            "RuntimeServices" | "ConversationRuntime" | "RuntimeService" | "CompositionRoot"
        ) {
            self.policy.max_composition_handles
        } else {
            self.policy.max_state_fields
        };
        if fields > limit {
            self.violations.push(format!(
                "{}:{} struct {name} has {fields} fields (max {limit})",
                self.path,
                node.span().start().line
            ));
        }
        syn::visit::visit_item_struct(self, node);
    }
}

impl RustLimits<'_> {
    fn check_function(
        &mut self,
        name: &str,
        span: proc_macro2::Span,
        arguments: usize,
        public: bool,
    ) {
        let start = span.start().line;
        let end = span.end().line;
        let lines = end.saturating_sub(start) + 1;
        if lines > self.policy.max_function_lines {
            self.violations.push(format!(
                "{}:{start} function {name} has {lines} lines (max {})",
                self.path, self.policy.max_function_lines
            ));
        }
        if public
            && (name == "new" || name.starts_with("with_"))
            && arguments > self.policy.max_constructor_arguments
        {
            self.violations.push(format!(
                "{}:{start} public constructor {name} has {arguments} arguments (max {})",
                self.path, self.policy.max_constructor_arguments
            ));
        }
    }
}

pub(super) fn run(roots: &Roots, arguments: &[String]) -> Result<(), String> {
    let policy_path = roots
        .core
        .join("tests/test-governance/source-size-policy.yaml");
    let policy: SourcePolicy = serde_yaml::from_str(
        &fs::read_to_string(&policy_path)
            .map_err(|error| format!("read {}: {error}", policy_path.display()))?,
    )
    .map_err(|error| format!("parse structural policy: {error}"))?;
    let mut violations = Vec::new();
    check_repository(
        &roots.core,
        &policy.structural.core_base,
        &policy.structural,
        &mut violations,
    )?;
    check_repository(
        &roots.edge,
        &policy.structural.edge_base,
        &policy.structural,
        &mut violations,
    )?;
    if !violations.is_empty() {
        return Err(format!(
            "structural limits failed:\n{}",
            violations.join("\n")
        ));
    }
    if has_flag(arguments, "--check") {
        println!("structural-limits gate passed for version diff");
    }
    Ok(())
}

fn check_repository(
    root: &Path,
    base: &str,
    policy: &StructuralPolicy,
    violations: &mut Vec<String>,
) -> Result<(), String> {
    let files = changed_files(root, base)?;
    for relative in files {
        if relative.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let normalized = relative.to_string_lossy().replace('\\', "/");
        let source = fs::read_to_string(root.join(&relative))
            .map_err(|error| format!("read {normalized}: {error}"))?;
        let syntax = syn::parse_file(&source)
            .map_err(|error| format!("parse {normalized} as Rust: {error}"))?;
        let mut visitor = RustLimits {
            path: &normalized,
            policy,
            violations: Vec::new(),
        };
        visitor.visit_file(&syntax);
        violations.extend(visitor.violations);
    }
    Ok(())
}

fn changed_files(root: &Path, base: &str) -> Result<Vec<std::path::PathBuf>, String> {
    let mut files = BTreeSet::new();
    for arguments in [
        vec!["diff", "--name-only", "--diff-filter=ACMR", base, "--"],
        vec!["ls-files", "--others", "--exclude-standard"],
    ] {
        let output = Command::new("git")
            .args(arguments)
            .current_dir(root)
            .output()
            .map_err(|error| format!("inspect changed files in {}: {error}", root.display()))?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
        }
        files.extend(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter(|line| !line.is_empty())
                .map(std::path::PathBuf::from),
        );
    }
    Ok(files.into_iter().collect())
}
