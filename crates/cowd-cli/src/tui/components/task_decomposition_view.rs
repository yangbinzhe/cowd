// ── Task Decomposition View ────────────────────────────────────────
// Displays a DAG-based tree of subtasks decomposed by the
// CollaborationOrchestrator. Each subtask shows description,
// required skills, dependency status, and tree indentation
// derived from the dependency graph depth.
//
// Features:
//   - sync_from_orchestrator: fetch subtasks from CollaborationOps
//   - DAG-based tree indentation
//   - Status: depends_on satisfied = ready, else pending
//   - Expand/collapse individual subtasks
//   - Toggle overall visibility
// -----------------------------------------------------------------

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame,
};

use runtime::{CollaborationOps, SubTask};

/// Renders a dependency-aware tree view of decomposed subtasks.
///
/// Built from a `SubTask` list obtained via [`CollaborationOps::decompose_task`].
/// The dependency graph (DAG) drives indentation depth and readiness status.
pub struct TaskDecompositionView {
    /// Subtasks from the orchestrator.
    subtasks: Vec<SubTask>,
    /// Dependency DAG: task_id → list of immediate child ids (tasks that depend on this).
    dag: HashMap<String, Vec<String>>,
    /// Set of expanded subtask ids for detail view.
    expanded: HashSet<String>,
    /// Whether the panel is visible (toggled externally).
    visible: bool,
}

impl TaskDecompositionView {
    /// Create a new, empty, hidden view.
    #[must_use]
    pub fn new() -> Self {
        Self {
            subtasks: Vec::new(),
            dag: HashMap::new(),
            expanded: HashSet::new(),
            visible: false,
        }
    }

    /// Sync subtasks from a [`CollaborationOps`] implementation.
    ///
    /// Calls `orch.decompose_task(task)` and rebuilds the DAG.
    pub fn sync_from_orchestrator(&mut self, orch: &dyn CollaborationOps, task: &str) {
        self.subtasks = orch.decompose_task(task);
        self.build_dag();
    }

    /// Build the dependency DAG from `SubTask.depends_on` fields.
    ///
    /// Each entry in `depends_on` represents an edge `dep → sub.id`.
    /// The DAG stores `dep_id → [child_ids]` for tree traversal.
    fn build_dag(&mut self) {
        self.dag.clear();
        for sub in &self.subtasks {
            for dep in &sub.depends_on {
                self.dag.entry(dep.clone()).or_default().push(sub.id.clone());
            }
        }
    }

    /// Toggle panel visibility.
    pub fn toggle(&mut self) {
        self.visible = !self.visible;
        if !self.visible {
            self.expanded.clear();
        }
    }

    /// Toggle the expanded detail state for a subtask.
    pub fn toggle_expand(&mut self, task_id: &str) {
        if self.expanded.contains(task_id) {
            self.expanded.remove(task_id);
        } else {
            self.expanded.insert(task_id.to_string());
        }
    }

    // ── Rendering ──────────────────────────────────────────────────

    /// Render the task decomposition tree into the given area.
    ///
    /// Tree indentation is computed from DAG depth (distance from
    /// root nodes with no incoming dependencies). Status is shown as
    /// ✅ ready (all deps present in the subtask list) or ⏳ pending.
    pub fn render(&mut self, area: Rect, frame: &mut Frame) {
        if !self.visible {
            return;
        }

        if area.width < 10 || area.height < 3 {
            return;
        }

        let title = if self.subtasks.is_empty() {
            " Task Decomposition (empty) ".to_string()
        } else {
            format!(" Task Decomposition ({}) ", self.subtasks.len())
        };

        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .fg(Color::Cyan);

        let inner = block.inner(area);
        frame.render_widget(block, area);

        if self.subtasks.is_empty() {
            let message = Paragraph::new("No subtasks. Press 's' to sync from orchestrator.")
                .style(Style::default().fg(Color::DarkGray))
                .wrap(Wrap { trim: false });
            frame.render_widget(message, inner);
            return;
        }

        // Compute tree depths and render each subtask.
        let depths = self.compute_depths();

        let mut lines: Vec<Line> = Vec::new();

        // Header
        lines.push(Line::from(Span::styled(
            " Subtask Tree (indented by dependency depth)",
            Style::default().fg(Color::Yellow),
        )));
        lines.push(Line::raw(""));

        for sub in &self.subtasks {
            let depth = depths.get(&sub.id).copied().unwrap_or(0);
            let is_expanded = self.expanded.contains(&sub.id);

            // Status: all depends_on present in subtask list = ready
            let deps_satisfied = self.all_deps_satisfied(sub);
            let (status_icon, status_label, status_color) = if deps_satisfied {
                ("✅", "ready", Color::Green)
            } else {
                ("⏳", "pending", Color::Yellow)
            };

            let indent = "  ".repeat(depth as usize);

            // Main line: tree branch + icon + description + status
            let branch = if depth > 0 { "└─ " } else { "" };
            lines.push(Line::from(vec![
                Span::raw(indent.clone()),
                Span::styled(
                    format!("{branch}{status_icon} {id}", id = sub.id),
                    Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled(
                    format!("[{status_label}]"),
                    Style::default().fg(status_color),
                ),
            ]));

            // Description line
            lines.push(Line::from(vec![
                Span::raw(format!("{indent}   ")),
                Span::styled(
                    &sub.description,
                    Style::default().fg(Color::Gray),
                ),
            ]));

            // Skills line
            if !sub.required_skills.is_empty() {
                let skills_str = sub.required_skills.join(", ");
                lines.push(Line::from(vec![
                    Span::raw(format!("{indent}   ")),
                    Span::styled(
                        format!("skills: [{skills_str}]"),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }

            // Depends-on line (only when there are dependencies)
            if !sub.depends_on.is_empty() {
                let deps_str = sub.depends_on.join(", ");
                lines.push(Line::from(vec![
                    Span::raw(format!("{indent}   ")),
                    Span::styled(
                        format!("depends on: [{deps_str}]"),
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::ITALIC),
                    ),
                ]));
            }

            // Expanded detail separator
            if is_expanded {
                lines.push(Line::from(Span::styled(
                    format!("{indent}   ── Details ──"),
                    Style::default().fg(Color::DarkGray),
                )));
                lines.push(Line::from(vec![
                    Span::raw(format!("{indent}   ")),
                    Span::styled(
                        format!("id: {}", sub.id),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::raw(format!("{indent}   ")),
                    Span::styled(
                        format!("children: {}",
                            self.dag.get(&sub.id).map_or(0, |v| v.len())),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }

            // Spacer between subtasks
            lines.push(Line::raw(""));
        }

        // Keyboard hint
        lines.push(Line::from(Span::styled(
            "s:sync  Tab:toggle  Enter:expand  +/-:collapse all",
            Style::default().fg(Color::DarkGray),
        )));

        let paragraph = Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: false });
        frame.render_widget(paragraph, inner);
    }

    // ── DAG helpers ────────────────────────────────────────────────

    /// Compute the depth of each node in the DAG.
    ///
    /// Depth is the maximum distance from a root node (no incoming edges).
    /// Root nodes have depth 0; their children have depth 1, and so on.
    fn compute_depths(&self) -> HashMap<String, u32> {
        // Identify root nodes: subtasks not listed as a dependency of any other subtask.
        let has_parent: HashSet<&str> = self
            .subtasks
            .iter()
            .flat_map(|s| s.depends_on.iter().map(String::as_str))
            .collect();

        let roots: Vec<&SubTask> = self
            .subtasks
            .iter()
            .filter(|s| !has_parent.contains(s.id.as_str()))
            .collect();

        let mut depths: HashMap<String, u32> = HashMap::new();

        // If everything has a parent (circular or all interdependent),
        // treat the first subtask as the root.
        if roots.is_empty() && !self.subtasks.is_empty() {
            self.assign_depth(&self.subtasks[0].id, 0, &mut depths);
        } else {
            for root in roots {
                self.assign_depth(&root.id, 0, &mut depths);
            }
        }

        // Ensure all nodes have a depth (handle disconnected nodes).
        for sub in &self.subtasks {
            depths.entry(sub.id.clone()).or_insert(0);
        }

        depths
    }

    /// Recursively assign depth to a node and its descendants.
    fn assign_depth(
        &self,
        node_id: &str,
        depth: u32,
        depths: &mut HashMap<String, u32>,
    ) {
        let current = depths.entry(node_id.to_string()).or_insert(depth);
        *current = (*current).max(depth);

        if let Some(children) = self.dag.get(node_id) {
            for child in children {
                self.assign_depth(child, depth + 1, depths);
            }
        }
    }

    /// Check whether all dependencies of a subtask are present in the
    /// subtask list (i.e., were decomposed together).
    fn all_deps_satisfied(&self, sub: &SubTask) -> bool {
        sub.depends_on
            .iter()
            .all(|dep_id| self.subtasks.iter().any(|s| s.id == *dep_id))
    }
}

// ── Default ────────────────────────────────────────────────────────

impl Default for TaskDecompositionView {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_subtask(
        id: &str,
        desc: &str,
        skills: Vec<&str>,
        deps: Vec<&str>,
    ) -> SubTask {
        SubTask {
            id: id.to_string(),
            description: desc.to_string(),
            required_skills: skills.into_iter().map(String::from).collect(),
            depends_on: deps.into_iter().map(String::from).collect(),
        }
    }

    // ── DAG construction ─────────────────────────────────────────

    #[test]
    fn build_dag_populates_from_depends_on() {
        let mut view = TaskDecompositionView::new();
        view.subtasks = vec![
            make_subtask("a", "Task A", vec!["rust"], vec![]),
            make_subtask("b", "Task B", vec!["testing"], vec!["a"]),
            make_subtask("c", "Task C", vec!["docs"], vec!["a", "b"]),
        ];
        view.build_dag();

        // a → [b, c], b → [c]
        let a_children = view.dag.get("a").unwrap();
        assert!(a_children.contains(&"b".to_string()));
        assert!(a_children.contains(&"c".to_string()));

        let b_children = view.dag.get("b").unwrap();
        assert!(b_children.contains(&"c".to_string()));
    }

    #[test]
    fn build_dag_handles_no_dependencies() {
        let mut view = TaskDecompositionView::new();
        view.subtasks = vec![
            make_subtask("x", "Standalone", vec!["general"], vec![]),
        ];
        view.build_dag();
        assert!(view.dag.is_empty());
    }

    // ── Depth computation ────────────────────────────────────────

    #[test]
    fn compute_depths_linear_chain() {
        let mut view = TaskDecompositionView::new();
        view.subtasks = vec![
            make_subtask("s1", "First", vec!["planning"], vec![]),
            make_subtask("s2", "Second", vec!["rust"], vec!["s1"]),
            make_subtask("s3", "Third", vec!["testing"], vec!["s2"]),
        ];
        view.build_dag();

        let depths = view.compute_depths();
        assert_eq!(depths["s1"], 0);
        assert_eq!(depths["s2"], 1);
        assert_eq!(depths["s3"], 2);
    }

    #[test]
    fn compute_depths_forked_dag() {
        let mut view = TaskDecompositionView::new();
        // root → a, root → b; a → c; b → c
        view.subtasks = vec![
            make_subtask("root", "Root", vec!["planning"], vec![]),
            make_subtask("a", "Branch A", vec!["rust"], vec!["root"]),
            make_subtask("b", "Branch B", vec!["docs"], vec!["root"]),
            make_subtask("c", "Merge", vec!["testing"], vec!["a", "b"]),
        ];
        view.build_dag();

        let depths = view.compute_depths();
        assert_eq!(depths["root"], 0);
        assert_eq!(depths["a"], 1);
        assert_eq!(depths["b"], 1);
        assert_eq!(depths["c"], 2, "c should be depth 2 (max of a→c and b→c)");
    }

    #[test]
    fn compute_depths_all_roots() {
        let mut view = TaskDecompositionView::new();
        view.subtasks = vec![
            make_subtask("p1", "Parallel 1", vec!["rust"], vec![]),
            make_subtask("p2", "Parallel 2", vec!["docs"], vec![]),
            make_subtask("p3", "Parallel 3", vec!["testing"], vec![]),
        ];
        view.build_dag();

        let depths = view.compute_depths();
        assert_eq!(depths["p1"], 0);
        assert_eq!(depths["p2"], 0);
        assert_eq!(depths["p3"], 0);
    }

    // ── Dependency satisfaction ──────────────────────────────────

    #[test]
    fn all_deps_satisfied_when_present() {
        let mut view = TaskDecompositionView::new();
        view.subtasks = vec![
            make_subtask("a", "A", vec![], vec![]),
            make_subtask("b", "B", vec![], vec!["a"]),
        ];
        view.build_dag();
        assert!(view.all_deps_satisfied(&view.subtasks[1])); // b depends on a, a is present
    }

    #[test]
    fn all_deps_satisfied_false_when_missing() {
        let mut view = TaskDecompositionView::new();
        view.subtasks = vec![
            make_subtask("b", "B", vec![], vec!["missing"]),
        ];
        view.build_dag();
        assert!(!view.all_deps_satisfied(&view.subtasks[0]));
    }

    #[test]
    fn all_deps_satisfied_no_deps() {
        let mut view = TaskDecompositionView::new();
        view.subtasks = vec![
            make_subtask("a", "A", vec![], vec![]),
        ];
        view.build_dag();
        assert!(view.all_deps_satisfied(&view.subtasks[0]));
    }

    // ── Visibility toggle ────────────────────────────────────────

    #[test]
    fn toggle_flips_visibility() {
        let mut view = TaskDecompositionView::new();
        assert!(!view.visible);
        view.toggle();
        assert!(view.visible);
        view.toggle();
        assert!(!view.visible);
    }

    #[test]
    fn toggle_clears_expanded_when_hiding() {
        let mut view = TaskDecompositionView::new();
        view.visible = true;
        view.expanded.insert("task-1".to_string());
        view.toggle(); // hide
        assert!(!view.visible);
        assert!(view.expanded.is_empty());
    }

    // ── Expand toggle ────────────────────────────────────────────

    #[test]
    fn toggle_expand_adds_and_removes() {
        let mut view = TaskDecompositionView::new();
        assert!(!view.expanded.contains("x"));
        view.toggle_expand("x");
        assert!(view.expanded.contains("x"));
        view.toggle_expand("x");
        assert!(!view.expanded.contains("x"));
    }

    // ── Default ──────────────────────────────────────────────────

    #[test]
    fn default_is_empty_and_hidden() {
        let view = TaskDecompositionView::default();
        assert!(view.subtasks.is_empty());
        assert!(view.dag.is_empty());
        assert!(view.expanded.is_empty());
        assert!(!view.visible);
    }
}
