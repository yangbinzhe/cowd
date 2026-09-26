//! One model-facing address space for every readable fact.
//!
//! Runtime storage is deliberately event-sourced across many streams with
//! different keys. The model must not have to learn that shape. This module is
//! the single canonical grammar the model is shown: one URI per readable fact,
//! with `node://` naming a whole execution-graph node view. Storage adapters
//! resolve an [`Address`] to durable content; nothing here touches stores.

use std::fmt;

/// A parsed model-facing reference. `Bare` preserves backward compatibility for
/// a legacy entity id that carries no scheme, which the directory resolves.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Address {
    File(String),
    Dir(String),
    Program(String),
    Task(String),
    Team(String),
    Agent(String),
    Topic(String),
    Issue(String),
    Criterion(String),
    Obligation(String),
    Review(String),
    Artifact(String),
    Tool(String),
    Session(String),
    Fact(String),
    Matrix(String),
    Memory(String),
    /// A whole execution-graph node view: inputs, outputs, evidence, artifacts.
    Node { graph: String, node: String },
    /// A legacy identifier with no scheme; the caller resolves it by directory.
    Bare(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressError {
    Empty,
    EmptyTarget(String),
    UnknownScheme(String),
    MalformedNode(String),
}

impl fmt::Display for AddressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(formatter, "reference is empty"),
            Self::EmptyTarget(scheme) => {
                write!(formatter, "reference `{scheme}://` has no target")
            }
            Self::UnknownScheme(scheme) => write!(
                formatter,
                "unknown reference scheme `{scheme}://`; supported: file, dir, program, task, team, agent, topic, issue, criterion, obligation, review, artifact, tool, session, fact, matrix, memory, node"
            ),
            Self::MalformedNode(value) => write!(
                formatter,
                "node reference `{value}` must be `node://<graph_id>/<node_id>`"
            ),
        }
    }
}

impl Address {
    /// Parse a model-facing reference. A value without `://` is a legacy `Bare`
    /// id so older citations keep working during migration.
    pub fn parse(raw: &str) -> Result<Self, AddressError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(AddressError::Empty);
        }
        let Some((scheme, target)) = trimmed.split_once("://") else {
            return Ok(Self::Bare(trimmed.to_string()));
        };
        if target.is_empty() {
            return Err(AddressError::EmptyTarget(scheme.to_string()));
        }
        let target = target.to_string();
        let address = match scheme {
            "file" => Self::File(target),
            "dir" => Self::Dir(target),
            "program" => Self::Program(target),
            "task" => Self::Task(target),
            "team" => Self::Team(target),
            "agent" => Self::Agent(target),
            "topic" => Self::Topic(target),
            "issue" => Self::Issue(target),
            "criterion" => Self::Criterion(target),
            "obligation" => Self::Obligation(target),
            "review" => Self::Review(target),
            "artifact" => Self::Artifact(target),
            "tool" => Self::Tool(target),
            "session" => Self::Session(target),
            "fact" => Self::Fact(target),
            "matrix" => Self::Matrix(target),
            "memory" => Self::Memory(target),
            "node" => {
                let Some((graph, node)) = target.split_once('/') else {
                    return Err(AddressError::MalformedNode(trimmed.to_string()));
                };
                if graph.is_empty() || node.is_empty() {
                    return Err(AddressError::MalformedNode(trimmed.to_string()));
                }
                Self::Node {
                    graph: graph.to_string(),
                    node: node.to_string(),
                }
            }
            other => return Err(AddressError::UnknownScheme(other.to_string())),
        };
        Ok(address)
    }

    /// The canonical, round-trippable text form.
    #[must_use]
    pub fn as_uri(&self) -> String {
        match self {
            Self::File(value)
            | Self::Dir(value)
            | Self::Program(value)
            | Self::Task(value)
            | Self::Team(value)
            | Self::Agent(value)
            | Self::Topic(value)
            | Self::Issue(value)
            | Self::Criterion(value)
            | Self::Obligation(value)
            | Self::Review(value)
            | Self::Artifact(value)
            | Self::Tool(value)
            | Self::Session(value)
            | Self::Fact(value)
            | Self::Matrix(value)
            | Self::Memory(value) => format!("{}://{value}", self.scheme()),
            Self::Node { graph, node } => format!("node://{graph}/{node}"),
            Self::Bare(value) => value.clone(),
        }
    }

    #[must_use]
    pub fn scheme(&self) -> &'static str {
        match self {
            Self::File(_) => "file",
            Self::Dir(_) => "dir",
            Self::Program(_) => "program",
            Self::Task(_) => "task",
            Self::Team(_) => "team",
            Self::Agent(_) => "agent",
            Self::Topic(_) => "topic",
            Self::Issue(_) => "issue",
            Self::Criterion(_) => "criterion",
            Self::Obligation(_) => "obligation",
            Self::Review(_) => "review",
            Self::Artifact(_) => "artifact",
            Self::Tool(_) => "tool",
            Self::Session(_) => "session",
            Self::Fact(_) => "fact",
            Self::Matrix(_) => "matrix",
            Self::Memory(_) => "memory",
            Self::Node { .. } => "node",
            Self::Bare(_) => "",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_declared_scheme() {
        for (raw, scheme) in [
            ("file://src/main.rs", "file"),
            ("dir://crates/runtime", "dir"),
            ("program://program:abc", "program"),
            ("task://task:abc", "task"),
            ("team://team:abc", "team"),
            ("agent://agent:abc", "agent"),
            ("topic://topic:abc", "topic"),
            ("issue://issue:abc", "issue"),
            ("criterion://criterion:abc", "criterion"),
            ("obligation://obligation:abc", "obligation"),
            ("review://review:abc", "review"),
            ("artifact://artifact:abc", "artifact"),
            ("tool://tool-raw-call_00_x-abcdef", "tool"),
            ("session://sess:abc", "session"),
            ("fact://fact:abc", "fact"),
            ("matrix://matrix:abc", "matrix"),
            ("memory://memory:abc", "memory"),
        ] {
            let address = Address::parse(raw).expect("declared scheme parses");
            assert_eq!(address.scheme(), scheme, "raw={raw}");
            assert_eq!(address.as_uri(), raw, "round-trip raw={raw}");
        }
    }

    #[test]
    fn parses_node_with_graph_and_node_id() {
        let address = Address::parse("node://graph:g1/agent:2").expect("node parses");
        assert_eq!(
            address,
            Address::Node {
                graph: "graph:g1".to_string(),
                node: "agent:2".to_string()
            }
        );
        assert_eq!(address.as_uri(), "node://graph:g1/agent:2");
    }

    #[test]
    fn bare_id_stays_resolvable_by_directory() {
        let address = Address::parse("task:abc").expect("bare id parses");
        assert_eq!(address, Address::Bare("task:abc".to_string()));
        assert_eq!(address.scheme(), "");
    }

    #[test]
    fn tool_ref_keeps_the_short_call_id_without_a_hash() {
        // The model may cite only the stable call id; prefix resolution happens
        // in the store, not here, so the parser must not require a hash suffix.
        let address = Address::parse("tool://tool-raw-call_00_qBJ9X5mlnWf0wuqa8lE07310")
            .expect("short tool ref parses");
        assert_eq!(
            address,
            Address::Tool("tool-raw-call_00_qBJ9X5mlnWf0wuqa8lE07310".to_string())
        );
    }

    #[test]
    fn rejects_empty_and_unknown_and_malformed() {
        assert_eq!(Address::parse("   "), Err(AddressError::Empty));
        assert_eq!(
            Address::parse("task://"),
            Err(AddressError::EmptyTarget("task".to_string()))
        );
        assert_eq!(
            Address::parse("weird://x"),
            Err(AddressError::UnknownScheme("weird".to_string()))
        );
        assert!(matches!(
            Address::parse("node://onlygraph"),
            Err(AddressError::MalformedNode(_))
        ));
        assert!(matches!(
            Address::parse("node:///node"),
            Err(AddressError::MalformedNode(_))
        ));
    }
}
