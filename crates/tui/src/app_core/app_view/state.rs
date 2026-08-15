use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cowd_app_protocol::{
    AppActionV1, AppComponentKindV1, AppComponentV1, AppViewDocumentV1, AppViewPatchOperationV1,
    AppViewPatchV1, ProtocolValidate,
};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use serde_json::{json, Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppViewStateLimits {
    pub maximum_document_bytes: usize,
    pub maximum_components: usize,
    pub maximum_actions: usize,
    pub maximum_bindings: usize,
    pub maximum_form_bytes: usize,
    pub maximum_patch_operations: usize,
    pub maximum_scroll: usize,
    pub maximum_recent_revisions: usize,
}

impl Default for AppViewStateLimits {
    fn default() -> Self {
        Self {
            maximum_document_bytes: 1_048_576,
            maximum_components: 4_096,
            maximum_actions: 1_024,
            maximum_bindings: 4_096,
            maximum_form_bytes: 65_536,
            maximum_patch_operations: 4_096,
            maximum_scroll: 1_000_000,
            maximum_recent_revisions: 256,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppViewStateError {
    InvalidDocument(String),
    IdentityMismatch,
    StaleRevision,
    PatchBaseMismatch,
    InvalidPatchPath(String),
    ResourceLimit(&'static str),
    ActionUnavailable,
}

impl fmt::Display for AppViewStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDocument(reason) => {
                write!(formatter, "invalid APP view document: {reason}")
            }
            Self::IdentityMismatch => formatter.write_str("APP view identity does not match"),
            Self::StaleRevision => formatter.write_str("APP view revision is stale or replayed"),
            Self::PatchBaseMismatch => {
                formatter.write_str("APP view patch base revision does not match")
            }
            Self::InvalidPatchPath(path) => {
                write!(formatter, "invalid APP view patch path: {path}")
            }
            Self::ResourceLimit(resource) => write!(formatter, "APP view exceeds {resource} limit"),
            Self::ActionUnavailable => formatter.write_str("APP action is unavailable"),
        }
    }
}

impl std::error::Error for AppViewStateError {}

#[derive(Debug, Clone, PartialEq)]
pub enum AppViewInputResult {
    Ignored,
    StateChanged,
    ConfirmationRequired { action_id: String },
    Action(AppActionV1),
}

#[derive(Debug, Clone)]
pub struct AppViewState {
    document: AppViewDocumentV1,
    focus_order: Vec<String>,
    focus_index: usize,
    selection: BTreeMap<String, usize>,
    scroll: BTreeMap<String, usize>,
    form: BTreeMap<String, String>,
    pending_confirmation: Option<String>,
    recent_revisions: Vec<String>,
    limits: AppViewStateLimits,
}

impl AppViewState {
    pub fn new(document: AppViewDocumentV1) -> Result<Self, AppViewStateError> {
        Self::with_limits(document, AppViewStateLimits::default())
    }

    pub fn with_limits(
        document: AppViewDocumentV1,
        limits: AppViewStateLimits,
    ) -> Result<Self, AppViewStateError> {
        validate_document(&document, limits)?;
        let focus_order = focus_order(&document.root);
        let focus_index = document
            .focus_component_id
            .as_ref()
            .and_then(|id| focus_order.iter().position(|candidate| candidate == id))
            .unwrap_or(0);
        let revision = document.revision.clone();
        Ok(Self {
            document,
            focus_order,
            focus_index,
            selection: BTreeMap::new(),
            scroll: BTreeMap::new(),
            form: BTreeMap::new(),
            pending_confirmation: None,
            recent_revisions: vec![revision],
            limits,
        })
    }

    #[must_use]
    pub fn document(&self) -> &AppViewDocumentV1 {
        &self.document
    }

    #[must_use]
    pub fn focused_component_id(&self) -> Option<&str> {
        self.focus_order.get(self.focus_index).map(String::as_str)
    }

    #[must_use]
    pub fn selection_for(&self, component_id: &str) -> usize {
        self.selection.get(component_id).copied().unwrap_or(0)
    }

    #[must_use]
    pub fn scroll_for(&self, component_id: &str) -> usize {
        self.scroll.get(component_id).copied().unwrap_or(0)
    }

    #[must_use]
    pub fn form_value(&self, component_id: &str) -> &str {
        self.form
            .get(component_id)
            .map(String::as_str)
            .unwrap_or("")
    }

    pub fn replace_document(
        &mut self,
        document: AppViewDocumentV1,
    ) -> Result<(), AppViewStateError> {
        if document.app_id != self.document.app_id || document.view_id != self.document.view_id {
            return Err(AppViewStateError::IdentityMismatch);
        }
        validate_document(&document, self.limits)?;
        self.ensure_new_revision(&document.revision)?;
        self.document = document;
        self.reconcile_after_document_change();
        Ok(())
    }

    pub fn apply_patch(&mut self, patch: &AppViewPatchV1) -> Result<(), AppViewStateError> {
        patch
            .validate()
            .map_err(|error| AppViewStateError::InvalidDocument(error.to_string()))?;
        if patch.app_id != self.document.app_id || patch.view_id != self.document.view_id {
            return Err(AppViewStateError::IdentityMismatch);
        }
        if patch.base_revision != self.document.revision {
            return Err(AppViewStateError::PatchBaseMismatch);
        }
        if patch.operations.len() > self.limits.maximum_patch_operations {
            return Err(AppViewStateError::ResourceLimit("patch operations"));
        }
        self.ensure_new_revision(&patch.revision)?;
        let mut value = serde_json::to_value(&self.document)
            .map_err(|error| AppViewStateError::InvalidDocument(error.to_string()))?;
        for operation in &patch.operations {
            apply_operation(&mut value, operation)?;
        }
        let object = value.as_object_mut().ok_or_else(|| {
            AppViewStateError::InvalidDocument("document root is not an object".to_owned())
        })?;
        object.insert("revision".to_owned(), Value::String(patch.revision.clone()));
        let document: AppViewDocumentV1 = serde_json::from_value(value)
            .map_err(|error| AppViewStateError::InvalidDocument(error.to_string()))?;
        validate_document(&document, self.limits)?;
        if document.app_id != self.document.app_id || document.view_id != self.document.view_id {
            return Err(AppViewStateError::IdentityMismatch);
        }
        self.document = document;
        self.reconcile_after_document_change();
        Ok(())
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<AppViewInputResult, AppViewStateError> {
        if key.kind != KeyEventKind::Press {
            return Ok(AppViewInputResult::Ignored);
        }
        match key.code {
            KeyCode::Tab => {
                if !self.focus_order.is_empty() {
                    self.focus_index = (self.focus_index + 1) % self.focus_order.len();
                    self.pending_confirmation = None;
                    return Ok(AppViewInputResult::StateChanged);
                }
            }
            KeyCode::BackTab => {
                if !self.focus_order.is_empty() {
                    self.focus_index = self
                        .focus_index
                        .checked_sub(1)
                        .unwrap_or(self.focus_order.len() - 1);
                    self.pending_confirmation = None;
                    return Ok(AppViewInputResult::StateChanged);
                }
            }
            KeyCode::Up | KeyCode::Char('k') => return Ok(self.move_selection(-1)),
            KeyCode::Down | KeyCode::Char('j') => return Ok(self.move_selection(1)),
            KeyCode::PageUp => return Ok(self.move_scroll(-10)),
            KeyCode::PageDown => return Ok(self.move_scroll(10)),
            KeyCode::Home => return Ok(self.set_position(false)),
            KeyCode::End => return Ok(self.set_position(true)),
            KeyCode::Left => return Ok(self.move_selection(-1)),
            KeyCode::Right => return Ok(self.move_selection(1)),
            KeyCode::Esc => {
                if self.pending_confirmation.take().is_some() {
                    return Ok(AppViewInputResult::StateChanged);
                }
            }
            KeyCode::Backspace if self.focused_kind() == Some(AppComponentKindV1::Form) => {
                if let Some(component_id) = self.focused_component_id().map(str::to_owned) {
                    self.form.entry(component_id).or_default().pop();
                    return Ok(AppViewInputResult::StateChanged);
                }
            }
            KeyCode::Char(character) if self.focused_kind() == Some(AppComponentKindV1::Form) => {
                if let Some(component_id) = self.focused_component_id().map(str::to_owned) {
                    let value = self.form.entry(component_id).or_default();
                    if value.len() + character.len_utf8() <= self.limits.maximum_form_bytes {
                        value.push(character);
                        return Ok(AppViewInputResult::StateChanged);
                    }
                    return Err(AppViewStateError::ResourceLimit("form bytes"));
                }
            }
            KeyCode::Enter => return self.activate_focused(),
            _ => {}
        }
        Ok(AppViewInputResult::Ignored)
    }

    fn focused_kind(&self) -> Option<AppComponentKindV1> {
        find_component(&self.document.root, self.focused_component_id()?).map(|item| item.kind)
    }

    fn move_selection(&mut self, delta: isize) -> AppViewInputResult {
        let Some(component_id) = self.focused_component_id().map(str::to_owned) else {
            return AppViewInputResult::Ignored;
        };
        let maximum = find_component(&self.document.root, &component_id)
            .map(|component| {
                if component.kind == AppComponentKindV1::ActionBar {
                    self.document
                        .actions
                        .iter()
                        .filter(|action| action.component_id == component_id && action.enabled)
                        .count()
                } else {
                    component_item_count(component)
                }
            })
            .unwrap_or(0);
        if maximum == 0 {
            return self.move_scroll(delta);
        }
        let selected = self.selection.entry(component_id).or_default();
        *selected = selected
            .saturating_add_signed(delta)
            .min(maximum.saturating_sub(1));
        AppViewInputResult::StateChanged
    }

    fn move_scroll(&mut self, delta: isize) -> AppViewInputResult {
        let Some(component_id) = self.focused_component_id().map(str::to_owned) else {
            return AppViewInputResult::Ignored;
        };
        let scroll = self.scroll.entry(component_id).or_default();
        *scroll = scroll
            .saturating_add_signed(delta)
            .min(self.limits.maximum_scroll);
        AppViewInputResult::StateChanged
    }

    fn set_position(&mut self, end: bool) -> AppViewInputResult {
        let Some(component_id) = self.focused_component_id().map(str::to_owned) else {
            return AppViewInputResult::Ignored;
        };
        let maximum = find_component(&self.document.root, &component_id)
            .map(component_item_count)
            .unwrap_or(0);
        self.selection.insert(
            component_id,
            if end { maximum.saturating_sub(1) } else { 0 },
        );
        AppViewInputResult::StateChanged
    }

    fn activate_focused(&mut self) -> Result<AppViewInputResult, AppViewStateError> {
        let component_id = self
            .focused_component_id()
            .ok_or(AppViewStateError::ActionUnavailable)?
            .to_owned();
        let matching: Vec<_> = self
            .document
            .actions
            .iter()
            .filter(|action| action.component_id == component_id && action.enabled)
            .collect();
        let selected = self
            .selection_for(&component_id)
            .min(matching.len().saturating_sub(1));
        let descriptor = matching
            .get(selected)
            .ok_or(AppViewStateError::ActionUnavailable)?;
        if descriptor.requires_confirmation
            && self.pending_confirmation.as_deref() != Some(&descriptor.action_id)
        {
            self.pending_confirmation = Some(descriptor.action_id.clone());
            return Ok(AppViewInputResult::ConfirmationRequired {
                action_id: descriptor.action_id.clone(),
            });
        }
        let confirmed = descriptor.requires_confirmation;
        self.pending_confirmation = None;
        let selection = Value::Object(
            self.selection
                .iter()
                .map(|(key, value)| (key.clone(), json!(value)))
                .collect::<Map<_, _>>(),
        );
        let form = Value::Object(
            self.form
                .iter()
                .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                .collect::<Map<_, _>>(),
        );
        let action = AppActionV1 {
            schema_version: 1,
            app_id: self.document.app_id.clone(),
            view_id: self.document.view_id.clone(),
            document_revision: self.document.revision.clone(),
            component_id,
            action_id: descriptor.action_id.clone(),
            selection,
            form,
            confirmed,
        };
        action
            .validate()
            .map_err(|error| AppViewStateError::InvalidDocument(error.to_string()))?;
        Ok(AppViewInputResult::Action(action))
    }

    fn ensure_new_revision(&self, revision: &str) -> Result<(), AppViewStateError> {
        if revision == self.document.revision
            || self.recent_revisions.iter().any(|item| item == revision)
        {
            return Err(AppViewStateError::StaleRevision);
        }
        if let (Ok(current), Ok(next)) = (
            self.document.revision.parse::<u128>(),
            revision.parse::<u128>(),
        ) {
            if next <= current {
                return Err(AppViewStateError::StaleRevision);
            }
        }
        Ok(())
    }

    fn reconcile_after_document_change(&mut self) {
        self.recent_revisions.push(self.document.revision.clone());
        if self.recent_revisions.len() > self.limits.maximum_recent_revisions {
            let excess = self.recent_revisions.len() - self.limits.maximum_recent_revisions;
            self.recent_revisions.drain(0..excess);
        }
        let old_focus = self.focused_component_id().map(str::to_owned);
        self.focus_order = focus_order(&self.document.root);
        self.focus_index = old_focus
            .and_then(|id| {
                self.focus_order
                    .iter()
                    .position(|candidate| candidate == &id)
            })
            .or_else(|| {
                self.document.focus_component_id.as_ref().and_then(|id| {
                    self.focus_order
                        .iter()
                        .position(|candidate| candidate == id)
                })
            })
            .unwrap_or(0);
        let valid: BTreeSet<_> = self.focus_order.iter().cloned().collect();
        self.selection.retain(|id, _| valid.contains(id));
        self.scroll.retain(|id, _| valid.contains(id));
        self.form.retain(|id, _| valid.contains(id));
        self.pending_confirmation = None;
    }
}

fn validate_document(
    document: &AppViewDocumentV1,
    limits: AppViewStateLimits,
) -> Result<(), AppViewStateError> {
    document
        .validate()
        .map_err(|error| AppViewStateError::InvalidDocument(error.to_string()))?;
    let encoded = serde_json::to_vec(document)
        .map_err(|error| AppViewStateError::InvalidDocument(error.to_string()))?;
    if encoded.len() > limits.maximum_document_bytes {
        return Err(AppViewStateError::ResourceLimit("document bytes"));
    }
    if document.actions.len() > limits.maximum_actions {
        return Err(AppViewStateError::ResourceLimit("actions"));
    }
    if document.bindings.len() > limits.maximum_bindings {
        return Err(AppViewStateError::ResourceLimit("bindings"));
    }
    let mut component_ids = BTreeSet::new();
    let count = collect_component_ids(&document.root, &mut component_ids)?;
    if count > limits.maximum_components {
        return Err(AppViewStateError::ResourceLimit("components"));
    }
    let mut action_ids = BTreeSet::new();
    for action in &document.actions {
        if !component_ids.contains(&action.component_id) {
            return Err(AppViewStateError::InvalidDocument(format!(
                "action {} targets an unknown component",
                action.action_id
            )));
        }
        if !action_ids.insert(action.action_id.clone()) {
            return Err(AppViewStateError::InvalidDocument(format!(
                "duplicate action id {}",
                action.action_id
            )));
        }
    }
    let mut subscription_ids = BTreeSet::new();
    if document.subscriptions.len() > 256 {
        return Err(AppViewStateError::ResourceLimit("subscriptions"));
    }
    for subscription in &document.subscriptions {
        if subscription.subscription_id.is_empty() || subscription.subscription_id.len() > 256 {
            return Err(AppViewStateError::InvalidDocument(
                "subscription id must contain 1..=256 bytes".to_owned(),
            ));
        }
        if !subscription_ids.insert(subscription.subscription_id.clone()) {
            return Err(AppViewStateError::InvalidDocument(format!(
                "duplicate subscription id {}",
                subscription.subscription_id
            )));
        }
    }
    Ok(())
}

fn collect_component_ids(
    component: &AppComponentV1,
    ids: &mut BTreeSet<String>,
) -> Result<usize, AppViewStateError> {
    if !ids.insert(component.component_id.clone()) {
        return Err(AppViewStateError::InvalidDocument(format!(
            "duplicate component id {}",
            component.component_id
        )));
    }
    let mut count = 1usize;
    for child in &component.children {
        count = count.saturating_add(collect_component_ids(child, ids)?);
    }
    Ok(count)
}

fn focus_order(root: &AppComponentV1) -> Vec<String> {
    fn visit(component: &AppComponentV1, output: &mut Vec<String>) {
        output.push(component.component_id.clone());
        for child in &component.children {
            visit(child, output);
        }
    }
    let mut output = Vec::new();
    visit(root, &mut output);
    output
}

fn find_component<'a>(component: &'a AppComponentV1, id: &str) -> Option<&'a AppComponentV1> {
    if component.component_id == id {
        return Some(component);
    }
    component
        .children
        .iter()
        .find_map(|child| find_component(child, id))
}

fn component_item_count(component: &AppComponentV1) -> usize {
    for key in ["rows", "items", "nodes", "events", "tabs", "fields"] {
        if let Some(length) = component
            .properties
            .get(key)
            .and_then(Value::as_array)
            .map(Vec::len)
        {
            return length;
        }
    }
    component.children.len()
}

fn decode_pointer(path: &str) -> Result<Vec<String>, AppViewStateError> {
    if !path.starts_with('/') || path == "/" || path.len() > 4_096 {
        return Err(AppViewStateError::InvalidPatchPath(path.to_owned()));
    }
    let tokens: Vec<String> = path[1..]
        .split('/')
        .map(|token| token.replace("~1", "/").replace("~0", "~"))
        .collect();
    if tokens.is_empty()
        || !matches!(
            tokens[0].as_str(),
            "title"
                | "root"
                | "bindings"
                | "actions"
                | "subscriptions"
                | "focus_component_id"
                | "refresh_policy"
        )
    {
        return Err(AppViewStateError::InvalidPatchPath(path.to_owned()));
    }
    Ok(tokens)
}

fn parent_mut<'a>(
    root: &'a mut Value,
    tokens: &[String],
    path: &str,
) -> Result<(&'a mut Value, String), AppViewStateError> {
    let (last, parents) = tokens
        .split_last()
        .ok_or_else(|| AppViewStateError::InvalidPatchPath(path.to_owned()))?;
    let mut current = root;
    for token in parents {
        current = match current {
            Value::Object(object) => object.get_mut(token),
            Value::Array(array) => token
                .parse::<usize>()
                .ok()
                .and_then(|index| array.get_mut(index)),
            _ => None,
        }
        .ok_or_else(|| AppViewStateError::InvalidPatchPath(path.to_owned()))?;
    }
    Ok((current, last.clone()))
}

fn apply_operation(
    document: &mut Value,
    operation: &AppViewPatchOperationV1,
) -> Result<(), AppViewStateError> {
    let (path, value, mode) = match operation {
        AppViewPatchOperationV1::Replace { path, value } => (path, Some(value.clone()), 0u8),
        AppViewPatchOperationV1::Add { path, value } => (path, Some(value.clone()), 1u8),
        AppViewPatchOperationV1::Remove { path } => (path, None, 2u8),
    };
    let tokens = decode_pointer(path)?;
    let (parent, key) = parent_mut(document, &tokens, path)?;
    match (parent, mode) {
        (Value::Object(object), 0) if object.contains_key(&key) => {
            object.insert(
                key,
                value.ok_or_else(|| AppViewStateError::InvalidPatchPath(path.to_owned()))?,
            );
        }
        (Value::Object(object), 1) if !object.contains_key(&key) => {
            object.insert(
                key,
                value.ok_or_else(|| AppViewStateError::InvalidPatchPath(path.to_owned()))?,
            );
        }
        (Value::Object(object), 2) => {
            if object.remove(&key).is_none() {
                return Err(AppViewStateError::InvalidPatchPath(path.to_owned()));
            }
        }
        (Value::Array(array), 0) => {
            let index = key
                .parse::<usize>()
                .map_err(|_| AppViewStateError::InvalidPatchPath(path.to_owned()))?;
            let slot = array
                .get_mut(index)
                .ok_or_else(|| AppViewStateError::InvalidPatchPath(path.to_owned()))?;
            *slot = value.ok_or_else(|| AppViewStateError::InvalidPatchPath(path.to_owned()))?;
        }
        (Value::Array(array), 1) => {
            let index = if key == "-" {
                array.len()
            } else {
                key.parse::<usize>()
                    .map_err(|_| AppViewStateError::InvalidPatchPath(path.to_owned()))?
            };
            if index > array.len() {
                return Err(AppViewStateError::InvalidPatchPath(path.to_owned()));
            }
            array.insert(
                index,
                value.ok_or_else(|| AppViewStateError::InvalidPatchPath(path.to_owned()))?,
            );
        }
        (Value::Array(array), 2) => {
            let index = key
                .parse::<usize>()
                .map_err(|_| AppViewStateError::InvalidPatchPath(path.to_owned()))?;
            if index >= array.len() {
                return Err(AppViewStateError::InvalidPatchPath(path.to_owned()));
            }
            array.remove(index);
        }
        _ => return Err(AppViewStateError::InvalidPatchPath(path.to_owned())),
    }
    Ok(())
}
