use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum GatewayHttpMethod {
    Delete,
    Get,
    Patch,
    Post,
    Put,
}

impl GatewayHttpMethod {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Get => "GET",
            Self::Patch => "PATCH",
            Self::Post => "POST",
            Self::Put => "PUT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct GatewayPathKey {
    id: &'static str,
    template: &'static str,
}

impl GatewayPathKey {
    #[must_use]
    pub const fn new(id: &'static str, template: &'static str) -> Self {
        Self { id, template }
    }

    #[must_use]
    pub const fn id(self) -> &'static str {
        self.id
    }

    #[must_use]
    pub const fn template(self) -> &'static str {
        self.template
    }

    /// Render an Axum path template using already percent-encoded values.
    /// Every declared parameter must be supplied exactly once and unknown
    /// parameters are rejected, preventing clients from silently drifting.
    pub fn render(self, parameters: &[(&str, &str)]) -> Result<String, RouteRenderError> {
        let mut rendered = String::with_capacity(self.template.len() + 32);
        let mut remaining = self.template;
        while let Some(marker) = remaining.find([':', '*']) {
            rendered.push_str(&remaining[..marker]);
            let tail = &remaining[marker + 1..];
            let end = tail.find('/').unwrap_or(tail.len());
            let name = &tail[..end];
            let value = parameters
                .iter()
                .find_map(|(candidate, value)| (*candidate == name).then_some(*value))
                .ok_or_else(|| RouteRenderError::MissingParameter(name.to_owned()))?;
            rendered.push_str(value);
            remaining = &tail[end..];
        }
        rendered.push_str(remaining);
        for (name, _) in parameters {
            let colon = format!(":{name}");
            let wildcard = format!("*{name}");
            if !self.template.contains(&colon) && !self.template.contains(&wildcard) {
                return Err(RouteRenderError::UnknownParameter((*name).to_owned()));
            }
        }
        Ok(rendered)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct GatewayRouteSpec {
    id: &'static str,
    method: GatewayHttpMethod,
    path: GatewayPathKey,
}

impl GatewayRouteSpec {
    #[must_use]
    pub const fn new(id: &'static str, method: GatewayHttpMethod, path: GatewayPathKey) -> Self {
        Self { id, method, path }
    }

    #[must_use]
    pub const fn id(self) -> &'static str {
        self.id
    }

    #[must_use]
    pub const fn method(self) -> GatewayHttpMethod {
        self.method
    }

    #[must_use]
    pub const fn path(self) -> GatewayPathKey {
        self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteRenderError {
    MissingParameter(String),
    UnknownParameter(String),
}

impl fmt::Display for RouteRenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingParameter(name) => write!(formatter, "missing route parameter `{name}`"),
            Self::UnknownParameter(name) => write!(formatter, "unknown route parameter `{name}`"),
        }
    }
}

impl std::error::Error for RouteRenderError {}

#[cfg(test)]
mod tests {
    use super::GatewayPathKey;

    #[test]
    fn renderer_requires_exact_template_parameters() {
        let route = GatewayPathKey::new("session_turn", "/sessions/:id/turns/:turn_id");
        assert_eq!(
            route.render(&[("id", "a"), ("turn_id", "b")]).unwrap(),
            "/sessions/a/turns/b"
        );
        assert!(route.render(&[("id", "a")]).is_err());
        assert!(route
            .render(&[("id", "a"), ("turn_id", "b"), ("extra", "c")])
            .is_err());
    }
}
