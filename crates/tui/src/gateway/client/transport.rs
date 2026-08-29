use surface::gateway_api::GatewayPathKey;

/// Render a canonical path using positional values in template order.
///
/// The catalog owns parameter names; callers only provide already encoded
/// values. A mismatch is a programming error caught by route parity tests.
pub(crate) fn render_route(path: GatewayPathKey, values: &[String]) -> String {
    let names = parameter_names(path.template());
    assert_eq!(
        names.len(),
        values.len(),
        "typed Gateway route `{}` expects {} parameters, got {}",
        path.id(),
        names.len(),
        values.len()
    );
    let parameters = names
        .iter()
        .zip(values)
        .map(|(name, value)| (*name, value.as_str()))
        .collect::<Vec<_>>();
    path.render(&parameters)
        .expect("catalog-derived route parameters must render")
}

pub(crate) fn route_with_query(path: GatewayPathKey, values: &[String], query: &str) -> String {
    let mut rendered = if values.is_empty() {
        path.template().to_owned()
    } else {
        render_route(path, values)
    };
    if !query.is_empty() {
        rendered.push('?');
        rendered.push_str(query.strip_prefix('?').unwrap_or(query));
    }
    rendered
}

fn parameter_names(template: &str) -> Vec<&str> {
    template
        .split('/')
        .filter_map(|segment| {
            segment
                .strip_prefix(':')
                .or_else(|| segment.strip_prefix('*'))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use surface::gateway_api::paths;

    use super::*;

    #[test]
    fn renders_catalog_path_and_query() {
        assert_eq!(
            route_with_query(
                paths::API_SESSIONS_BY_ID_MESSAGES,
                &["session%201".to_owned()],
                "offset=0&limit=20",
            ),
            "/api/sessions/session%201/messages?offset=0&limit=20"
        );
    }
}
