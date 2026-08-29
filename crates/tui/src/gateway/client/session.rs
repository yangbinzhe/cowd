use surface::gateway_api::GatewayPathKey;

use super::transport::render_route;

pub(crate) fn for_session(path: GatewayPathKey, encoded_session_id: String) -> String {
    render_route(path, &[encoded_session_id])
}
