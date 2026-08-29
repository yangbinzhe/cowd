use surface::gateway_api::GatewayPathKey;

use super::transport::render_route;

pub(crate) fn for_platform_entity(path: GatewayPathKey, encoded_id: String) -> String {
    render_route(path, &[encoded_id])
}
