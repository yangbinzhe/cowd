use std::collections::BTreeSet;

use surface::gateway_api::gateway_routes;

#[test]
fn surface_catalog_gateway_bindings_and_openapi_are_identical() {
    let surface = gateway_routes()
        .iter()
        .map(|route| {
            (
                route.method().as_str().to_owned(),
                route.path().template().to_owned(),
            )
        })
        .collect::<BTreeSet<_>>();
    let (bindings, openapi) = gateway::test_support::route_contract_snapshots();
    let openapi_expected = surface
        .iter()
        .map(|(method, path)| (method.clone(), path.replace('*', ":")))
        .collect::<BTreeSet<_>>();

    assert_eq!(surface.len(), 482, "real public route surface regressed");
    assert_eq!(bindings, surface, "Gateway handler binding drift");
    assert_eq!(openapi, openapi_expected, "OpenAPI projection drift");
}
