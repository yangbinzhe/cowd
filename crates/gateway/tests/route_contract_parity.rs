use std::collections::BTreeSet;

use surface::gateway_api::gateway_routes;

#[test]
fn r1_message_ack_route_contract_has_two_mounted_read_authorized_handlers() {
    let expected = [
        (
            "GET",
            "/api/sessions/:id/messages/:message_id/receipt-context",
        ),
        ("POST", "/api/sessions/:id/messages/:message_id/ack"),
    ];
    let (bindings, openapi) = gateway::test_support::route_contract_snapshots();
    for (method, path) in expected {
        assert!(bindings.contains(&(method.into(), path.into())));
        assert!(openapi.contains(&(method.into(), path.into())));
    }
    let source = include_str!("../src/api_routes/message_routes.rs");
    let source = source.chars().filter(|character| !character.is_whitespace()).collect::<String>();
    assert!(source.contains("RECEIPT_CONTEXT.template(),get(get_terminal_message_receipt_context)"));
    assert!(source.contains("ACK.template(),post(record_terminal_message_ack)"));
    let contract = include_str!("../src/api_routes/capability_contract.rs");
    for schema in [
        "SessionMessageReceiptContext",
        "SessionMessageAckRequest",
        "SessionMessageAck",
    ] {
        assert!(contract.contains(schema));
    }
}

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

    assert_eq!(bindings, surface, "Gateway handler binding drift");
    assert_eq!(openapi, openapi_expected, "OpenAPI projection drift");
}
