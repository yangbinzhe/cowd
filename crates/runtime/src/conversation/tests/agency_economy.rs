use crate::SystemPromptBuilder;

fn r1_assert_no_absolute_economy(output: &str) {
    for fragments in [
        ["Never re-read a file", " an accepted Task already read"],
        ["Batch every independent", " read/search"],
        ["Decide the whole answer", " before the first call"],
        ["every call you can issue", " in this response, issue now"],
        [
            "every re-read of already-read content",
            " is avoidable spend",
        ],
        ["Batch independent work", " into one round"],
        ["Batch independent work", " into ONE response"],
        ["instead of re-reading", " source files"],
        ["scope each Task", " to a batch of related reads"],
        ["issue independent calls", " in the same response"],
        ["Batch independent tool calls", " into one turn"],
        ["batch every call", " you can issue now"],
        [
            "Do not repeatedly read overlapping ranges",
            " of the same small file.",
        ],
        [
            "Do not use execute_code to slice a text file",
            " when read_file or grep_search can gather the evidence.",
        ],
    ] {
        let forbidden = fragments.concat();
        assert!(
            !output.contains(&forbidden),
            "absolute economy directive: {forbidden}"
        );
    }
}




#[tokio::test]
async fn r1_agency_economy_absolute_protocol_is_absent() {
    let system = SystemPromptBuilder::new().build();
    let system_text = system.join("\n");
    assert!(system_text.contains("You own semantic decisions"));
    assert!(system_text.contains("authorization, revisions, idempotency, leases"));
    assert!(system_text.contains("a different Agent accepts it"));
    assert!(system_text.contains("Prefer parallelizing independent tool calls."));
    assert!(system_text.contains("read only the range you need"));
    let callable = ["read_file", "tool_orchestrate", "execute_code"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let guidance = crate::capability_manifest::render_tool_guidance(&callable);
    assert!(guidance.contains("`read_file`"));
    assert!(guidance.contains("`tool_orchestrate`"));
    assert!(guidance.contains("`batch`"));
    assert!(guidance.contains("`compute`"));
    assert!(guidance.contains("`execute_code`"));
    r1_assert_no_absolute_economy(&system_text);
    use crate::evidence_planner::EvidenceAcquisitionMode;
    for (objective, mode) in [
        ("Review README.md", EvidenceAcquisitionMode::SmallEvidence),
        (
            "Summarize the supplied observation",
            EvidenceAcquisitionMode::MediumEvidence,
        ),
        (
            "Inspect the entire workspace",
            EvidenceAcquisitionMode::LargeEvidence,
        ),
        (
            "Review the architecture across runtime and gateway",
            EvidenceAcquisitionMode::ComplexEvidence,
        ),
    ] {
        let plan = crate::evidence_planner::plan_evidence(objective);
        assert_eq!(plan.mode, mode);
        let rendered = crate::evidence_planner::evidence_plan_prompt(&plan);
        r1_assert_no_absolute_economy(&rendered);
    }
    for objective in [
        "Read the supplied source file",
        "Explain the supplied observation",
    ] {
        let decision = crate::execution_core::build_runtime_execution_decision(objective, None);
        for compact in [false, true] {
            let guidance = crate::execution_core::model_affordance::runtime_execution_guidance_prompt_with_tool_exposure_mode(
                &decision, None, compact,
            );
            r1_assert_no_absolute_economy(&guidance);
        }
    }

}
