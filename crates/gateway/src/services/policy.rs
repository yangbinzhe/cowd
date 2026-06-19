#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServicePolicy {
    pub(crate) owner: &'static str,
    pub(crate) boundary_status: &'static str,
}

impl ServicePolicy {
    pub(crate) const fn final_boundary(owner: &'static str) -> Self {
        Self {
            owner,
            boundary_status: "0618_final_boundary",
        }
    }
}
