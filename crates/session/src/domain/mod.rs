mod event;
mod lifecycle_operation;

pub use event::{
    SessionDomainEvent, SessionDomainEventPage, SessionDomainRef, SessionDomainScope,
    SESSION_DOMAIN_EVENT_TYPE,
};
pub use lifecycle_operation::{
    SessionBranchActivation, SessionBranchActivationPhase, SessionBranchActivationTransition,
    SessionCloseDisposition, SessionLifecycleIntent, SessionLifecyclePhase, SessionLifecyclePlan,
    SessionLifecycleTransition,
};
