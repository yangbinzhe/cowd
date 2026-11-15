// Keep the pre-existing cross-process PostgreSQL control when the business
// recovery tests move to their original Gateway owner.
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
#[path = "support/own823n_historical_issuer_restart.rs"]
mod own823n_historical_issuer_restart;
