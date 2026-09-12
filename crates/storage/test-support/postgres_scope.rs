//! Shared test-only namespace owner. Include with #[path] from PG fixtures.
//! No production connection behavior or process-wide search path is changed.
use std::sync::atomic::{AtomicU64, Ordering};
use storage::{PostgresConnectionConfig, PostgresExecutor, StaticSecretRefResolver};

pub struct PostgresTestScope {
    base: PostgresExecutor,
    config: PostgresConnectionConfig,
    resolver: StaticSecretRefResolver,
    schema: String,
}

impl PostgresTestScope {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let schema = format!(
            "cowdtest_{}_{}_{}",
            std::process::id(),
            stamp,
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("isolated test PostgreSQL required");
        let resolver = StaticSecretRefResolver::new([("fixture.pg".into(), url)]);
        let config = PostgresConnectionConfig::new(&schema, "fixture.pg", "cowd-owned-test-schema");
        let base = PostgresExecutor::connect(config.clone(), &resolver).expect("test connection");
        base.checkout_critical()
            .expect("namespace connection")
            .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
            .expect("create a new owned namespace");
        eprintln!("postgres_fixture created {}", schema);
        Self {
            base,
            config,
            resolver,
            schema,
        }
    }

    /// A genuinely new pool/executor for adapter restart and migration tests.
    pub fn reconnect(&self) -> PostgresExecutor {
        let executor = PostgresExecutor::connect(self.config.clone(), &self.resolver)
            .expect("reconnect fixture pool");
        self.bind(&executor)
    }

    pub fn bind(&self, base: &PostgresExecutor) -> PostgresExecutor {
        let executor = base
            .scoped_namespace(&self.schema)
            .expect("explicit owned namespace");
        let actual: String = executor
            .checkout_critical()
            .expect("scope check")
            .query_one("SELECT current_schema()", &[])
            .expect("current namespace")
            .get(0);
        assert_eq!(actual, self.schema);
        executor
    }
}

impl Drop for PostgresTestScope {
    fn drop(&mut self) {
        let result = self
            .base
            .checkout_critical()
            .map_err(|error| error.to_string())
            .and_then(|mut connection| {
                connection
                    .batch_execute(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
                    .map_err(|error| error.to_string())
            });
        if result.is_ok() {
            eprintln!("postgres_fixture dropped {}", self.schema);
        }
        if let Err(error) = result {
            if std::thread::panicking() {
                eprintln!("owned PostgreSQL fixture cleanup failed: {error}");
            } else {
                panic!("owned PostgreSQL fixture cleanup failed: {error}");
            }
        }
    }
}
