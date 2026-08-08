//! Task aggregate invariants shared by stores and application services.

use harness_contract::task::{TaskAggregate, TaskCreateCommand, TaskTurnBinding};

pub fn validate_create_command(command: &TaskCreateCommand) -> Result<(), String> {
    command.validate()
}

pub fn validate_aggregate(aggregate: &TaskAggregate) -> Result<(), String> {
    aggregate.validate()
}

pub fn validate_binding(binding: &TaskTurnBinding) -> Result<(), String> {
    binding.validate().map_err(str::to_string)
}
