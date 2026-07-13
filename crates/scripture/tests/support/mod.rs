//! Test-only support shared by real-actor integration tests.

#![allow(dead_code, unused_imports)]

mod fixtures;
mod scripted_log_drive;

pub(crate) use fixtures::{
    address, cohort, hostile_policy, journal, policy, producer, record, tiny_policy, writer_id,
};
pub(crate) use scripted_log_drive::{PollGate, ScriptedLogDrive};
