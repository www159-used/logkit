#![recursion_limit = "256"]

include!(concat!(env!("OUT_DIR"), "/logen.v1.rs"));

pub mod version_support;

pub mod agent {
    include!(concat!(env!("OUT_DIR"), "/logen.agent.v1.rs"));
}

pub use agent::EventInfo;
