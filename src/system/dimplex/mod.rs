//! The follow-up for a Dimplex heater of the Remo app, which starts in Standby whenever its plug
//! powers it on: it is put in Manual over Bluetooth LE.

// BlueZ only exists on Linux. Elsewhere every attempt fails, so that the service still builds and
// its tests still run.
#[cfg(target_os = "linux")]
mod bluetooth;
#[cfg(not(target_os = "linux"))]
#[path = "bluetooth_unsupported.rs"]
mod bluetooth;
pub mod dimplex_actor;
pub mod protocol;

use derive_more::Display;

/// How an attempt that reached the heater ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum Outcome {
    /// The heater was already in Manual at the setpoint, so nothing was written.
    AlreadySet,
    /// The heater was put in Manual at the setpoint, and read back as such.
    Written,
}
