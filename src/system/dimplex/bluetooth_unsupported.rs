use derive_more::Display;

use crate::system::dimplex::Outcome;

/// The heater is only reached through BlueZ, which this platform does not have.
#[derive(Debug, Display)]
#[display("The heater follow-up needs BlueZ, which only runs on Linux")]
pub struct Error;

impl std::error::Error for Error {}

/// Fails: see `Error`.
pub async fn ensure_manual(_bluetooth_address: &str, _temperature: u8) -> Result<Outcome, Error> {
    Err(Error)
}
