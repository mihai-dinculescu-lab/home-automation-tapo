use std::fmt;
use std::time::Duration;

use bluer::agent::{Agent, ReqError};
use bluer::gatt::WriteOp;
use bluer::gatt::remote::{Characteristic, CharacteristicWriteRequest};
use bluer::{
    Adapter, AdapterEvent, Address, Device, DiscoveryFilter, DiscoveryTransport, InvalidAddress,
    Session, Uuid,
};
use futures_util::{StreamExt, pin_mut};
use tokio::time::{Instant, sleep, timeout, timeout_at};
use tracing::{debug, warn};

use crate::system::dimplex::{Outcome, protocol};

/// The service of the heater that holds its mode.
const MODE_SERVICE: Uuid = Uuid::from_u128(0x0000_0000_0000_1000_8000_0080_5f9b_34fb);

/// The mode: 6 bytes, byte 0 the mode and byte 4 the setpoint in °C.
const MODE_CHARACTERISTIC: Uuid = Uuid::from_u128(0x0000_1001_0000_1000_8000_0080_5f9b_34fb);

/// Bound on the whole attempt: reaching BlueZ, discovery and retries included. Under the
/// shortest time between two thermostat ticks even with `DISCONNECT_TIMEOUT` on top, so that an
/// attempt has usually ended by the time the next tick asks for one; one that has not is about
/// to fail, and the tick after asks again. bluer's own bound on a request is two minutes.
const DEADLINE: Duration = Duration::from_secs(45);

/// Bound on letting go of the link once the attempt has ended, kept apart from `DEADLINE` so
/// that a heater set just before it is not reported as a failure.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How often an attempt that failed over Bluetooth is made again within the deadline. BlueZ
/// often aborts the first connection or two within a second, and a heater just powered on takes
/// a few seconds to advertise.
const RETRIES: u32 = 5;

const RETRY_DELAY: Duration = Duration::from_secs(2);

/// Why the heater could not be put in Manual.
#[derive(Debug)]
pub enum Error {
    /// `bluetooth_address` in the settings is not a Bluetooth address.
    InvalidAddress(InvalidAddress),
    /// BlueZ could not be reached or failed a request: the adapter, the connection, the pairing,
    /// the read or the write.
    Bluetooth(bluer::Error),
    /// The deadline passed before BlueZ answered, or with the heater known but not yet set, e.g.
    /// one held by the Remo app.
    Timeout,
    /// The heater did not advertise before the deadline, so BlueZ has no device to connect to:
    /// it is unpowered or out of range, or held by another connection.
    NotFound,
    /// The heater has no mode characteristic, so it is not the model this was written for.
    NoModeCharacteristic,
    /// The heater took the write but still reads back as another mode or setpoint, which is
    /// carried here.
    NotApplied(Vec<u8>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAddress(e) => e.fmt(f),
            Self::Bluetooth(e) => e.fmt(f),
            Self::Timeout => write!(
                f,
                "The heater was not put in Manual within {} seconds",
                DEADLINE.as_secs()
            ),
            Self::NotFound => f.write_str("The heater was not found over Bluetooth"),
            Self::NoModeCharacteristic => f.write_str("The heater has no mode characteristic"),
            Self::NotApplied(frame) => write!(
                f,
                "The heater does not read back as Manual at the setpoint, but as {frame:02x?}"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidAddress(e) => e.source(),
            Self::Bluetooth(e) => e.source(),
            Self::Timeout | Self::NotFound | Self::NoModeCharacteristic | Self::NotApplied(_) => {
                None
            }
        }
    }
}

impl From<bluer::Error> for Error {
    fn from(e: bluer::Error) -> Self {
        Self::Bluetooth(e)
    }
}

/// Makes sure the heater at `bluetooth_address` is in Manual at `temperature` °C: it is only
/// written to when it reads as anything else. A host that has not bonded with it yet does so on
/// the way, so that the service works on whichever node it runs on.
///
/// The link is let go of however the attempt ends, since the heater takes one connection at a
/// time and the Remo app needs it too. An attempt that is dropped half way leaves it to BlueZ.
///
/// The deadline covers everything but that disconnect, which has a short bound of its own.
pub async fn ensure_manual(bluetooth_address: &str, temperature: u8) -> Result<Outcome, Error> {
    let address: Address = bluetooth_address.parse().map_err(Error::InvalidAddress)?;

    let deadline = Instant::now() + DEADLINE;

    let setup = async {
        let session = Session::new().await?;
        let adapter = session.default_adapter().await?;
        let agent = session.register_agent(passkey_agent(address)).await?;

        Ok::<_, Error>((session, adapter, agent))
    };

    // The agent is unregistered when its handle is dropped.
    let (_session, adapter, _agent) = timeout_at(deadline, setup)
        .await
        .map_err(|_| Error::Timeout)??;

    // Only a handle: it asks nothing of BlueZ, which may not know the heater yet.
    let device = adapter.device(address)?;

    let result = async {
        timeout_at(deadline, discover_if_unknown(&adapter, address))
            .await
            .map_err(|_| Error::NotFound)??;

        timeout_at(deadline, ensure_manual_with_retries(&device, temperature))
            .await
            .map_err(|_| Error::Timeout)?
    }
    .await;

    // Whether it ended by itself or was dropped at the deadline, the link may still be up.
    if let Ok(Err(e)) = timeout(DISCONNECT_TIMEOUT, device.disconnect()).await {
        debug!("Could not disconnect from the heater: {e}");
    }

    result
}

/// An agent that answers the passkey request of the heater at `address` and of no other device.
///
/// The host's default while it is registered: the pairing is started by the heater refusing a
/// read rather than by a request of this service, and BlueZ asks the default agent then.
fn passkey_agent(address: Address) -> Agent {
    let passkey = protocol::passkey(*address);

    Agent {
        request_default: true,
        request_passkey: Some(Box::new(move |request| {
            Box::pin(async move {
                if request.device == address {
                    debug!("Answering the heater's passkey request");
                    Ok(passkey)
                } else {
                    Err(ReqError::Rejected)
                }
            })
        })),
        ..Default::default()
    }
}

/// Makes the heater known to BlueZ. One it has bonded with is known for good; any other has to
/// be seen advertising first, so it is discovered until it shows up, which the caller bounds.
async fn discover_if_unknown(adapter: &Adapter, address: Address) -> Result<(), Error> {
    if !adapter.device_addresses().await?.contains(&address) {
        debug!("The heater is not known to BlueZ yet, discovering it");

        adapter
            .set_discovery_filter(DiscoveryFilter {
                transport: DiscoveryTransport::Le,
                ..Default::default()
            })
            .await?;

        // Discovery stops when the stream is dropped, before the connection is made.
        let events = adapter.discover_devices().await?;
        pin_mut!(events);

        loop {
            match events.next().await {
                Some(AdapterEvent::DeviceAdded(found)) if found == address => break,
                Some(_) => {}
                None => return Err(Error::NotFound),
            }
        }
    }

    Ok(())
}

/// Tries again when Bluetooth fails; anything else would fail the same way again.
async fn ensure_manual_with_retries(device: &Device, temperature: u8) -> Result<Outcome, Error> {
    let mut retries = RETRIES;
    // Kept across the retries: a write whose response or read-back was lost to the link is
    // found already set by the next try, which would otherwise report that nothing was written.
    let mut written = false;

    loop {
        match ensure_manual_once(device, temperature, &mut written).await {
            Err(Error::Bluetooth(e)) if retries > 0 => {
                retries -= 1;
                warn!("The heater could not be reached, trying again: {e}");

                // A link left half up would fail the next connection too.
                let _ = device.disconnect().await;
                sleep(RETRY_DELAY).await;
            }
            result => return result,
        }
    }
}

/// One try. `written` is set once a write has been sent, in this try or an earlier one.
async fn ensure_manual_once(
    device: &Device,
    temperature: u8,
    written: &mut bool,
) -> Result<Outcome, Error> {
    if !device.is_connected().await? {
        debug!("Connecting to the heater");
        device.connect().await?;
    }

    debug!("Looking up the mode characteristic");
    let mode = mode_characteristic(device).await?;

    // Every characteristic needs an authenticated link, so on a host that has not bonded with
    // the heater yet this read makes it ask for the passkey, which the agent answers; its
    // pairing button is not needed. The pairing is left to the read rather than asked for
    // first: `Device::pair` did not return here although BlueZ had bonded.
    let frame = mode.read().await?;
    debug!("The heater's mode reads {frame:02x?}");

    if protocol::is_manual_at(&frame, temperature) {
        return Ok(if *written {
            Outcome::Written
        } else {
            Outcome::AlreadySet
        });
    }

    // With a response: the heater ignores a write without one, which is bluer's default.
    let request = CharacteristicWriteRequest {
        op_type: WriteOp::Request,
        ..Default::default()
    };
    // Before the write: the heater may take it and the link drop before it answers.
    *written = true;
    mode.write_ext(&protocol::manual_frame(temperature), &request)
        .await?;

    let frame = mode.read().await?;
    debug!("After the write the heater's mode reads {frame:02x?}");

    if protocol::is_manual_at(&frame, temperature) {
        Ok(Outcome::Written)
    } else {
        Err(Error::NotApplied(frame))
    }
}

async fn mode_characteristic(device: &Device) -> Result<Characteristic, Error> {
    for service in device.services().await? {
        if service.uuid().await? != MODE_SERVICE {
            continue;
        }

        for characteristic in service.characteristics().await? {
            if characteristic.uuid().await? == MODE_CHARACTERISTIC {
                return Ok(characteristic);
            }
        }
    }

    Err(Error::NoModeCharacteristic)
}
