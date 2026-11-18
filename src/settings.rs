use std::collections::HashSet;
use std::time::Duration;

use anyhow::ensure;
use chrono::{DateTime, NaiveTime, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tapo::ApiClient;

/// The longest window a camera's detection search accepts. Kept for every presence source, so
/// that there is one rule rather than one per device.
const MAX_PRESENCE_WINDOW_S: u64 = 24 * 60 * 60;

/// A sensor silent for a day is broken, not slow. The bound also keeps the age arithmetic in the
/// decision well inside the range `chrono::Duration` accepts.
const MAX_READING_MAX_AGE_S: u64 = 24 * 60 * 60;

/// About the longest an evaluation with a slow plug takes. A reading evaluated sooner after the
/// last evaluation would be likely to cancel its switch; one evaluated later still can, if the
/// evaluation runs long.
const MIN_MIN_INTERVAL_S: u64 = 60;

/// Bound on each request to a device. The devices are on the local network, so a request that
/// takes longer than this has failed; tapo's default of 30 seconds would let a single unreachable
/// device hold up a thermostat tick or the shutdown for far longer than either can afford.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

// Unknown keys are rejected on every section, so that a settings file in the shape of an earlier
// release fails at startup instead of being read with a feature silently dropped.

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Api {
    pub host: String,
    pub port: u16,
}

/// The credentials of every Tapo device: the API, the usage polling and the heating.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tapo {
    pub username: String,
    pub password: String,
}

impl Tapo {
    /// A client for these credentials with the request timeout applied. Every device call goes
    /// through here so that the timeout is set in one place.
    pub fn client(&self) -> ApiClient {
        ApiClient::new(self.username.clone(), self.password.clone()).with_timeout(REQUEST_TIMEOUT)
    }
}

/// The broker. Each feature that uses it names its own topics.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mqtt {
    /// `tcp://host:port`
    pub address: String,
}

/// Polls the usage of the devices and publishes it over MQTT.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceUsage {
    /// A disabled section starts no polling, and the rest of it is kept as written.
    pub enabled: bool,
    /// How often the usage of each device is fetched.
    pub refresh_rate_s: u64,
    /// Topic the usage is published on.
    pub publish_topic: String,
    /// At least one.
    pub devices: Vec<Device>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Device {
    pub ip_address: String,
    pub name: String,
    pub record_time_usage: bool,
}

/// Unknown keys are rejected, as on `Thermostat`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Heater {
    pub name: String,
    pub ip_address: String,
    /// What is done to the heater itself once its plug has been switched on. Optional.
    pub follow_up: Option<FollowUp>,
}

impl Heater {
    /// The follow-up when there is one and it is enabled.
    pub fn enabled_follow_up(&self) -> Option<&FollowUp> {
        self.follow_up
            .as_ref()
            .filter(|follow_up| follow_up.enabled())
    }
}

/// What a heater needs after its plug has powered it on, for one that does not heat by itself
/// then. `device_type` names the kind of heater, as it does the device on `PresenceSource`.
///
/// It never delays or changes the switching of the plug, which stays the only way a thermostat
/// stops a heater.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "device_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FollowUp {
    /// A Dimplex heater of the Remo app, such as the EVO2BTA. It starts in Standby whenever it
    /// is powered on, so it is put in Manual over Bluetooth LE.
    DimplexRemo {
        /// A disabled follow-up leaves the heater as its plug powers it on, and the rest of it
        /// is kept as written.
        enabled: bool,
        /// The heater's Bluetooth address, e.g. `00:A0:50:41:9A:26`. The passkey it pairs with
        /// is derived from it.
        bluetooth_address: String,
        /// The setpoint in °C the heater is put in Manual at. Set it above every period's
        /// target, so that the thermostat rather than the heater decides when the room is warm.
        temperature: u8,
    },
}

impl FollowUp {
    pub fn enabled(&self) -> bool {
        match self {
            Self::DimplexRemo { enabled, .. } => *enabled,
        }
    }
}

/// A part of the day in which a thermostat runs, with the rule it runs under. Outside every
/// period nothing is queried and the heaters are switched off on every tick.
///
/// The times are validated on the local clock, so on the two daylight saving nights a period or
/// gap inside the skipped hour gets no tick, and one inside the repeated hour runs twice. Keep
/// the boundaries away from that hour.
///
/// Unknown keys are rejected, as on `Thermostat`: a per-period `deadband` or a `time_zone` left
/// over from the single-period shape would otherwise be read and silently dropped.
///
/// It is serialized as written, into the published decisions.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActivePeriod {
    /// Inclusive, as `HH:MM` in the thermostat's `time_zone`.
    #[serde(
        deserialize_with = "deserialize_time",
        serialize_with = "serialize_time"
    )]
    pub start: NaiveTime,
    /// Exclusive, as `HH:MM` in the thermostat's `time_zone`. An end before the start crosses
    /// midnight.
    #[serde(
        deserialize_with = "deserialize_time",
        serialize_with = "serialize_time"
    )]
    pub end: NaiveTime,
    /// Target temperature in °C.
    pub target_temperature: f64,
    /// Whether the heaters are only switched on while a person is present. `false` heats the
    /// room regardless, e.g. before anyone is up.
    pub require_presence: bool,
}

impl ActivePeriod {
    /// How long the period lasts, as the local clock goes; a period crossing midnight is counted
    /// around it.
    fn length(&self) -> chrono::Duration {
        wrap_around_midnight(self.end - self.start)
    }

    /// The time from this period's end to the start of `next`, wrapping around midnight; zero
    /// when they abut.
    fn gap_before(&self, next: &ActivePeriod) -> chrono::Duration {
        wrap_around_midnight(next.start - self.end)
    }

    /// Whether the local `time` falls in the period.
    pub fn contains(&self, time: NaiveTime) -> bool {
        if self.start < self.end {
            self.start <= time && time < self.end
        } else {
            self.start <= time || time < self.end
        }
    }

    /// Whether the two periods share any instant. Two half-open arcs on the clock overlap
    /// exactly when one's start lies inside the other.
    fn overlaps(&self, other: &ActivePeriod) -> bool {
        self.contains(other.start) || other.contains(self.start)
    }
}

/// `HH:MM-HH:MM`, as the period is named in the validation errors.
impl std::fmt::Display for ActivePeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}",
            self.start.format("%H:%M"),
            self.end.format("%H:%M")
        )
    }
}

/// A difference of two clock times, counted forwards around midnight when it comes out negative.
fn wrap_around_midnight(duration: chrono::Duration) -> chrono::Duration {
    if duration < chrono::Duration::zero() {
        duration + chrono::Duration::days(1)
    } else {
        duration
    }
}

/// `HH:MM` rather than chrono's own `HH:MM:SS`, which nobody writes in a settings file.
fn deserialize_time<'de, D: Deserializer<'de>>(deserializer: D) -> Result<NaiveTime, D::Error> {
    let s = String::deserialize(deserializer)?;

    NaiveTime::parse_from_str(&s, "%H:%M").map_err(|e| {
        serde::de::Error::custom(format!("'{s}' is not a time in the HH:MM format: {e}"))
    })
}

/// `HH:MM`, as `deserialize_time` reads it.
fn serialize_time<S: Serializer>(time: &NaiveTime, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&time.format("%H:%M"))
}

/// The device a thermostat asks for presence. `device_type` names the Tapo handler it is reached
/// through, not its model, as `DeviceType` does for the devices of the API. It is kept apart
/// from `DeviceType`, which lists what is switched through a session; none of these are.
///
/// `window_s` is declared on each variant rather than on a struct with the enum flattened into
/// it: serde's `flatten` disables `deny_unknown_fields`, which rejecting a file in the shape of
/// an earlier release relies on.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "device_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PresenceSource {
    /// The `CameraHandler`: C120.
    Camera {
        ip_address: String,
        /// The camera's name in the Tapo app. Every query checks that the device at
        /// `ip_address` carries it, so an address that has moved to another device is a
        /// presence error rather than another room's presence.
        name: String,
        /// A person detected this many seconds ago still counts as present. At least
        /// `interval_s` and at most 24 hours.
        window_s: u64,
    },
    /// The `CameraPtzHandler`: C210, C220, C225, C325WB, C520WS, TC40, TC70.
    CameraPtz {
        ip_address: String,
        /// As on `Camera`.
        name: String,
        /// As on `Camera`.
        window_s: u64,
    },
    /// The `T100Handler`: a T100 motion sensor, reached through the hub it is paired with.
    T100 {
        /// The Tapo handler of the hub: a T100 can be paired with more than one kind.
        hub_type: HubType,
        /// The hub's address.
        ip_address: String,
        /// The sensor's nickname in the Tapo app, which it is looked up by on every query.
        /// Renaming the sensor there makes every query a presence error until this follows,
        /// and so does a second T100 with the same nickname on the hub.
        name: String,
        /// Motion this many seconds ago still counts as presence. Bounded as on `Camera`.
        window_s: u64,
    },
}

/// The hub a T100 is paired with, named by its Tapo handler as `PresenceSource` is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HubType {
    /// The `HubHandler`: H100.
    Hub,
    /// The `HubIrHandler`: H110.
    HubIr,
    /// The `CameraHubHandler`: H200, H500. It is logged into with the password only; the
    /// username is ignored.
    CameraHub,
}

impl PresenceSource {
    /// As written in the settings, for the spans.
    pub fn device_type(&self) -> &'static str {
        match self {
            Self::Camera { .. } => "camera",
            Self::CameraPtz { .. } => "camera_ptz",
            Self::T100 { .. } => "t100",
        }
    }

    /// The address that is logged into: the camera's, or the hub's for a T100.
    pub fn ip_address(&self) -> &str {
        match self {
            Self::Camera { ip_address, .. }
            | Self::CameraPtz { ip_address, .. }
            | Self::T100 { ip_address, .. } => ip_address,
        }
    }

    /// The device's name in the Tapo app: the one a camera is checked against, and the one a
    /// T100 is looked up by on its hub.
    pub fn name(&self) -> &str {
        match self {
            Self::Camera { name, .. } | Self::CameraPtz { name, .. } | Self::T100 { name, .. } => {
                name
            }
        }
    }

    /// Presence this many seconds ago still counts.
    pub fn window_s(&self) -> u64 {
        match self {
            Self::Camera { window_s, .. }
            | Self::CameraPtz { window_s, .. }
            | Self::T100 { window_s, .. } => *window_s,
        }
    }
}

/// The temperature sensor a thermostat reads.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sensor {
    /// Matches the `location` field of the sensor payload.
    pub location: String,
    /// A reading older than this many seconds is treated as missing.
    pub reading_max_age_s: u64,
}

/// Unknown keys are rejected so that a settings file in the shape of an earlier release, which
/// kept `active_period` and `target_temperature` here, and later `camera_ip_address` and
/// `presence_window_s`, and then `sensor_location` and `reading_max_age_s`, fails at startup
/// instead of being read with those silently ignored.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Thermostat {
    pub name: String,
    /// A disabled thermostat is not started and leaves its heaters alone.
    pub enabled: bool,
    /// IANA name, e.g. `Europe/London`, that the periods are in, so that they follow the local
    /// clock across the daylight saving changes rather than the container's, which is UTC.
    pub time_zone: Tz,
    /// At least one. The periods may abut but not overlap, and a gap between two of them has to
    /// last at least `interval_s` or not exist at all.
    pub active_periods: Vec<ActivePeriod>,
    /// The device that decides presence. Required even when no period requires presence.
    pub presence: PresenceSource,
    /// The temperature sensor whose readings are evaluated.
    pub sensor: Sensor,
    /// Plugs switched together. Optional; without any, the decision is only logged and published.
    #[serde(default)]
    pub heaters: Vec<Heater>,
    /// Half-width of the band around the target in which the last command is repeated.
    pub deadband: f64,
    /// The longest time between two evaluations, the one used when no reading arrives.
    pub interval_s: u64,
    /// The shortest time between two evaluations. Every reading is evaluated as it arrives, or
    /// once this has passed since the last evaluation. At least 60 and less than `interval_s`.
    pub min_interval_s: u64,
}

impl Thermostat {
    /// The period the thermostat runs under at `now`, or `None` outside all of them. The periods
    /// are validated not to overlap, so there is at most one.
    pub fn active_period(&self, now: DateTime<Utc>) -> Option<&ActivePeriod> {
        let time = now.with_timezone(&self.time_zone).time();

        self.active_periods
            .iter()
            .find(|period| period.contains(time))
    }
}

/// Runs the thermostats on the sensor readings received over MQTT.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Heating {
    /// A disabled section starts no thermostat and leaves every heater alone, and the rest of it
    /// is kept as written.
    pub enabled: bool,
    /// Topic filter the sensor readings are received on, e.g. `sensors/#`.
    pub sensors_subscribe_topic: String,
    /// Topic every thermostat's decisions are published on. Not optional: the decision stream is
    /// what an alert on a stopped thermostat or an undeliverable command is built on.
    pub decisions_publish_topic: String,
    /// At least one; a single thermostat is switched off with its own `enabled`.
    pub thermostats: Vec<Thermostat>,
}

impl Heating {
    /// The thermostats with `enabled` set, which are started when the section is enabled too.
    pub fn enabled_thermostats(&self) -> impl Iterator<Item = &Thermostat> {
        self.thermostats
            .iter()
            .filter(|thermostat| thermostat.enabled)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub api: Api,
    pub tapo: Tapo,
    /// Optional; required when `device_usage` or `heating` is enabled.
    pub mqtt: Option<Mqtt>,
    /// Optional; a missing section is the same as a disabled one.
    pub device_usage: Option<DeviceUsage>,
    /// Optional; a missing section is the same as a disabled one.
    pub heating: Option<Heating>,
}

impl Settings {
    pub fn new() -> Result<Self, anyhow::Error> {
        let base_path = std::env::current_dir().expect("Failed to determine the current directory");

        let mut builder = config::Config::builder();

        let config_path = base_path.join("settings.yaml");
        builder = builder.add_source(config::File::new(
            config_path.to_str().expect("This should never happen"),
            config::FileFormat::Yaml,
        ));

        let settings = builder.build()?.try_deserialize::<Self>()?;
        settings.validate()?;

        Ok(settings)
    }

    /// The device usage section when it is present and enabled.
    pub fn enabled_device_usage(&self) -> Option<&DeviceUsage> {
        self.device_usage
            .as_ref()
            .filter(|device_usage| device_usage.enabled)
    }

    /// The heating section when it is present and enabled.
    pub fn enabled_heating(&self) -> Option<&Heating> {
        self.heating.as_ref().filter(|heating| heating.enabled)
    }

    /// Every section is validated whether it is enabled or not, so that a mistake in a disabled
    /// one surfaces when it is written rather than when it is switched on.
    ///
    /// Rejects values that deserialize fine but would fail later, where the cause is harder to
    /// trace back to the settings: an enabled feature without the broker it publishes to or
    /// reads from cannot run, an empty list of devices or thermostats is a disabled section
    /// spelled another way, a zero interval busy-loops its actor's ticks, a presence window
    /// shorter than the interval misses anyone detected early in it and an empty one never sees
    /// anyone, a presence window over 24 hours is refused by a camera on every tick, a minimum
    /// interval under a minute makes a reading likely to cancel a switch still running and one of
    /// a whole interval or more never evaluates a reading early, a zero reading age makes every
    /// reading stale and an oversized one overflows the age arithmetic, a
    /// negative deadband inverts the rules, a non-finite temperature makes every comparison
    /// false, a duplicate key makes the coordinator skip an entry, a follow-up without a
    /// Bluetooth address has no heater to reach and two with the same one would each confirm
    /// the other's heater, a thermostat without a period
    /// would never heat, an active period that starts when it ends is either empty or the whole
    /// day, which the thermostat cannot tell apart, one shorter than the interval can pass
    /// between two ticks without either landing in it, two that overlap leave the rule for the
    /// overlap to the order of the list, a gap between two periods shorter than the interval
    /// can pass between two ticks without the `Off` handover ever happening, and an empty topic
    /// leaves its feature with nothing to publish on or subscribe to.
    fn validate(&self) -> Result<(), anyhow::Error> {
        ensure!(
            self.mqtt.is_some()
                || (self.enabled_device_usage().is_none() && self.enabled_heating().is_none()),
            "mqtt must be set when device_usage or heating is enabled"
        );

        if let Some(device_usage) = &self.device_usage {
            device_usage.validate()?;
        }

        if let Some(heating) = &self.heating {
            heating.validate()?;
        }

        Ok(())
    }
}

/// A topic has to be given. Whether it is a valid one is left to paho, which refuses it on the
/// publish or the subscription.
fn validate_topic(key: &str, topic: &str) -> Result<(), anyhow::Error> {
    ensure!(!topic.is_empty(), "{key} must not be empty");

    Ok(())
}

impl DeviceUsage {
    fn validate(&self) -> Result<(), anyhow::Error> {
        ensure!(
            self.refresh_rate_s > 0,
            "device_usage.refresh_rate_s must be greater than 0"
        );
        validate_topic("device_usage.publish_topic", &self.publish_topic)?;
        ensure!(
            !self.devices.is_empty(),
            "device_usage.devices must have at least one entry; set device_usage.enabled to \
             false to switch the polling off"
        );

        let mut ip_addresses = HashSet::new();
        for device in &self.devices {
            ensure!(
                ip_addresses.insert(&device.ip_address),
                "Duplicate device ip_address '{}'",
                device.ip_address
            );
        }

        Ok(())
    }
}

impl Heating {
    fn validate(&self) -> Result<(), anyhow::Error> {
        ensure!(
            !self.thermostats.is_empty(),
            "heating.thermostats must have at least one entry; set heating.enabled to false to \
             switch the heating off"
        );

        validate_topic(
            "heating.sensors_subscribe_topic",
            &self.sensors_subscribe_topic,
        )?;
        validate_topic(
            "heating.decisions_publish_topic",
            &self.decisions_publish_topic,
        )?;

        let mut names = HashSet::new();
        let mut bluetooth_addresses = HashSet::new();
        for thermostat in &self.thermostats {
            let name = &thermostat.name;

            ensure!(names.insert(name), "Duplicate thermostat name '{name}'");
            // The two bounds on `min_interval_s` keep `interval_s` over a minute too, so a zero
            // interval needs no check of its own.
            ensure!(
                thermostat.min_interval_s >= MIN_MIN_INTERVAL_S,
                "Thermostat '{name}': min_interval_s must be at least {MIN_MIN_INTERVAL_S}, about \
                 the longest an evaluation with a slow plug takes, so that a reading is unlikely \
                 to cancel a switch still running"
            );
            ensure!(
                thermostat.min_interval_s < thermostat.interval_s,
                "Thermostat '{name}': min_interval_s must be less than interval_s, otherwise no \
                 reading is evaluated before the interval runs out"
            );
            ensure!(
                thermostat.presence.window_s() >= thermostat.interval_s,
                "Thermostat '{name}': presence.window_s must be at least interval_s, otherwise \
                 presence detected between two ticks can go unnoticed"
            );
            ensure!(
                thermostat.presence.window_s() <= MAX_PRESENCE_WINDOW_S,
                "Thermostat '{name}': presence.window_s must be at most {MAX_PRESENCE_WINDOW_S} \
                 (24 hours), the longest window a camera accepts, kept for every presence source"
            );
            ensure!(
                thermostat.sensor.reading_max_age_s > 0,
                "Thermostat '{name}': sensor.reading_max_age_s must be greater than 0"
            );
            ensure!(
                thermostat.sensor.reading_max_age_s <= MAX_READING_MAX_AGE_S,
                "Thermostat '{name}': sensor.reading_max_age_s must be at most \
                 {MAX_READING_MAX_AGE_S} (24 hours)"
            );
            ensure!(
                thermostat.deadband.is_finite(),
                "Thermostat '{name}': deadband must be a finite number"
            );
            ensure!(
                thermostat.deadband >= 0.0,
                "Thermostat '{name}': deadband must not be negative"
            );
            for heater in &thermostat.heaters {
                // Whether it is an address is left to bluer, which refuses it on every attempt.
                if let Some(FollowUp::DimplexRemo {
                    enabled,
                    bluetooth_address,
                    ..
                }) = &heater.follow_up
                {
                    ensure!(
                        !bluetooth_address.is_empty(),
                        "Thermostat '{name}': follow_up.bluetooth_address of the heater '{}' must \
                         not be empty",
                        heater.name
                    );
                    // Only among the follow-ups that run: a heater may be listed by a second
                    // thermostat that is switched off, as an alternative to the first. The case
                    // is not part of an address.
                    if thermostat.enabled && *enabled {
                        ensure!(
                            bluetooth_addresses.insert(bluetooth_address.to_ascii_uppercase()),
                            "Duplicate follow_up.bluetooth_address '{bluetooth_address}' among \
                             the enabled follow-ups of the enabled thermostats"
                        );
                    }
                }
            }
            thermostat.validate_active_periods()?;
        }

        Ok(())
    }
}

impl Thermostat {
    fn validate_active_periods(&self) -> Result<(), anyhow::Error> {
        let name = &self.name;
        let interval = chrono::Duration::seconds(self.interval_s as i64);

        ensure!(
            !self.active_periods.is_empty(),
            "Thermostat '{name}': active_periods must have at least one entry; set enabled to \
             false to switch the thermostat off"
        );

        for period in &self.active_periods {
            ensure!(
                period.start != period.end,
                "Thermostat '{name}': the active period {period} must have a start and an end \
                 that differ; cover the whole day with abutting periods instead"
            );
            ensure!(
                period.length() >= interval,
                "Thermostat '{name}': the active period {period} must last at least interval_s, \
                 otherwise it can pass between two ticks"
            );
            ensure!(
                period.target_temperature.is_finite(),
                "Thermostat '{name}': the active period {period} must have a finite \
                 target_temperature"
            );
        }

        for (i, period) in self.active_periods.iter().enumerate() {
            for other in &self.active_periods[i + 1..] {
                ensure!(
                    !period.overlaps(other),
                    "Thermostat '{name}': the active periods {period} and {other} overlap"
                );
            }
        }

        // Sorted by start, each period is followed around the clock by the next one, the last
        // by the first. Non-overlap makes this ordering the one along the day.
        let mut sorted: Vec<&ActivePeriod> = self.active_periods.iter().collect();
        sorted.sort_by_key(|period| period.start);

        for (i, period) in sorted.iter().enumerate() {
            let next = sorted[(i + 1) % sorted.len()];
            let gap = period.gap_before(next);

            if gap == chrono::Duration::zero() || gap >= interval {
                continue;
            }

            if sorted.len() == 1 {
                anyhow::bail!(
                    "Thermostat '{name}': the time outside the active period {period} must last \
                     at least interval_s, otherwise no tick switches the heaters off"
                );
            }

            anyhow::bail!(
                "Thermostat '{name}': the gap between the active periods {period} and {next} must \
                 last at least interval_s, otherwise no tick switches the heaters off in it; make \
                 the periods abut or widen the gap"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl ActivePeriod {
        /// A period from `HH:MM` strings, for the tests in this crate.
        pub fn new(
            start: &str,
            end: &str,
            target_temperature: f64,
            require_presence: bool,
        ) -> Self {
            Self {
                start: NaiveTime::parse_from_str(start, "%H:%M").unwrap(),
                end: NaiveTime::parse_from_str(end, "%H:%M").unwrap(),
                target_temperature,
                require_presence,
            }
        }
    }

    fn thermostat(name: &str) -> Thermostat {
        Thermostat {
            name: name.to_string(),
            enabled: true,
            time_zone: chrono_tz::Europe::London,
            active_periods: vec![period("08:00", "22:00")],
            presence: camera(900),
            sensor: Sensor {
                location: "Living Room".to_string(),
                reading_max_age_s: 900,
            },
            heaters: vec![],
            deadband: 0.25,
            interval_s: 300,
            min_interval_s: 60,
        }
    }

    fn camera(window_s: u64) -> PresenceSource {
        PresenceSource::Camera {
            ip_address: "192.168.1.154".to_string(),
            name: "Living Room Camera".to_string(),
            window_s,
        }
    }

    fn settings() -> Settings {
        Settings {
            api: Api {
                host: "127.0.0.1".to_string(),
                port: 8080,
            },
            tapo: Tapo {
                username: String::new(),
                password: String::new(),
            },
            mqtt: Some(Mqtt {
                address: "tcp://localhost:1883".to_string(),
            }),
            device_usage: Some(DeviceUsage {
                enabled: true,
                refresh_rate_s: 600,
                publish_topic: "devices/tapo".to_string(),
                devices: vec![device("192.168.1.100")],
            }),
            heating: Some(Heating {
                enabled: true,
                sensors_subscribe_topic: "sensors/#".to_string(),
                decisions_publish_topic: "thermostats/decisions".to_string(),
                thermostats: vec![thermostat("Living Room")],
            }),
        }
    }

    fn device(ip_address: &str) -> Device {
        Device {
            ip_address: ip_address.to_string(),
            name: "Plug".to_string(),
            record_time_usage: false,
        }
    }

    /// A heater with a Dimplex follow-up.
    fn follow_up_heater(name: &str, bluetooth_address: &str, enabled: bool) -> Heater {
        Heater {
            name: name.to_string(),
            ip_address: "192.168.1.115".to_string(),
            follow_up: Some(FollowUp::DimplexRemo {
                enabled,
                bluetooth_address: bluetooth_address.to_string(),
                temperature: 25,
            }),
        }
    }

    fn device_usage(settings: &mut Settings) -> &mut DeviceUsage {
        settings.device_usage.as_mut().unwrap()
    }

    fn heating(settings: &mut Settings) -> &mut Heating {
        settings.heating.as_mut().unwrap()
    }

    fn error(settings: &Settings) -> String {
        settings
            .validate()
            .expect_err("the settings should be rejected")
            .to_string()
    }

    #[test]
    fn valid_settings_pass() {
        settings().validate().unwrap();
    }

    #[test]
    fn zero_refresh_rate_is_rejected() {
        let mut settings = settings();
        device_usage(&mut settings).refresh_rate_s = 0;

        assert!(error(&settings).contains("refresh_rate_s"));
    }

    #[test]
    fn empty_topic_is_rejected_even_when_disabled() {
        for enabled in [true, false] {
            // The error of the settings with one topic emptied by `clear`.
            let error_with = |clear: fn(&mut Settings)| {
                let mut settings = settings();
                device_usage(&mut settings).enabled = enabled;
                heating(&mut settings).enabled = enabled;
                clear(&mut settings);
                error(&settings)
            };

            for (key, error) in [
                (
                    "device_usage.publish_topic",
                    error_with(|settings| device_usage(settings).publish_topic.clear()),
                ),
                (
                    "heating.sensors_subscribe_topic",
                    error_with(|settings| heating(settings).sensors_subscribe_topic.clear()),
                ),
                (
                    "heating.decisions_publish_topic",
                    error_with(|settings| heating(settings).decisions_publish_topic.clear()),
                ),
            ] {
                assert!(
                    error.contains(&format!("{key} must not be empty")),
                    "{key}, enabled {enabled}: {error}"
                );
            }
        }
    }

    #[test]
    fn duplicate_device_ip_address_is_rejected() {
        let mut settings = settings();
        device_usage(&mut settings).devices =
            vec![device("192.168.1.100"), device("192.168.1.100")];

        assert!(error(&settings).contains("192.168.1.100"));
    }

    #[test]
    fn duplicate_thermostat_name_is_rejected() {
        let mut settings = settings();
        heating(&mut settings)
            .thermostats
            .push(thermostat("Living Room"));

        assert!(error(&settings).contains("Duplicate thermostat name"));
    }

    #[test]
    fn zero_interval_is_rejected() {
        let mut settings = settings();
        heating(&mut settings).thermostats[0].interval_s = 0;

        assert!(error(&settings).contains("interval_s"));
    }

    #[test]
    fn min_interval_not_less_than_interval_is_rejected() {
        for min_interval_s in [300, 301] {
            let mut settings = settings();
            heating(&mut settings).thermostats[0].min_interval_s = min_interval_s;

            assert!(
                error(&settings).contains("min_interval_s must be less than interval_s"),
                "min_interval_s {min_interval_s}"
            );
        }
    }

    #[test]
    fn min_interval_under_a_minute_is_rejected() {
        for min_interval_s in [0, 59] {
            let mut settings = settings();
            heating(&mut settings).thermostats[0].min_interval_s = min_interval_s;

            assert!(
                error(&settings).contains("min_interval_s must be at least 60"),
                "min_interval_s {min_interval_s}"
            );
        }
    }

    #[test]
    fn presence_window_shorter_than_interval_is_rejected() {
        for window_s in [0, 299] {
            let mut settings = settings();
            heating(&mut settings).thermostats[0].presence = camera(window_s);

            assert!(
                error(&settings).contains("presence.window_s must be at least interval_s"),
                "window_s {window_s}"
            );
        }
    }

    #[test]
    fn presence_window_equal_to_interval_passes() {
        let mut settings = settings();
        heating(&mut settings).thermostats[0].presence = camera(300);

        settings.validate().unwrap();
    }

    #[test]
    fn presence_window_over_a_day_is_rejected() {
        let mut settings = settings();
        heating(&mut settings).thermostats[0].presence = camera(MAX_PRESENCE_WINDOW_S + 1);

        assert!(error(&settings).contains("presence.window_s must be at most"));
    }

    #[test]
    fn zero_reading_max_age_is_rejected() {
        let mut settings = settings();
        heating(&mut settings).thermostats[0]
            .sensor
            .reading_max_age_s = 0;

        assert!(error(&settings).contains("sensor.reading_max_age_s must be greater than 0"));
    }

    #[test]
    fn reading_max_age_over_a_day_is_rejected() {
        for reading_max_age_s in [MAX_READING_MAX_AGE_S + 1, u64::MAX] {
            let mut settings = settings();
            heating(&mut settings).thermostats[0]
                .sensor
                .reading_max_age_s = reading_max_age_s;

            assert!(
                error(&settings).contains("sensor.reading_max_age_s must be at most"),
                "reading_max_age_s {reading_max_age_s}"
            );
        }
    }

    #[test]
    fn non_finite_target_temperature_is_rejected() {
        for target_temperature in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut settings = settings();
            heating(&mut settings).thermostats[0].active_periods[0].target_temperature =
                target_temperature;

            assert!(
                error(&settings).contains("finite target_temperature"),
                "target_temperature {target_temperature}"
            );
        }
    }

    #[test]
    fn non_finite_deadband_is_rejected() {
        for deadband in [f64::NAN, f64::INFINITY] {
            let mut settings = settings();
            heating(&mut settings).thermostats[0].deadband = deadband;

            assert!(
                error(&settings).contains("deadband must be a finite number"),
                "deadband {deadband}"
            );
        }
    }

    #[test]
    fn negative_deadband_is_rejected() {
        let mut settings = settings();
        heating(&mut settings).thermostats[0].deadband = -0.25;

        assert!(error(&settings).contains("deadband"));
    }

    #[test]
    fn empty_follow_up_bluetooth_address_is_rejected_even_when_disabled() {
        for enabled in [true, false] {
            let mut settings = settings();
            heating(&mut settings).thermostats[0].heaters =
                vec![follow_up_heater("Office Heater", "", enabled)];

            assert!(
                error(&settings).contains("follow_up.bluetooth_address"),
                "enabled {enabled}"
            );
        }
    }

    #[test]
    fn duplicate_follow_up_bluetooth_address_is_rejected() {
        const DUPLICATE: &str = "Duplicate follow_up.bluetooth_address";

        // In one thermostat; the case of the address does not tell them apart.
        let mut in_one = settings();
        heating(&mut in_one).thermostats[0].heaters = vec![
            follow_up_heater("Office Heater", "00:A0:50:41:9A:26", true),
            follow_up_heater("Other Heater", "00:a0:50:41:9a:26", true),
        ];
        assert!(error(&in_one).contains(DUPLICATE));

        // Across two.
        let mut settings = settings();
        let mut other = thermostat("Office");
        other.heaters = vec![follow_up_heater("Other Heater", "00:A0:50:41:9A:26", true)];
        heating(&mut settings).thermostats[0].heaters =
            vec![follow_up_heater("Office Heater", "00:A0:50:41:9A:26", true)];
        heating(&mut settings).thermostats.push(other);
        assert!(error(&settings).contains(DUPLICATE));

        // A second thermostat that is switched off may list the same heater, and so may a
        // follow-up that is.
        heating(&mut settings).thermostats[1].enabled = false;
        settings.validate().unwrap();
        heating(&mut settings).thermostats[1].enabled = true;

        heating(&mut settings).thermostats[1].heaters[0] =
            follow_up_heater("Other Heater", "00:A0:50:41:9A:26", false);
        settings.validate().unwrap();

        heating(&mut settings).thermostats[1].heaters[0] =
            follow_up_heater("Other Heater", "00:A0:50:41:9A:27", true);
        settings.validate().unwrap();
    }

    #[test]
    fn empty_devices_are_rejected_even_when_disabled() {
        for enabled in [true, false] {
            let mut settings = settings();
            device_usage(&mut settings).enabled = enabled;
            device_usage(&mut settings).devices = vec![];

            assert!(
                error(&settings).contains("device_usage.devices must have at least one entry"),
                "enabled {enabled}"
            );
        }
    }

    #[test]
    fn empty_thermostats_are_rejected_even_when_disabled() {
        for enabled in [true, false] {
            let mut settings = settings();
            heating(&mut settings).enabled = enabled;
            heating(&mut settings).thermostats = vec![];

            assert!(
                error(&settings).contains("heating.thermostats must have at least one entry"),
                "enabled {enabled}"
            );
        }
    }

    #[test]
    fn disabled_device_usage_is_still_validated() {
        let mut settings = settings();
        device_usage(&mut settings).enabled = false;
        device_usage(&mut settings).refresh_rate_s = 0;

        assert!(error(&settings).contains("refresh_rate_s"));
    }

    #[test]
    fn disabled_heating_is_still_validated() {
        let mut settings = settings();
        heating(&mut settings).enabled = false;
        heating(&mut settings).thermostats[0].interval_s = 0;

        assert!(error(&settings).contains("interval_s"));
    }

    #[test]
    fn enabled_feature_without_mqtt_is_rejected() {
        for (device_usage_enabled, heating_enabled) in [(true, false), (false, true), (true, true)]
        {
            let mut settings = settings();
            settings.mqtt = None;
            device_usage(&mut settings).enabled = device_usage_enabled;
            heating(&mut settings).enabled = heating_enabled;

            assert!(
                error(&settings).contains("mqtt must be set"),
                "device_usage {device_usage_enabled}, heating {heating_enabled}"
            );
        }
    }

    #[test]
    fn disabled_features_without_mqtt_pass() {
        let mut settings = settings();
        settings.mqtt = None;
        device_usage(&mut settings).enabled = false;
        heating(&mut settings).enabled = false;

        settings.validate().unwrap();

        settings.device_usage = None;
        settings.heating = None;

        settings.validate().unwrap();
    }

    #[test]
    fn only_thermostats_enabled_in_an_enabled_section_are_started() {
        let mut settings = settings();
        let mut disabled = thermostat("Bedroom");
        disabled.enabled = false;
        heating(&mut settings).thermostats.push(disabled);

        // As the coordinator picks them.
        let names = |settings: &Settings| {
            settings
                .enabled_heating()
                .into_iter()
                .flat_map(|heating| heating.enabled_thermostats())
                .map(|thermostat| thermostat.name.clone())
                .collect::<Vec<_>>()
        };

        assert_eq!(names(&settings), ["Living Room"]);

        heating(&mut settings).enabled = false;

        assert!(names(&settings).is_empty());
    }

    fn period(start: &str, end: &str) -> ActivePeriod {
        ActivePeriod::new(start, end, 22.0, true)
    }

    fn time(s: &str) -> NaiveTime {
        NaiveTime::parse_from_str(s, "%H:%M").unwrap()
    }

    /// A UTC instant from a London wall-clock time on the given day.
    fn london(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        use chrono::TimeZone;

        chrono_tz::Europe::London
            .with_ymd_and_hms(y, m, d, h, min, 0)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn with_periods(periods: Vec<ActivePeriod>) -> Settings {
        let mut settings = settings();
        heating(&mut settings).thermostats[0].active_periods = periods;
        settings
    }

    #[test]
    fn active_period_with_the_same_start_and_end_is_rejected() {
        let settings = with_periods(vec![period("08:00", "08:00")]);

        assert!(error(&settings).contains("start and an end that differ"));
    }

    #[test]
    fn active_period_passes() {
        with_periods(vec![period("08:00", "22:00")])
            .validate()
            .unwrap();
    }

    #[test]
    fn empty_active_periods_are_rejected() {
        let settings = with_periods(vec![]);

        assert!(error(&settings).contains("at least one entry"));
    }

    #[test]
    fn active_period_shorter_than_interval_is_rejected() {
        // interval_s is 300; the second period crosses midnight.
        for (start, end) in [("08:00", "08:04"), ("23:58", "00:02")] {
            let settings = with_periods(vec![period(start, end)]);

            assert!(
                error(&settings).contains("must last at least interval_s, otherwise it can pass"),
                "period {start}-{end}"
            );
        }
    }

    #[test]
    fn gap_outside_active_period_shorter_than_interval_is_rejected() {
        // interval_s is 300; the second period crosses midnight.
        for (start, end) in [("08:04", "08:00"), ("00:02", "23:58")] {
            let settings = with_periods(vec![period(start, end)]);

            assert!(
                error(&settings).contains("the time outside the active period"),
                "period {start}-{end}"
            );
        }
    }

    #[test]
    fn gap_outside_active_period_as_long_as_interval_passes() {
        for (start, end) in [("08:05", "08:00"), ("00:03", "23:58")] {
            with_periods(vec![period(start, end)]).validate().unwrap();
        }
    }

    #[test]
    fn active_period_as_long_as_interval_passes() {
        for (start, end) in [("08:00", "08:05"), ("23:58", "00:03")] {
            with_periods(vec![period(start, end)]).validate().unwrap();
        }
    }

    #[test]
    fn overlapping_active_periods_are_rejected() {
        for (a, b) in [
            (("05:45", "07:30"), ("07:00", "22:00")),
            // One contains the other.
            (("07:00", "22:00"), ("08:00", "09:00")),
            // The first crosses midnight and contains the second's start.
            (("22:00", "06:00"), ("05:00", "07:00")),
            // Both cross midnight.
            (("22:00", "06:00"), ("23:00", "01:00")),
        ] {
            let settings = with_periods(vec![period(a.0, a.1), period(b.0, b.1)]);

            assert!(
                error(&settings).contains("overlap"),
                "periods {}-{} and {}-{}",
                a.0,
                a.1,
                b.0,
                b.1
            );
        }
    }

    #[test]
    fn abutting_active_periods_pass() {
        with_periods(vec![period("05:45", "07:00"), period("07:00", "22:00")])
            .validate()
            .unwrap();
        // Abutting at midnight and tiling the whole day, in list order and reversed.
        with_periods(vec![period("22:00", "06:00"), period("06:00", "22:00")])
            .validate()
            .unwrap();
        with_periods(vec![period("06:00", "22:00"), period("22:00", "06:00")])
            .validate()
            .unwrap();
    }

    #[test]
    fn gap_between_active_periods_shorter_than_interval_is_rejected() {
        // interval_s is 300.
        for (first, second) in [
            (("05:45", "06:56"), ("07:00", "22:00")),
            // The gap wraps around midnight.
            (("07:00", "23:58"), ("00:02", "06:00")),
            // The list order is not the clock order.
            (("07:00", "22:00"), ("05:45", "06:56")),
        ] {
            let settings = with_periods(vec![period(first.0, first.1), period(second.0, second.1)]);

            assert!(
                error(&settings).contains("the gap between the active periods"),
                "periods {}-{} and {}-{}",
                first.0,
                first.1,
                second.0,
                second.1
            );
        }
    }

    #[test]
    fn gap_between_active_periods_as_long_as_interval_passes() {
        with_periods(vec![period("05:45", "06:55"), period("07:00", "22:00")])
            .validate()
            .unwrap();
        with_periods(vec![period("07:00", "23:58"), period("00:03", "06:00")])
            .validate()
            .unwrap();
    }

    const YAML_HEAD: &str = r#"
api: { host: 127.0.0.1, port: 8080 }
tapo: { username: "", password: "" }
mqtt: { address: tcp://localhost:1883 }
heating:
  enabled: true
  sensors_subscribe_topic: "sensors/#"
  decisions_publish_topic: thermostats/decisions
"#;

    fn parse(yaml: &str) -> Result<Settings, config::ConfigError> {
        config::Config::builder()
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize::<Settings>()
    }

    #[test]
    fn active_periods_are_read_from_yaml() {
        let yaml = format!(
            "{YAML_HEAD}
  thermostats:
    - name: Living Room
      enabled: true
      time_zone: Europe/London
      active_periods:
        - start: \"05:45\"
          end: \"07:00\"
          target_temperature: 21.0
          require_presence: false
        - start: \"07:00\"
          end: \"22:00\"
          target_temperature: 22.0
          require_presence: true
{PRESENCE_YAML}
      sensor:
        location: Living Room
        reading_max_age_s: 900
      deadband: 0.25
      interval_s: 300
      min_interval_s: 60
"
        );

        let settings = parse(&yaml).unwrap();
        settings.validate().unwrap();

        let thermostat = &settings.heating.unwrap().thermostats[0];
        assert_eq!(thermostat.time_zone, chrono_tz::Europe::London);
        assert_eq!(thermostat.active_periods.len(), 2);

        let morning = &thermostat.active_periods[0];
        assert_eq!(morning.start, time("05:45"));
        assert_eq!(morning.end, time("07:00"));
        assert_eq!(morning.target_temperature, 21.0);
        assert!(!morning.require_presence);

        let day = &thermostat.active_periods[1];
        assert_eq!(day.start, time("07:00"));
        assert_eq!(day.end, time("22:00"));
        assert_eq!(day.target_temperature, 22.0);
        assert!(day.require_presence);
    }

    #[test]
    fn active_period_without_require_presence_is_rejected() {
        let yaml = format!(
            "{YAML_HEAD}
  thermostats:
    - name: Living Room
      enabled: true
      time_zone: Europe/London
      active_periods:
        - start: \"07:00\"
          end: \"22:00\"
          target_temperature: 22.0
{PRESENCE_YAML}
      sensor:
        location: Living Room
        reading_max_age_s: 900
      deadband: 0.25
      interval_s: 300
      min_interval_s: 60
"
        );

        let error = parse(&yaml).expect_err("require_presence is required");
        assert!(
            error.to_string().contains("require_presence"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn unknown_key_in_an_active_period_is_rejected() {
        let yaml = format!(
            "{YAML_HEAD}
  thermostats:
    - name: Living Room
      enabled: true
      time_zone: Europe/London
      active_periods:
        - start: \"07:00\"
          end: \"22:00\"
          target_temperature: 22.0
          require_presence: true
          deadband: 0.5
{PRESENCE_YAML}
      sensor:
        location: Living Room
        reading_max_age_s: 900
      deadband: 0.25
      interval_s: 300
      min_interval_s: 60
"
        );

        let error = parse(&yaml).expect_err("a per-period deadband does not exist");
        assert!(
            error.to_string().contains("unknown field"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn thermostat_in_the_old_shape_is_rejected() {
        // The shape of the release before the schedule: a single period holding the zone and
        // the target on the thermostat. Both keys have to fail rather than be ignored.
        let yaml = format!(
            "{YAML_HEAD}
  thermostats:
    - name: Living Room
      enabled: true
      time_zone: Europe/London
      active_periods:
        - start: \"07:00\"
          end: \"22:00\"
          target_temperature: 22.0
          require_presence: true
      active_period:
        start: \"07:30\"
        end: \"22:00\"
        time_zone: Europe/London
{PRESENCE_YAML}
      sensor:
        location: Living Room
        reading_max_age_s: 900
      target_temperature: 22.0
      deadband: 0.25
      interval_s: 300
      min_interval_s: 60
"
        );

        let error = parse(&yaml).expect_err("unknown keys are rejected");
        assert!(
            error.to_string().contains("unknown field"),
            "unexpected error: {error}"
        );
    }

    /// A valid thermostat but for its presence, which is `keys`: lines of the thermostat's own
    /// keys, indented by six spaces.
    fn yaml_with_presence(keys: &str) -> String {
        format!(
            "{YAML_HEAD}
  thermostats:
    - name: Living Room
      enabled: true
      time_zone: Europe/London
      active_periods:
        - start: \"07:00\"
          end: \"22:00\"
          target_temperature: 22.0
          require_presence: true
{keys}
      sensor:
        location: Living Room
        reading_max_age_s: 900
      deadband: 0.25
      interval_s: 300
      min_interval_s: 60
"
        )
    }

    const PRESENCE_YAML: &str = "      presence:
        device_type: camera
        ip_address: 192.168.1.154
        name: Living Room Camera
        window_s: 900";

    /// `PRESENCE_YAML` with `device_type` in place of `camera`.
    fn presence_yaml_of(device_type: &str) -> String {
        PRESENCE_YAML.replace(
            "device_type: camera",
            &format!("device_type: {device_type}"),
        )
    }

    #[test]
    fn camera_presence_sources_are_read_from_yaml() {
        for device_type in ["camera", "camera_ptz"] {
            let yaml = yaml_with_presence(&presence_yaml_of(device_type));

            let settings = parse(&yaml).unwrap();
            settings.validate().unwrap();

            let presence = &settings.heating.unwrap().thermostats[0].presence;
            assert_eq!(
                matches!(presence, PresenceSource::Camera { .. }),
                device_type == "camera"
            );
            assert_eq!(presence.device_type(), device_type);
            assert_eq!(presence.ip_address(), "192.168.1.154");
            assert_eq!(presence.name(), "Living Room Camera");
            assert_eq!(presence.window_s(), 900);
        }
    }

    #[test]
    fn unknown_key_in_presence_is_rejected() {
        let yaml = yaml_with_presence(&format!("{PRESENCE_YAML}\n        hub_type: hub"));

        let error = parse(&yaml).expect_err("a camera has no hub");
        assert!(
            error.to_string().contains("unknown field"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn unknown_presence_device_type_is_rejected() {
        // `hub` is how the T100's hub is named, not a source of its own.
        for device_type in ["hub", "plug"] {
            let yaml = yaml_with_presence(&presence_yaml_of(device_type));

            let error = parse(&yaml)
                .expect_err("the device type does not exist")
                .to_string();
            assert!(
                error.contains("unknown variant"),
                "device_type {device_type}: {error}"
            );
        }
    }

    #[test]
    fn camera_without_name_is_rejected() {
        for device_type in ["camera", "camera_ptz"] {
            let yaml = yaml_with_presence(
                &presence_yaml_of(device_type).replace("        name: Living Room Camera\n", ""),
            );

            let error = parse(&yaml).expect_err("name is required").to_string();
            assert!(
                error.contains("missing configuration field") && error.ends_with("name\""),
                "device_type {device_type}: {error}"
            );
        }
    }

    /// A T100 presence block whose hub is `hub_type_line`, a line indented by eight spaces, or
    /// nothing.
    fn t100_yaml(hub_type_line: &str) -> String {
        yaml_with_presence(&format!(
            "      presence:
        device_type: t100
{hub_type_line}
        ip_address: 192.168.1.160
        name: Living Room Motion
        window_s: 1800"
        ))
    }

    #[test]
    fn t100_presence_source_is_read_from_yaml() {
        for (written, expected) in [
            ("hub", HubType::Hub),
            ("hub_ir", HubType::HubIr),
            ("camera_hub", HubType::CameraHub),
        ] {
            let settings = parse(&t100_yaml(&format!("        hub_type: {written}"))).unwrap();
            settings.validate().unwrap();

            let presence = &settings.heating.unwrap().thermostats[0].presence;
            assert!(
                matches!(presence, PresenceSource::T100 { hub_type, .. } if *hub_type == expected),
                "hub_type {written}: {presence:?}"
            );
            assert_eq!(presence.device_type(), "t100");
            assert_eq!(presence.ip_address(), "192.168.1.160");
            assert_eq!(presence.name(), "Living Room Motion");
            assert_eq!(presence.window_s(), 1800);
        }
    }

    #[test]
    fn t100_without_hub_type_is_rejected() {
        let error = parse(&t100_yaml(""))
            .expect_err("hub_type is required")
            .to_string();

        assert!(
            error.contains("missing configuration field") && error.ends_with("hub_type\""),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn t100_with_an_unknown_hub_type_is_rejected() {
        // The hub is named by its handler, not its model.
        for hub_type in ["h100", "camera"] {
            let error = parse(&t100_yaml(&format!("        hub_type: {hub_type}")))
                .expect_err("the hub type does not exist")
                .to_string();

            assert!(
                error.contains("unknown variant"),
                "hub_type {hub_type}: {error}"
            );
        }
    }

    #[test]
    fn presence_in_the_old_shape_is_rejected() {
        // The keys of the release before the presence block: the camera's address and the
        // window on the thermostat. Each has to fail rather than be ignored.
        for key in ["camera_ip_address: 192.168.1.154", "presence_window_s: 900"] {
            let yaml = yaml_with_presence(&format!("{PRESENCE_YAML}\n      {key}"));

            let error = parse(&yaml)
                .expect_err("unknown keys are rejected")
                .to_string();
            assert!(error.contains("unknown field"), "{key}: {error}");
        }
    }

    #[test]
    fn thermostat_without_presence_is_rejected() {
        let error = parse(&yaml_with_presence(""))
            .expect_err("presence is required")
            .to_string();

        assert!(
            error.contains("missing configuration field")
                && error.ends_with("presence\"")
                && !error.contains("require_presence"),
            "unexpected error: {error}"
        );
    }

    /// A valid thermostat with one heater, whose keys after its address are `keys`: lines
    /// indented by ten spaces, or nothing.
    fn yaml_with_heater(keys: &str) -> String {
        yaml_with_presence(&format!(
            "{PRESENCE_YAML}
      heaters:
        - name: Office Heater
          ip_address: 192.168.1.115
{keys}"
        ))
    }

    const FOLLOW_UP_YAML: &str = "          follow_up:
            device_type: dimplex_remo
            enabled: true
            bluetooth_address: \"00:A0:50:41:9A:26\"
            temperature: 25";

    #[test]
    fn heater_without_follow_up_has_none() {
        let settings = parse(&yaml_with_heater("")).unwrap();
        settings.validate().unwrap();

        let heater = &settings.heating.unwrap().thermostats[0].heaters[0];
        assert!(heater.follow_up.is_none());
        assert!(heater.enabled_follow_up().is_none());
    }

    #[test]
    fn follow_up_is_read_from_yaml() {
        let settings = parse(&yaml_with_heater(FOLLOW_UP_YAML)).unwrap();
        settings.validate().unwrap();

        let heater = &settings.heating.unwrap().thermostats[0].heaters[0];
        let Some(FollowUp::DimplexRemo {
            enabled,
            bluetooth_address,
            temperature,
        }) = heater.enabled_follow_up()
        else {
            panic!("unexpected follow-up: {:?}", heater.follow_up);
        };

        assert!(enabled);
        assert_eq!(bluetooth_address, "00:A0:50:41:9A:26");
        assert_eq!(*temperature, 25);
    }

    #[test]
    fn disabled_follow_up_is_kept_but_not_enabled() {
        let yaml = yaml_with_heater(&FOLLOW_UP_YAML.replace("enabled: true", "enabled: false"));

        let settings = parse(&yaml).unwrap();
        settings.validate().unwrap();

        let heater = &settings.heating.unwrap().thermostats[0].heaters[0];
        assert!(heater.follow_up.is_some());
        assert!(heater.enabled_follow_up().is_none());
    }

    #[test]
    fn follow_up_without_a_key_is_rejected() {
        for key in ["enabled", "bluetooth_address", "temperature"] {
            let keys: Vec<&str> = FOLLOW_UP_YAML
                .lines()
                .filter(|line| !line.trim_start().starts_with(key))
                .collect();

            let error = parse(&yaml_with_heater(&keys.join("\n")))
                .expect_err("the key is required")
                .to_string();
            assert!(
                error.contains("missing configuration field")
                    && error.ends_with(&format!("{key}\"")),
                "{key}: {error}"
            );
        }
    }

    #[test]
    fn unknown_key_in_follow_up_is_rejected() {
        let yaml = yaml_with_heater(&format!("{FOLLOW_UP_YAML}\n            passkey: 379085"));

        let error = parse(&yaml).expect_err("the passkey is derived");
        assert!(
            error.to_string().contains("unknown field"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn unknown_follow_up_device_type_is_rejected() {
        let yaml = yaml_with_heater(&FOLLOW_UP_YAML.replace("dimplex_remo", "dimplex"));

        let error = parse(&yaml).expect_err("the device type does not exist");
        assert!(
            error.to_string().contains("unknown variant"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn follow_up_temperature_outside_a_byte_is_rejected() {
        // The setpoint is one byte of the frame written to the heater.
        for temperature in ["-1", "256", "21.5"] {
            let yaml = yaml_with_heater(
                &FOLLOW_UP_YAML.replace("temperature: 25", &format!("temperature: {temperature}")),
            );

            assert!(parse(&yaml).is_err(), "temperature {temperature}");
        }
    }

    const YAML_BASE: &str = r#"
api: { host: 127.0.0.1, port: 8080 }
tapo: { username: "", password: "" }
"#;

    #[test]
    fn missing_sections_are_read_as_disabled() {
        let settings = parse(YAML_BASE).unwrap();
        settings.validate().unwrap();

        assert!(settings.mqtt.is_none());
        assert!(settings.enabled_device_usage().is_none());
        assert!(settings.enabled_heating().is_none());
    }

    #[test]
    fn section_without_enabled_is_rejected() {
        for section in [
            "device_usage: { refresh_rate_s: 600, publish_topic: devices/tapo, devices: [] }",
            "heating: { sensors_subscribe_topic: \"sensors/#\", decisions_publish_topic: \
             thermostats/decisions, thermostats: [] }",
        ] {
            let error = parse(&format!("{YAML_BASE}{section}\n"))
                .expect_err("enabled is required")
                .to_string();

            assert!(error.contains("enabled"), "unexpected error: {error}");
        }
    }

    #[test]
    fn heating_without_decisions_publish_topic_is_rejected() {
        let yaml = format!(
            "{YAML_BASE}heating: {{ enabled: false, sensors_subscribe_topic: \"sensors/#\", \
             thermostats: [] }}\n"
        );

        let error = parse(&yaml)
            .expect_err("decisions_publish_topic is required")
            .to_string();
        assert!(
            error.contains("decisions_publish_topic"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn heating_without_thermostats_is_rejected() {
        let yaml = format!(
            "{YAML_BASE}heating: {{ enabled: false, sensors_subscribe_topic: \"sensors/#\", \
             decisions_publish_topic: thermostats/decisions }}\n"
        );

        let error = parse(&yaml)
            .expect_err("thermostats is required")
            .to_string();
        assert!(error.contains("thermostats"), "unexpected error: {error}");
    }

    #[test]
    fn settings_in_the_old_shape_are_rejected() {
        // The keys of the release before the sections: the polling rate with the credentials,
        // the usage topic and the sensors with the broker, and the devices and thermostats at
        // the top. Each has to fail rather than be ignored.
        for yaml in [
            "api: { host: 127.0.0.1, port: 8080 }\n\
             tapo: { username: \"\", password: \"\", refresh_rate_s: 600 }\n",
            "api: { host: 127.0.0.1, port: 8080 }\n\
             tapo: { username: \"\", password: \"\" }\n\
             mqtt: { address: tcp://localhost:1883, topic_name: devices/tapo }\n",
            "api: { host: 127.0.0.1, port: 8080 }\n\
             tapo: { username: \"\", password: \"\" }\n\
             mqtt:\n  address: tcp://localhost:1883\n  \
             sensors: { enabled: true, topic_name: \"sensors/#\" }\n",
            "api: { host: 127.0.0.1, port: 8080 }\n\
             tapo: { username: \"\", password: \"\" }\n\
             devices: []\n",
            "api: { host: 127.0.0.1, port: 8080 }\n\
             tapo: { username: \"\", password: \"\" }\n\
             thermostats: []\n",
        ] {
            let error = parse(yaml)
                .expect_err("unknown keys are rejected")
                .to_string();

            assert!(
                error.contains("unknown field"),
                "unexpected error: {error}\n{yaml}"
            );
        }
    }

    #[test]
    fn topic_keys_without_their_direction_are_rejected() {
        // The topic keys of the release before they were named for whether they are published
        // on or subscribed to. Each has to fail rather than be ignored.
        for section in [
            "device_usage: { enabled: false, refresh_rate_s: 600, publish_topic: devices/tapo, \
             topic_name: devices/tapo, devices: [] }",
            "heating: { enabled: false, sensors_subscribe_topic: \"sensors/#\", \
             decisions_publish_topic: thermostats/decisions, sensors_topic_name: \"sensors/#\", \
             thermostats: [] }",
        ] {
            let error = parse(&format!("{YAML_BASE}{section}\n"))
                .expect_err("unknown keys are rejected")
                .to_string();

            assert!(
                error.contains("unknown field"),
                "unexpected error: {error}\n{section}"
            );
        }
    }

    #[test]
    fn active_period_includes_the_start_and_excludes_the_end() {
        let period = period("08:00", "22:00");

        assert!(!period.contains(time("07:59")));
        assert!(period.contains(time("08:00")));
        assert!(period.contains(time("21:59")));
        assert!(!period.contains(time("22:00")));
        assert!(!period.contains(time("03:00")));
    }

    #[test]
    fn active_period_crossing_midnight() {
        let period = period("22:00", "06:00");

        assert!(period.contains(time("22:00")));
        assert!(period.contains(time("23:59")));
        assert!(period.contains(time("00:00")));
        assert!(period.contains(time("05:59")));
        assert!(!period.contains(time("06:00")));
        assert!(!period.contains(time("12:00")));
    }

    #[test]
    fn active_period_is_looked_up_by_the_local_time() {
        let mut thermostat = thermostat("Living Room");
        thermostat.active_periods = vec![period("05:45", "07:00"), period("07:00", "22:00")];
        thermostat.active_periods[0].target_temperature = 21.0;

        let target = |h, min| {
            thermostat
                .active_period(london(2026, 10, 26, h, min))
                .map(|period| period.target_temperature)
        };

        assert_eq!(target(5, 44), None);
        assert_eq!(target(5, 45), Some(21.0));
        assert_eq!(target(6, 59), Some(21.0));
        assert_eq!(
            target(7, 0),
            Some(22.0),
            "the second period at exactly 07:00"
        );
        assert_eq!(target(21, 59), Some(22.0));
        assert_eq!(target(22, 0), None);
        assert_eq!(target(3, 0), None);
    }

    #[test]
    fn active_period_follows_the_local_clock_through_daylight_saving() {
        let thermostat = thermostat("Living Room");
        let utc = |s: &str| DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc);

        // British Summer Time: 07:30 UTC is 08:30 in London.
        assert!(
            thermostat
                .active_period(utc("2026-07-01T07:30:00Z"))
                .is_some()
        );
        // Greenwich Mean Time: 07:30 UTC is 07:30 in London.
        assert!(
            thermostat
                .active_period(utc("2026-12-01T07:30:00Z"))
                .is_none()
        );
    }

    #[test]
    fn time_is_parsed_from_hh_mm() {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(deserialize_with = "deserialize_time")]
            time: NaiveTime,
        }

        let wrapper: Wrapper = serde_json::from_str(r#"{"time": "08:05"}"#).unwrap();
        assert_eq!(wrapper.time, NaiveTime::from_hms_opt(8, 5, 0).unwrap());

        for invalid in ["8am", "08:00:00", "24:00", ""] {
            let result = serde_json::from_str::<Wrapper>(&format!(r#"{{"time": "{invalid}"}}"#));
            assert!(result.is_err(), "'{invalid}' should be rejected");
        }
    }
}
