use std::collections::HashSet;
use std::time::Duration;

use anyhow::ensure;
use serde::Deserialize;
use tapo::ApiClient;

/// The longest window the camera's detection search accepts.
const MAX_PRESENCE_WINDOW_S: u64 = 24 * 60 * 60;

/// A sensor silent for a day is broken, not slow. The bound also keeps the age arithmetic in the
/// decision well inside the range `chrono::Duration` accepts.
const MAX_READING_MAX_AGE_S: u64 = 24 * 60 * 60;

/// Bound on each request to a device. The devices are on the local network, so a request that
/// takes longer than this has failed; tapo's default of 30 seconds would let a single unreachable
/// device hold up a thermostat tick or the shutdown for far longer than either can afford.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Deserialize)]
pub struct Api {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Tapo {
    pub username: String,
    pub password: String,
    pub refresh_rate_s: u64,
}

impl Tapo {
    /// A client for these credentials with the request timeout applied. Every device call goes
    /// through here so that the timeout is set in one place.
    pub fn client(&self) -> ApiClient {
        ApiClient::new(self.username.clone(), self.password.clone()).with_timeout(REQUEST_TIMEOUT)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Mqtt {
    pub address: String,
    pub topic_name: String,
    /// Optional; the sensor readings are only read when present and enabled.
    pub sensors: Option<Sensors>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Sensors {
    pub enabled: bool,
    /// Topic filter the sensor readings are received on, e.g. `sensors/#`.
    pub topic_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Device {
    pub ip_address: String,
    pub name: String,
    pub record_time_usage: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Heater {
    pub name: String,
    pub ip_address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Thermostat {
    pub name: String,
    /// A disabled thermostat is not started and leaves its heaters alone.
    pub enabled: bool,
    /// Camera whose person detections decide presence.
    pub camera_ip_address: String,
    /// Matches the `location` field of the sensor payload.
    pub sensor_location: String,
    /// Plugs switched together. Optional; without any, the decision is only logged.
    #[serde(default)]
    pub heaters: Vec<Heater>,
    /// Target temperature in °C.
    pub target_temperature: f64,
    /// Half-width of the band around the target in which the heaters are left as they are.
    pub deadband: f64,
    /// A person detected this many seconds ago still counts as present. At most 24 hours.
    pub presence_window_s: u64,
    /// A reading older than this many seconds is treated as missing.
    pub reading_max_age_s: u64,
    /// How often the thermostat is evaluated.
    pub interval_s: u64,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub api: Api,
    pub tapo: Tapo,
    pub mqtt: Mqtt,
    pub devices: Vec<Device>,
    /// Optional; an empty list disables the thermostat.
    #[serde(default)]
    pub thermostats: Vec<Thermostat>,
}

impl Settings {
    pub fn new() -> Result<Self, anyhow::Error> {
        let base_path = std::env::current_dir().expect("failed to determine the current directory");

        let mut builder = config::Config::builder();

        let config_path = base_path.join("settings.yaml");
        builder = builder.add_source(config::File::new(
            config_path.to_str().expect("this should never happen"),
            config::FileFormat::Yaml,
        ));

        let settings = builder.build()?.try_deserialize::<Self>()?;
        settings.validate()?;

        Ok(settings)
    }

    /// Rejects values that deserialize fine but would fail later, where the cause is harder to
    /// trace back to the settings: a zero interval panics the actor's tick loop, a presence
    /// window shorter than the interval misses anyone detected early in it and an empty one
    /// never sees anyone, a presence window over 24 hours is refused by the camera on every
    /// tick, a zero reading age makes every reading stale and an oversized one overflows the
    /// age arithmetic, a negative deadband inverts the rules, a non-finite temperature makes
    /// every comparison false, a duplicate key makes the coordinator skip an entry, and a
    /// thermostat without the sensors would never see a reading.
    fn validate(&self) -> Result<(), anyhow::Error> {
        ensure!(
            self.tapo.refresh_rate_s > 0,
            "tapo.refresh_rate_s must be greater than 0"
        );

        let mut ip_addresses = HashSet::new();
        for device in &self.devices {
            ensure!(
                ip_addresses.insert(&device.ip_address),
                "Duplicate device ip_address '{}'",
                device.ip_address
            );
        }

        let sensors_enabled = self.mqtt.sensors.as_ref().is_some_and(|s| s.enabled);

        let mut names = HashSet::new();
        for thermostat in &self.thermostats {
            let name = &thermostat.name;

            ensure!(names.insert(name), "Duplicate thermostat name '{name}'");
            ensure!(
                thermostat.interval_s > 0,
                "Thermostat '{name}': interval_s must be greater than 0"
            );
            ensure!(
                thermostat.presence_window_s >= thermostat.interval_s,
                "Thermostat '{name}': presence_window_s must be at least interval_s, otherwise a \
                 person detected between two ticks can go unnoticed"
            );
            ensure!(
                thermostat.presence_window_s <= MAX_PRESENCE_WINDOW_S,
                "Thermostat '{name}': presence_window_s must be at most {MAX_PRESENCE_WINDOW_S} \
                 (24 hours), the longest window the camera accepts"
            );
            ensure!(
                thermostat.reading_max_age_s > 0,
                "Thermostat '{name}': reading_max_age_s must be greater than 0"
            );
            ensure!(
                thermostat.reading_max_age_s <= MAX_READING_MAX_AGE_S,
                "Thermostat '{name}': reading_max_age_s must be at most {MAX_READING_MAX_AGE_S} \
                 (24 hours)"
            );
            ensure!(
                thermostat.target_temperature.is_finite(),
                "Thermostat '{name}': target_temperature must be a finite number"
            );
            ensure!(
                thermostat.deadband.is_finite(),
                "Thermostat '{name}': deadband must be a finite number"
            );
            ensure!(
                thermostat.deadband >= 0.0,
                "Thermostat '{name}': deadband must not be negative"
            );
            ensure!(
                !thermostat.enabled || sensors_enabled,
                "Thermostat '{name}' is enabled but mqtt.sensors is not, so it would never see \
                 a reading"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thermostat(name: &str) -> Thermostat {
        Thermostat {
            name: name.to_string(),
            enabled: true,
            camera_ip_address: "192.168.1.154".to_string(),
            sensor_location: "Living Room".to_string(),
            heaters: vec![],
            target_temperature: 22.0,
            deadband: 0.25,
            presence_window_s: 900,
            reading_max_age_s: 900,
            interval_s: 300,
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
                refresh_rate_s: 600,
            },
            mqtt: Mqtt {
                address: "tcp://localhost:1883".to_string(),
                topic_name: "devices/tapo".to_string(),
                sensors: Some(Sensors {
                    enabled: true,
                    topic_name: "sensors/#".to_string(),
                }),
            },
            devices: vec![],
            thermostats: vec![thermostat("Living Room")],
        }
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
        settings.tapo.refresh_rate_s = 0;

        assert!(error(&settings).contains("refresh_rate_s"));
    }

    #[test]
    fn duplicate_device_ip_address_is_rejected() {
        let device = Device {
            ip_address: "192.168.1.100".to_string(),
            name: "Plug".to_string(),
            record_time_usage: false,
        };
        let mut settings = settings();
        settings.devices = vec![device.clone(), device];

        assert!(error(&settings).contains("192.168.1.100"));
    }

    #[test]
    fn duplicate_thermostat_name_is_rejected() {
        let mut settings = settings();
        settings.thermostats.push(thermostat("Living Room"));

        assert!(error(&settings).contains("Duplicate thermostat name"));
    }

    #[test]
    fn zero_interval_is_rejected() {
        let mut settings = settings();
        settings.thermostats[0].interval_s = 0;

        assert!(error(&settings).contains("interval_s"));
    }

    #[test]
    fn presence_window_shorter_than_interval_is_rejected() {
        for presence_window_s in [0, 299] {
            let mut settings = settings();
            settings.thermostats[0].presence_window_s = presence_window_s;

            assert!(
                error(&settings).contains("presence_window_s must be at least interval_s"),
                "presence_window_s {presence_window_s}"
            );
        }
    }

    #[test]
    fn presence_window_equal_to_interval_passes() {
        let mut settings = settings();
        settings.thermostats[0].presence_window_s = 300;

        settings.validate().unwrap();
    }

    #[test]
    fn presence_window_over_a_day_is_rejected() {
        let mut settings = settings();
        settings.thermostats[0].presence_window_s = MAX_PRESENCE_WINDOW_S + 1;

        assert!(error(&settings).contains("presence_window_s"));
    }

    #[test]
    fn zero_reading_max_age_is_rejected() {
        let mut settings = settings();
        settings.thermostats[0].reading_max_age_s = 0;

        assert!(error(&settings).contains("reading_max_age_s must be greater than 0"));
    }

    #[test]
    fn reading_max_age_over_a_day_is_rejected() {
        for reading_max_age_s in [MAX_READING_MAX_AGE_S + 1, u64::MAX] {
            let mut settings = settings();
            settings.thermostats[0].reading_max_age_s = reading_max_age_s;

            assert!(
                error(&settings).contains("reading_max_age_s must be at most"),
                "reading_max_age_s {reading_max_age_s}"
            );
        }
    }

    #[test]
    fn non_finite_target_temperature_is_rejected() {
        for target_temperature in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut settings = settings();
            settings.thermostats[0].target_temperature = target_temperature;

            assert!(
                error(&settings).contains("target_temperature must be a finite number"),
                "target_temperature {target_temperature}"
            );
        }
    }

    #[test]
    fn non_finite_deadband_is_rejected() {
        for deadband in [f64::NAN, f64::INFINITY] {
            let mut settings = settings();
            settings.thermostats[0].deadband = deadband;

            assert!(
                error(&settings).contains("deadband must be a finite number"),
                "deadband {deadband}"
            );
        }
    }

    #[test]
    fn negative_deadband_is_rejected() {
        let mut settings = settings();
        settings.thermostats[0].deadband = -0.25;

        assert!(error(&settings).contains("deadband"));
    }

    #[test]
    fn enabled_thermostat_without_sensors_is_rejected() {
        for sensors in [
            None,
            Some(Sensors {
                enabled: false,
                topic_name: "sensors/#".to_string(),
            }),
        ] {
            let mut settings = settings();
            settings.mqtt.sensors = sensors;

            assert!(error(&settings).contains("mqtt.sensors"));
        }
    }

    #[test]
    fn disabled_thermostat_without_sensors_passes() {
        let mut settings = settings();
        settings.mqtt.sensors = None;
        settings.thermostats[0].enabled = false;

        settings.validate().unwrap();
    }
}
