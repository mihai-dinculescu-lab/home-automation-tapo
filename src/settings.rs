use std::collections::HashSet;
use std::time::Duration;

use anyhow::ensure;
use chrono::{DateTime, NaiveTime, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Deserializer};
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

/// Unknown keys are rejected, as on `Thermostat`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Heater {
    pub name: String,
    pub ip_address: String,
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
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivePeriod {
    /// Inclusive, as `HH:MM` in the thermostat's `time_zone`.
    #[serde(deserialize_with = "deserialize_time")]
    pub start: NaiveTime,
    /// Exclusive, as `HH:MM` in the thermostat's `time_zone`. An end before the start crosses
    /// midnight.
    #[serde(deserialize_with = "deserialize_time")]
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

/// Unknown keys are rejected so that a settings file in the shape of an earlier release, which
/// kept `active_period` and `target_temperature` here, fails at startup instead of being read
/// with those silently ignored.
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
    /// Camera whose person detections decide presence.
    pub camera_ip_address: String,
    /// Matches the `location` field of the sensor payload.
    pub sensor_location: String,
    /// Plugs switched together. Optional; without any, the decision is only logged.
    #[serde(default)]
    pub heaters: Vec<Heater>,
    /// Half-width of the band around the target in which the last command is repeated.
    pub deadband: f64,
    /// A person detected this many seconds ago still counts as present. At most 24 hours.
    pub presence_window_s: u64,
    /// A reading older than this many seconds is treated as missing.
    pub reading_max_age_s: u64,
    /// How often the thermostat is evaluated.
    pub interval_s: u64,
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

    /// Rejects values that deserialize fine but would fail later, where the cause is harder to
    /// trace back to the settings: a zero interval panics the actor's tick loop, a presence
    /// window shorter than the interval misses anyone detected early in it and an empty one
    /// never sees anyone, a presence window over 24 hours is refused by the camera on every
    /// tick, a zero reading age makes every reading stale and an oversized one overflows the
    /// age arithmetic, a negative deadband inverts the rules, a non-finite temperature makes
    /// every comparison false, a duplicate key makes the coordinator skip an entry, a thermostat
    /// without the sensors would never see a reading, one without a period would never heat, an
    /// active period that starts when it ends is either empty or the whole day, which the
    /// thermostat cannot tell apart, one shorter than the interval can pass between two ticks
    /// without either landing in it, two that overlap leave the rule for the overlap to the
    /// order of the list, and a gap between two periods shorter than the interval can pass
    /// between two ticks without the `Off` handover ever happening.
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
            camera_ip_address: "192.168.1.154".to_string(),
            sensor_location: "Living Room".to_string(),
            heaters: vec![],
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
            settings.thermostats[0].active_periods[0].target_temperature = target_temperature;

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
        settings.thermostats[0].active_periods = periods;
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
tapo: { username: "", password: "", refresh_rate_s: 600 }
mqtt:
  address: tcp://localhost:1883
  topic_name: devices/tapo
  sensors: { enabled: true, topic_name: "sensors/#" }
devices: []
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
    camera_ip_address: 192.168.1.154
    sensor_location: Living Room
    deadband: 0.25
    presence_window_s: 900
    reading_max_age_s: 900
    interval_s: 300
"
        );

        let settings = parse(&yaml).unwrap();
        settings.validate().unwrap();

        let thermostat = &settings.thermostats[0];
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
    camera_ip_address: 192.168.1.154
    sensor_location: Living Room
    deadband: 0.25
    presence_window_s: 900
    reading_max_age_s: 900
    interval_s: 300
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
    camera_ip_address: 192.168.1.154
    sensor_location: Living Room
    deadband: 0.25
    presence_window_s: 900
    reading_max_age_s: 900
    interval_s: 300
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
    camera_ip_address: 192.168.1.154
    sensor_location: Living Room
    target_temperature: 22.0
    deadband: 0.25
    presence_window_s: 900
    reading_max_age_s: 900
    interval_s: 300
"
        );

        let error = parse(&yaml).expect_err("unknown keys are rejected");
        assert!(
            error.to_string().contains("unknown field"),
            "unexpected error: {error}"
        );
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
