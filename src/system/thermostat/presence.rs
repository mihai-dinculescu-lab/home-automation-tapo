use chrono::{DateTime, Utc};
use tapo::responses::{DetectionResult, RecordingType};
use tracing::debug;

use crate::settings::{PresenceSource, Tapo};
use crate::system::thermostat::decision::{Presence, PresenceError};
use crate::telemetry::record_error;

/// Asks `source` whether anyone was present between `window_start` and `now`.
///
/// The device is only ever queried here, and a tick cancels the previous one, so it is logged
/// into on every query rather than through a session.
///
/// A failure is recorded on the caller's span and returned as unknown presence, never as nobody
/// present.
pub async fn query(
    tapo: &Tapo,
    source: &PresenceSource,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Presence {
    let result = async {
        match source {
            PresenceSource::Camera { ip_address, .. } => {
                let handler = tapo.client().c120(ip_address.clone()).await?;

                handler.get_detections(window_start, now).await
            }
            PresenceSource::CameraPtz { ip_address, .. } => {
                let handler = tapo.client().c220(ip_address.clone()).await?;

                handler.get_detections(window_start, now).await
            }
        }
    }
    .await;

    match result {
        Ok(detections) => {
            debug!("{} detections in the presence window", detections.len());
            Ok(person_present(&detections, window_start))
        }
        Err(e) => {
            record_error!(&e);
            Err(PresenceError)
        }
    }
}

/// A person counts as present when any detection has `Person` among its events and ended at or
/// after `window_start`.
pub fn person_present(detections: &[DetectionResult], window_start: DateTime<Utc>) -> bool {
    detections.iter().any(|detection| {
        detection.end_time >= window_start && detection.events.contains(&RecordingType::Person)
    })
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 25, 12, 0, 0).unwrap()
    }

    /// `DetectionResult` is not constructible outside `tapo`, so it is built from its serialized form.
    fn detection(end_time: DateTime<Utc>, events: Vec<RecordingType>) -> DetectionResult {
        serde_json::from_value(serde_json::json!({
            "start_time": (end_time - Duration::seconds(10)).to_rfc3339(),
            "end_time": end_time.to_rfc3339(),
            "trigger": events.last().unwrap(),
            "events": events,
        }))
        .unwrap()
    }

    #[test]
    fn person_present_requires_a_person_event_in_the_window() {
        let window_start = now() - Duration::seconds(900);

        assert!(person_present(
            &[detection(
                now(),
                vec![RecordingType::Motion, RecordingType::Person]
            )],
            window_start
        ));
        assert!(
            !person_present(
                &[detection(now(), vec![RecordingType::Motion])],
                window_start
            ),
            "motion alone is not a person"
        );
        assert!(
            !person_present(
                &[detection(
                    window_start - Duration::seconds(1),
                    vec![RecordingType::Person]
                )],
                window_start
            ),
            "a detection that ended before the window does not count"
        );
        assert!(!person_present(&[], window_start));
    }

    #[test]
    fn detection_ending_exactly_at_window_start_counts() {
        let window_start = now() - Duration::seconds(900);

        assert!(person_present(
            &[detection(window_start, vec![RecordingType::Person])],
            window_start
        ));
    }
}
