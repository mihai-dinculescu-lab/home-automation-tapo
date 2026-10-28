# Home Automation - Tapo

[![CI][ci_badge]][ci]
[![license][license_badge]][license]

Reads the `device_usage` of multiple devices and sends the data through MQTT.
It also includes an API that can control the devices and a thermostat that switches heater plugs
based on presence and temperature.

Actor System consisting of:

- Coordinator Actor - makes sure that everything is running as expected
- Device Usage Actor - reads the device usage and sends it to the Coordinator Actor, which forwards it to the MQTT Actor (only when `device_usage` is enabled)
- Device Session Actor - one per device, runs every request to it on a single session, one at a time
- MQTT Actor - publishes the device usage and the thermostat decisions to the MQTT broker (only when `device_usage` or `heating` is enabled)
- MQTT Sensors Actor - subscribes to the sensor readings on the MQTT broker and keeps the latest one per location (only when `heating` and at least one of its thermostats are enabled)
- Thermostat Actor - decides whether a room should be heated, switches the heater plugs and sends the decision to the Coordinator Actor, which forwards it to the MQTT Actor (only when `heating` and the thermostat are enabled)
- API Actor - REST API for turning devices on/off and getting their status

## Usage

Building requires [CMake](https://cmake.org/) and OpenSSL, which are needed by the MQTT client.

Rename and update `settings.sample.yaml` to `settings.yaml`.

The API always runs. The usage polling and the heating are optional sections, `device_usage` and `heating`, each
switched by its own `enabled`, so a feature can be turned off without removing the rest of its settings; a missing
section is the same as a disabled one. `mqtt` is only required while one of them is enabled. Every section is
validated whether it is enabled or not, and a key the service does not know is rejected, so a settings file in the
shape of an earlier release fails at startup rather than running with a feature dropped.

```bash
cargo run
```

## API

The API listens on the `api.host` and `api.port` from `settings.yaml`.

| Method | Path            | JSON body                                                                           | Description                                                                       |
| ------ | --------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `GET`  | `/health-check` |                                                                                     | Returns `200 OK` if the coordinator answers within a second, `503` if it does not |
| `GET`  | `/device`       | `{ "ip_address": "192.168.1.100", "device_type": "plug" }`                          | Returns the `state` of the device                                                 |
| `POST` | `/device`       | `{ "ip_address": "192.168.1.100", "device_type": "plug", "state": { "on": true } }` | Turns the device on or off                                                        |

Both `/device` endpoints respond with `{ "ip_address": "192.168.1.100", "state": { "on": true } }`. A `POST`
without `state.on` changes nothing and reports the state the device is in.

`device_type` selects the Tapo handler used to talk to the device.

Every device has a single session, shared by the API, the usage polling and the thermostat, and one request runs
on it at a time: a Tapo device keeps one session, and a login while another request is in progress fails both.
The session is kept between requests. One that the device no longer accepts, because it expired, is reopened and
the request retried once; any other failure drops it, so that the next request logs in afresh. A request that has
started is finished even if its caller gives up, so the device is never left mid-request. A session is only reused
by requests with the same `device_type`, so a device that the service also polls is best addressed as
`plug_energy_monitoring`.

| `device_type`            | Devices          |
| ------------------------ | ---------------- |
| `light`                  | L510, L520, L610 |
| `color_light`            | L530, L535, L630 |
| `rgb_light_strip`        | L900             |
| `rgbic_light_strip`      | L920, L930       |
| `plug`                   | P100, P105       |
| `plug_energy_monitoring` | P110, P115       |

## Sensors

While `heating` and at least one of its thermostats are enabled, the service subscribes to the MQTT topic filter
`heating.sensors_subscribe_topic` (e.g. `sensors/#`) and keeps the latest reading per `location` from JSON payloads
with at least `{"location": "Living Room", "temperature": 21.4}`. The messages are not retained, so readings are only
known from the moment the service subscribes.

## Thermostat

Each enabled entry in `heating.thermostats` is evaluated every `interval_s` seconds under the rule of the active period
containing the current time. The heaters are turned **on** when a person was detected by the camera in the last
`presence_window_s` seconds and the latest sensor reading for `sensor_location` is under the period's
`target_temperature - deadband`. They are turned **off** when nobody was detected, the camera cannot be queried,
the reading is missing or older than `reading_max_age_s` seconds, or the temperature is above
`target_temperature + deadband`. In between, the last command is repeated, so that they do not cycle around the
setpoint; right after a start there is no last command and they are turned off. A period with
`require_presence: false` skips the camera and heats the room on the reading alone, e.g. before anyone is up.

Every tick commands the heaters, so a plug switched by hand is corrected within one interval. Nothing happens on
shutdown: the next start asserts the heaters on its first tick.

The readings are not retained, so none is known right after a start and the heaters are turned off until the first
one arrives. This also covers plugs left on by a run that crashed.

The readings are the ones kept by the [Sensors](#sensors) subscription, matched on `sensor_location`. Person
detection is read from the camera's SD card through `get_detections`, so it has to be enabled in the Tapo app, and
`presence_window_s` is capped at 24 hours by the camera. It also has to be at least `interval_s`, otherwise a person
detected between two ticks can go unnoticed.

`active_periods` lists the parts of the day in which a thermostat runs, each from `start` (inclusive) to `end`
(exclusive) as `HH:MM` in the thermostat's `time_zone` (an IANA name such as `Europe/London`); an `end` before
`start` crosses midnight. Each period carries its own `target_temperature` and `require_presence`, so a morning
period can warm the room before anyone is up and the day period can require someone to be there. Outside every
period nothing is queried and the heaters are switched off on every tick, which also covers a start outside the
schedule and plugs left on by a run that crashed. The list needs at least one entry; a thermostat that heats all
day tiles it with abutting periods. The periods may abut but not overlap, each has to last at least `interval_s`,
and so does any gap between two of them, otherwise a period or a gap can pass between two ticks. These lengths are
checked on the local clock, so on the two daylight saving nights a period or gap inside the skipped hour gets no
tick and one inside the repeated hour runs twice; keep the boundaries away from that hour.

`heating.thermostats` needs at least one entry. `heating.enabled: false` switches every thermostat off, and a single
entry can be switched off with its own `enabled: false`; either leaves the heaters alone.

### Decisions

Every tick publishes its decision on `heating.decisions_publish_topic` once it has switched the heaters, one message per
tick for every thermostat, told apart by `thermostat_name`. This is not optional: the stream is what an alert on a
stopped thermostat or an undeliverable command is built on.

```json
{
  "thermostat_name": "Living Room",
  "decided_at": "2026-10-28T14:00:00.123456789Z",
  "command": "on",
  "reason": "below_target",
  "active_period": { "start": "07:00", "end": "22:00", "target_temperature": 21.0, "require_presence": true },
  "person_detected": true,
  "temperature": 20.1,
  "reading_age_s": 42,
  "heaters": [{ "name": "Radiator", "outcome": "switched" }]
}
```

- `reason` is one of `outside_active_period`, `camera_error`, `nobody_present`, `no_reading`, `stale_reading`,
  `above_target`, `below_target` and `within_deadband`.
- `active_period` is the period the tick ran under, with its times in the thermostat's `time_zone`; it is `null`
  outside every period, and so is every input, since nothing is queried there.
- `person_detected` is `null` when the period does not require presence or the camera could not be queried.
- `temperature` and `reading_age_s` are `null` when there is no reading for `sensor_location`.
- `heaters` lists every heater in the order of the settings with the `outcome` of its switch: `switched`, `failed`, or
  `shutting_down` when the service was stopping and the command was not sent.

A tick cancelled by the next one publishes nothing, so a gap of more than one `interval_s` between two messages of the
same thermostat means its ticks are not completing. The messages are not retained, and while the broker is
unreachable they are buffered together with the device usage, oldest dropped first, so `decided_at` is the time to
go by rather than the time of arrival.

### Safety properties

The thermostat is held to the guarantees below. Each is a bound that can be checked, not a mechanism. Two are not
fully met yet and are marked as such.

1. **A tick every `interval_s`.** The tick loop never waits on the work it starts, and each tick cancels the previous
   one if it is still running, so a slow camera or plug can neither block the next evaluation nor switch the heaters
   on older inputs. Every request to a device times out after 10 seconds, so even a tick that has to log in to a
   slow plug again ends within about a minute.
2. **A stuck process is replaced within about a minute.** `/health-check` round-trips a probe to the coordinator actor
   on the thread every actor runs on, so a blocked thread or a stopped coordinator fails the Kubernetes liveness probe,
   which restarts the pod. A thermostat actor that has died is recreated by the coordinator within 60 seconds.
3. **Within one interval of a period ending, the heaters follow the next period's rule, or Off in a gap.** Every
   tick outside the schedule switches them off, so a plug that could not be switched is tried again on the next one.
   Every period and every gap between two of them is validated to last at least `interval_s`, so a tick always
   lands in each.
4. **Every start asserts Off within seconds, unless fresh inputs say On.** The first tick runs as soon as the actor
   starts. Outside every period it switches the heaters off; inside one, no reading and no last command are known yet,
   either of which decides Off. This covers plugs left on by a run that crashed, was killed or was stopped.
5. **Unknown decides Off.** A camera that cannot be queried, nobody present, a missing reading and a stale reading all
   switch off. On requires a fresh reading below the band, and a person present in any period that requires it. A
   period with `require_presence: false` is an explicit, logged opt-out, so property 12 still holds.
6. **Exposure is bounded once a trigger disappears.** After a person leaves or a sensor falls silent, the heaters are
   off within `presence_window_s` or `reading_max_age_s` plus one interval. Periods that do not require presence
   are not bounded by the presence window: the heaters can run on the reading alone until the next period that
   requires presence or the next gap, plus one interval. A schedule in which no period requires presence is bounded
   by `reading_max_age_s` only.
7. **No heater stays on without a fresh On, even with the service gone.** Not met yet. Every safeguard above runs in
   the service, so a pod that is gone, cut off from the network or unable to reach a plug leaves the plug as it was.
   The plug's own countdown timer, armed on every On, would close this.
8. **An undeliverable command or a stopped decision stream raises an alert.** Partly met. A plug that rejects every
   command, or a thermostat whose ticks never complete, looks healthy to Kubernetes, so every completed tick publishes
   its [decision](#decisions) with the outcome of each switch. The alert itself lives with whatever consumes the
   topic: on a heater whose `outcome` is `failed`, and on a thermostat with no message for more than about two
   intervals. `shutting_down` is left out, since a tick that overlaps a deploy reports it.
9. **One controller at a time.** The deployment runs a single replica with the `Recreate` strategy, so a deploy
   stops the old pod before the new one starts and the two never log in to the same plug at once. A local run against
   the same plugs is the one case this cannot prevent; keep `heating.enabled: false` in a local settings file.
10. **A manual switch is reconciled within one interval.** Every tick commands the heaters: the decision inside the
    period, Off outside it, and the last command within the deadband. A plug switched by hand is therefore corrected
    on the next tick wherever the temperature sits.
11. **Reaching a safe state after a restart depends on nothing outside the LAN.** The settings come from a secret
    already mounted, and MQTT or the camera being down decides Off. The image is still pulled on every restart, so a
    registry outage can keep a replacement pod from starting.
12. **Configuration cannot silently disable safety.** The settings are validated at startup and the service refuses to
    start on a zero interval, a presence window shorter than the interval, a period or a gap between two of them
    shorter than the interval, overlapping periods, an empty list of periods or thermostats, a key it does not know
    anywhere in the settings, a non-finite temperature or deadband, an enabled feature without `mqtt`, or an empty
    topic.

## Telemetry

Logs are written to stdout. Traces are exported through OTLP over HTTP/protobuf when `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g. `http://localhost:4318`) or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set.
The standard `OTEL_*` environment variables are honoured. `service.name` defaults to `home-automation-tapo`.
Only HTTP/protobuf is supported, so point the endpoint at the collector's OTLP HTTP port (usually `4318`), not the gRPC one (`4317`). `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` is not honoured.
Extra resource attributes can be set through `OTEL_RESOURCE_ATTRIBUTES`, e.g. `service.namespace=home-automation,deployment.environment.name=production`.

## Docker

### linux/amd64 & linux/arm64

The image does not include a usable `settings.yaml`, so one has to be mounted at `/app/settings.yaml`.
The published port has to match the `api.port` from `settings.yaml`.

A prebuilt image is published to the GitHub Container Registry.

```bash
docker run -d -p 80:80 -v ./settings.yaml:/app/settings.yaml ghcr.io/mihai-dinculescu-lab/home-automation-tapo:main
```

Alternatively, build it locally.

```bash
docker build -t home-automation-tapo .
docker run -d -p 80:80 -v ./settings.yaml:/app/settings.yaml home-automation-tapo
```

## Kubernetes

The manifests in `kubernetes/` are applied by CI on every push to `main`.

```bash
kubectl apply -k kubernetes/
```


[ci_badge]: https://github.com/mihai-dinculescu-lab/home-automation-tapo/actions/workflows/ci.yml/badge.svg?branch=main
[ci]: https://github.com/mihai-dinculescu-lab/home-automation-tapo/actions
[license_badge]: https://img.shields.io/github/license/mihai-dinculescu-lab/home-automation-tapo
[license]: https://github.com/mihai-dinculescu-lab/home-automation-tapo/blob/main/LICENSE
