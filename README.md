# Home Automation - Tapo

[![CI][ci_badge]][ci]
[![license][license_badge]][license]

Reads the `device_usage` of multiple devices and sends the data through MQTT.
It also includes an API that can control the devices and a thermostat that switches heater plugs
based on presence and temperature.

Actor System consisting of:

- Coordinator Actor - makes sure that everything is running as expected
- Device Actor - reads the device usage and sends it to the Coordinator Actor, which forwards it to the MQTT Actor
- MQTT Actor - publishes the data to the MQTT broker
- MQTT Sensors Actor - subscribes to the sensor readings on the MQTT broker and keeps the latest one per location (only when `mqtt.sensors` is enabled)
- Thermostat Actor - decides whether a room should be heated and switches the heater plugs
- API Actor - REST API for turning devices on/off and getting their status

## Usage

Building requires [CMake](https://cmake.org/) and OpenSSL, which are needed by the MQTT client.

Rename and update `settings.sample.yaml` to `settings.yaml`.

```bash
cargo run
```

## API

The API listens on the `api.host` and `api.port` from `settings.yaml`.

| Method | Path            | JSON body                                                                           | Description                       |
| ------ | --------------- | ----------------------------------------------------------------------------------- | --------------------------------- |
| `GET`  | `/health-check` |                                                                                     | Returns `200 OK` if the API is up |
| `GET`  | `/device`       | `{ "ip_address": "192.168.1.100", "device_type": "plug" }`                          | Returns the `state` of the device |
| `POST` | `/device`       | `{ "ip_address": "192.168.1.100", "device_type": "plug", "state": { "on": true } }` | Turns the device on or off        |

Both `/device` endpoints respond with `{ "ip_address": "192.168.1.100", "state": { "on": true } }`. A `POST`
without `state.on` changes nothing and reports the state the device is in.

`device_type` selects the Tapo handler used to talk to the device.

Every device has a single session, shared by the API, the usage polling and the thermostat, and one request runs
on it at a time: a Tapo device keeps one session, and a login while another request is in progress fails both.
The session is kept between requests. One that the device no longer accepts, because it expired or a cancelled
request left it out of step, is reopened and the request retried once; any other failure drops it, so that the
next request logs in afresh. A session is only reused by requests with the same `device_type`, so a device that
the service also polls is best addressed as `plug_energy_monitoring`.

| `device_type`            | Devices          |
| ------------------------ | ---------------- |
| `light`                  | L510, L520, L610 |
| `color_light`            | L530, L535, L630 |
| `rgb_light_strip`        | L900             |
| `rgbic_light_strip`      | L920, L930       |
| `plug`                   | P100, P105       |
| `plug_energy_monitoring` | P110, P115       |

## Sensors

When `mqtt.sensors` is present and `enabled`, the service subscribes to the MQTT topic filter `mqtt.sensors.topic_name`
(e.g. `sensors/#`) and keeps the latest reading per `location` from JSON payloads with at least
`{"location": "Living Room", "temperature": 21.4}`. The messages are not retained, so readings are only known from the
moment the service subscribes. `mqtt.sensors` is optional; leaving it out or setting `enabled: false` skips the subscription.

## Thermostat

Each enabled entry in `thermostats` is evaluated every `interval_s` seconds. The heaters are turned **on** when a
person was detected by the camera in the last `presence_window_s` seconds and the latest sensor reading for
`sensor_location` is under `target_temperature - deadband`. They are turned **off** when nobody was detected,
the camera cannot be queried, the reading is missing or older than `reading_max_age_s` seconds, or the
temperature is above `target_temperature + deadband`. In between, the heaters are left as they are so that
they do not cycle around the setpoint.

`On` and `Off` are asserted on every tick, so a plug switched by hand is corrected within one interval. On shutdown
the heaters are switched off, so that a service that does not come back cannot leave them running.

The readings are not retained, so none is known right after a start and the heaters are turned off until the first
one arrives. This also covers plugs left on by a run that crashed.

The readings are the ones kept by the [Sensors](#sensors) subscription, matched on `sensor_location`, so an enabled
thermostat requires `mqtt.sensors` to be enabled. Person detection is read from the camera's SD card through
`get_detections`, so it has to be enabled in the Tapo app, and `presence_window_s` is capped at 24 hours by the camera.
It also has to be at least `interval_s`, otherwise a person detected between two ticks can go unnoticed.

`active_period` limits a thermostat to part of the day, from `start` (inclusive) to `end` (exclusive) as `HH:MM`
in `time_zone` (an IANA name such as `Europe/London`); an `end` before `start` crosses midnight. Outside the period
nothing is queried: the first tick after the period ends switches the heaters off, the following ticks send no
command, and only a plug that could not be switched is retried on the next tick. A start outside the period
switches them off the same way, which covers plugs left on by a run that crashed. Both the period and the rest of
the day have to last at least `interval_s`, otherwise one of them can pass between two ticks. `active_period` is
optional; without it the thermostat runs around the clock.

`thermostats` is optional; leaving it out or empty disables the thermostat, and a single entry can be switched off
with `enabled: false`, which leaves its heaters alone.

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
