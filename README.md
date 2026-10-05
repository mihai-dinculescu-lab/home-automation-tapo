# Home Automation - Tapo

[![CI][ci_badge]][ci]
[![license][license_badge]][license]

Reads the `device_usage` of multiple devices and sends the data through MQTT.
It also includes an API that can control the devices.

Actor System consisting of:

- Coordinator Actor - makes sure that everything is running as expected
- Device Actor - reads the device usage and sends it to the MQTT Actor
- MQTT Actor - publishes the data to the MQTT broker
- API Actor - REST API for turning devices on/off and getting their status

## Usage

Building requires [CMake](https://cmake.org/) and OpenSSL, which are needed by the MQTT client.

Rename and update `settings.sample.yaml` to `settings.yaml`.

```bash
cargo run
```

## API

The API listens on the `api.host` and `api.port` from `settings.yaml`.

| Method | Path            | JSON body                                              | Description                       |
| ------ | --------------- | ------------------------------------------------------ | --------------------------------- |
| `GET`  | `/health-check` |                                                        | Returns `200 OK` if the API is up |
| `GET`  | `/device`       | `{ "ip_address": "192.168.1.100" }`                    | Returns whether the device is on  |
| `POST` | `/device`       | `{ "ip_address": "192.168.1.100", "device_on": true }` | Turns the device on or off        |

## Telemetry

Logs are written to stdout. Traces are exported through OTLP (gRPC) when `telemetry.otlp_endpoint` is set in `settings.yaml`.

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
