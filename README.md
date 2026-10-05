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

Rename and update `settings.sample.yaml` to `settings.yaml`.

```bash
cargo run
```

## Docker

### linux/amd64 & linux/arm64

```bash
docker build -t home-automation-tapo .
docker run -d -p 80:80 home-automation-tapo
```


[ci_badge]: https://github.com/mihai-dinculescu-lab/home-automation-tapo/actions/workflows/ci.yml/badge.svg?branch=main
[ci]: https://github.com/mihai-dinculescu-lab/home-automation-tapo/actions
[license_badge]: https://img.shields.io/github/license/mihai-dinculescu-lab/home-automation-tapo
[license]: https://github.com/mihai-dinculescu-lab/home-automation-tapo/blob/main/LICENSE
