use std::collections::HashMap;
use std::time::Duration;

use actix::clock::sleep;
use actix::dev::ToEnvelope;
use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture,
    ResponseFuture, WrapFuture,
};
use anyhow::Context as _;
use futures_util::future::join_all;
use tracing::{Instrument, info, info_span, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::settings::{Mqtt, Settings};
use crate::system::api::api_actor::ApiActor;
use crate::system::device_usage_actor::DeviceUsageActor;
use crate::system::devices::{DeviceSessions, device_session_actor::DeviceSessionActor};
use crate::system::messages::{
    DeviceUsageMessage, GetDeviceSessionMessage, GetSensorReadingMessage, ProbeMessage,
    SensorReading, StopMessage,
};
use crate::system::mqtt_actor::MqttActor;
use crate::system::mqtt_sensors_actor::MqttSensorsActor;
use crate::system::thermostat::thermostat_actor::ThermostatActor;
use crate::telemetry::{consumer_span, record_error};

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct CoordinatorActor {
    settings: Settings,
    /// The session actor of each device that has been talked to, by IP address. A device has a
    /// single session, so every request to it goes through this one actor.
    device_session_actor_addrs: HashMap<String, Addr<DeviceSessionActor>>,
    api_actor_addr: Addr<ApiActor>,
    mqtt_actor_addr: Addr<MqttActor>,
    /// `None` when `mqtt.sensors` is missing or disabled.
    mqtt_sensors_actor_addr: Option<Addr<MqttSensorsActor>>,
    device_usage_actor_addrs: HashMap<String, Addr<DeviceUsageActor>>,
    thermostat_actor_addrs: HashMap<String, Addr<ThermostatActor>>,
    shutting_down: bool,
}

impl CoordinatorActor {
    /// Starts the coordinator and its child actors.
    #[instrument(name = "CoordinatorActor::spawn", skip_all)]
    pub fn spawn(settings: Settings) -> anyhow::Result<Addr<Self>> {
        // The actor constructors record their own errors.
        let mqtt_actor =
            MqttActor::new(settings.mqtt.clone()).context("Failed to create the MQTT actor")?;
        let mqtt_actor_addr = mqtt_actor.start();

        let mqtt_sensors_actor_addr = start_mqtt_sensors_actor(&settings.mqtt)
            .context("Failed to create the MQTT sensors actor")?;

        // The API actor probes the coordinator, and every actor that talks to a device asks it
        // for the session, so both need the address before the actor exists.
        Ok(Self::create(move |ctx| {
            let api_actor = ApiActor::new(
                settings.api.clone(),
                DeviceSessions::new(ctx.address().recipient()),
                ctx.address().recipient(),
            );
            let api_actor_addr = api_actor.start();

            Self {
                settings,
                device_session_actor_addrs: HashMap::new(),
                api_actor_addr,
                mqtt_actor_addr,
                mqtt_sensors_actor_addr,
                device_usage_actor_addrs: HashMap::new(),
                thermostat_actor_addrs: HashMap::new(),
                shutting_down: false,
            }
        }))
    }

    /// Stops the coordinator and all of its child actors, returning once they have all stopped.
    pub async fn stop(addr: Addr<Self>) {
        stop_actor(addr).await;
    }

    /// The health check: (re)creates every child actor that is missing or has stopped.
    fn tick(&mut self, ctx: &mut Context<Self>) {
        let span = info_span!("CoordinatorActor::tick");
        let _enter = span.enter();

        // don't restart the actors that are being stopped
        if self.shutting_down {
            return;
        }

        let coordinator_addr = ctx.address();

        // check api
        if !self.api_actor_addr.connected() {
            warn!("API Actor is not connected, restarting...");
            let api_actor = ApiActor::new(
                self.settings.api.clone(),
                DeviceSessions::new(coordinator_addr.clone().recipient()),
                coordinator_addr.clone().recipient(),
            );
            self.api_actor_addr = api_actor.start();
        }

        // check mqtt
        if !self.mqtt_actor_addr.connected() {
            warn!("MQTT Actor is not connected, restarting...");
            // A failure is recorded by `MqttActor::new` and retried on the next tick.
            if let Ok(mqtt_actor) = MqttActor::new(self.settings.mqtt.clone()) {
                self.mqtt_actor_addr = mqtt_actor.start();
            }
        }

        // check sensors
        if let Some(mqtt_sensors_actor_addr) = &self.mqtt_sensors_actor_addr
            && !mqtt_sensors_actor_addr.connected()
        {
            warn!("MQTT Sensors Actor is not connected, restarting...");
            // A failure is recorded by `MqttSensorsActor::new` and retried on the next tick.
            if let Ok(mqtt_sensors_actor_addr) = start_mqtt_sensors_actor(&self.settings.mqtt) {
                self.mqtt_sensors_actor_addr = mqtt_sensors_actor_addr;
            }
        }

        // check devices
        for device in &self.settings.devices {
            match self.device_usage_actor_addrs.get(&device.ip_address) {
                // device usage actor is alive and well, nothing to do here
                Some(device_usage_actor_addr) if device_usage_actor_addr.connected() => continue,
                Some(_) => warn!(
                    device.name = device.name,
                    device.ip_address = device.ip_address,
                    "Device usage actor is not connected, restarting...",
                ),
                None => info!(
                    device.name = device.name,
                    device.ip_address = device.ip_address,
                    "Device usage actor not found, creating a new one...",
                ),
            }

            // device usage actor hasn't been created yet or has died -> (re)create
            let device_usage_actor = DeviceUsageActor::new(
                coordinator_addr.clone().recipient(),
                DeviceSessions::new(coordinator_addr.clone().recipient()),
                Duration::from_secs(self.settings.tapo.refresh_rate_s),
                device.clone(),
            );
            let device_usage_actor_addr = device_usage_actor.start();

            self.device_usage_actor_addrs
                .insert(device.ip_address.clone(), device_usage_actor_addr);
        }

        // check thermostats
        for thermostat in &self.settings.thermostats {
            if !thermostat.enabled {
                continue;
            }

            match self.thermostat_actor_addrs.get(&thermostat.name) {
                Some(thermostat_actor_addr) if thermostat_actor_addr.connected() => continue,
                Some(_) => warn!(
                    thermostat.name = thermostat.name,
                    "Thermostat actor is not connected, restarting...",
                ),
                None => info!(
                    thermostat.name = thermostat.name,
                    "Thermostat actor not found, creating a new one...",
                ),
            }

            // thermostat actor hasn't been created yet or has died -> (re)create
            let thermostat_actor = ThermostatActor::new(
                coordinator_addr.clone().recipient(),
                DeviceSessions::new(coordinator_addr.clone().recipient()),
                self.settings.tapo.clone(),
                thermostat.clone(),
            );
            let thermostat_actor_addr = thermostat_actor.start();

            self.thermostat_actor_addrs
                .insert(thermostat.name.clone(), thermostat_actor_addr);
        }
    }
}

impl Actor for CoordinatorActor {
    type Context = Context<Self>;

    #[instrument(name = "CoordinatorActor::started", skip_all)]
    fn started(&mut self, ctx: &mut Self::Context) {
        // `run_interval` waits a full interval before its first tick, so the first health check,
        // which creates the device and thermostat actors, is scheduled at once. It runs after
        // `started` returns, so that its trace is its own rather than part of this span.
        ctx.run_later(Duration::ZERO, |act, ctx| act.tick(ctx));
        ctx.run_interval(HEALTH_CHECK_INTERVAL, |act, ctx| act.tick(ctx));
    }

    #[instrument(name = "CoordinatorActor::stopped", level = "error", skip_all)]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<ProbeMessage> for CoordinatorActor {
    type Result = ();

    /// Answers the API's health check; see `ProbeMessage`. Not traced: the kubelet asks every
    /// few seconds and the request span already covers the round trip.
    fn handle(&mut self, _: ProbeMessage, _: &mut Context<Self>) -> Self::Result {}
}

impl Handler<StopMessage> for CoordinatorActor {
    type Result = ResponseActFuture<Self, ()>;

    #[instrument(name = "CoordinatorActor::Handler<StopMessage>", skip_all)]
    fn handle(&mut self, _: StopMessage, _: &mut Context<Self>) -> Self::Result {
        self.shutting_down = true;

        let thermostat_actor_addrs: Vec<_> = self
            .thermostat_actor_addrs
            .drain()
            .map(|(_, addr)| addr)
            .collect();
        let device_usage_actor_addrs: Vec<_> = self
            .device_usage_actor_addrs
            .drain()
            .map(|(_, addr)| addr)
            .collect();
        let api_actor_addr = self.api_actor_addr.clone();
        let mqtt_actor_addr = self.mqtt_actor_addr.clone();
        let mqtt_sensors_actor_addr = self.mqtt_sensors_actor_addr.take();

        // The thermostats and device usage actors stop first, since they send the messages that the
        // others act on: a usage poll that finished meanwhile is then forwarded to the MQTT actor
        // before it is asked to stop, rather than after it has gone. A poll still running is
        // abandoned: its session actor finishes the request, but the reading is not sent. They
        // have nothing to drain, so the stop still takes as long as the slowest of the rest: the
        // MQTT publishes in flight or the API's graceful stop. The device sessions stop last,
        // since the API's requests in flight still use them; each finishes the request it is
        // running first, including one a stopped thermostat or poll abandoned.
        Box::pin(
            async move {
                tokio::join!(
                    join_all(thermostat_actor_addrs.into_iter().map(stop_actor)),
                    join_all(device_usage_actor_addrs.into_iter().map(stop_actor)),
                );

                tokio::join!(
                    stop_actor(api_actor_addr),
                    stop_actor(mqtt_actor_addr),
                    async {
                        if let Some(mqtt_sensors_actor_addr) = mqtt_sensors_actor_addr {
                            stop_actor(mqtt_sensors_actor_addr).await;
                        }
                    }
                );
            }
            .into_actor(self)
            .then(|_, act, _| {
                let device_session_actor_addrs: Vec<_> = act
                    .device_session_actor_addrs
                    .drain()
                    .map(|(_, addr)| addr)
                    .collect();

                join_all(device_session_actor_addrs.into_iter().map(stop_actor)).into_actor(act)
            })
            .map(|_, _, ctx| ctx.stop()),
        )
    }
}

impl Handler<GetDeviceSessionMessage> for CoordinatorActor {
    type Result = Option<Addr<DeviceSessionActor>>;

    /// The device's session actor, created on first use and recreated if it has died. Once the
    /// service is shutting down no session is created, so that none outlives the stop.
    fn handle(&mut self, message: GetDeviceSessionMessage, _: &mut Context<Self>) -> Self::Result {
        let ip_address = message.ip_address;

        let device_session_actor_addr = self.device_session_actor_addrs.get(&ip_address);

        if let Some(device_session_actor_addr) = device_session_actor_addr
            && device_session_actor_addr.connected()
        {
            return Some(device_session_actor_addr.clone());
        }

        if self.shutting_down {
            return None;
        }

        if device_session_actor_addr.is_some() {
            warn!(
                device.ip_address = ip_address,
                "Device session actor is not connected, restarting...",
            );
        }

        let device_session_actor_addr =
            DeviceSessionActor::new(self.settings.tapo.clone(), ip_address.clone()).start();
        self.device_session_actor_addrs
            .insert(ip_address, device_session_actor_addr.clone());

        Some(device_session_actor_addr)
    }
}

/// Starts a `MqttSensorsActor` when `mqtt.sensors` is present and enabled.
fn start_mqtt_sensors_actor(
    mqtt: &Mqtt,
) -> Result<Option<Addr<MqttSensorsActor>>, paho_mqtt::Error> {
    let Some(sensors) = mqtt.sensors.clone().filter(|sensors| sensors.enabled) else {
        return Ok(None);
    };

    let actor = MqttSensorsActor::new(mqtt.address.clone(), sensors)?;

    Ok(Some(actor.start()))
}

/// Sends `StopMessage` and waits until the actor's mailbox is gone, i.e. `stopped()` has run.
async fn stop_actor<A>(addr: Addr<A>)
where
    A: Actor + Handler<StopMessage>,
    A::Context: ToEnvelope<A, StopMessage>,
{
    // an error means the actor has already stopped
    let _ = addr.send(StopMessage).await;

    wait_until_stopped(&addr).await;
}

/// Waits until the actor's mailbox is gone, i.e. `stopped()` has run.
pub async fn wait_until_stopped<A: Actor>(addr: &Addr<A>) {
    while addr.connected() {
        sleep(Duration::from_millis(10)).await;
    }
}

impl Handler<DeviceUsageMessage> for CoordinatorActor {
    type Result = ();

    fn handle(&mut self, message: DeviceUsageMessage, _: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "CoordinatorActor::Handler<DeviceUsageMessage>",
            message = DeviceUsageMessage,
            parent = message.span_context,
            destination = "CoordinatorActor",
            device.name = %message.device.name,
            device.ip_address = %message.device.ip_address,
        );
        let _enter = span.enter();

        let result = self.mqtt_actor_addr.try_send({
            DeviceUsageMessage {
                span_context: span.context(),
                device: message.device,
                device_usage: message.device_usage,
            }
        });

        if let Err(e) = result {
            record_error!(&e);
        }
    }
}

impl Handler<GetSensorReadingMessage> for CoordinatorActor {
    type Result = ResponseFuture<Option<SensorReading>>;

    /// Forwarded to whichever sensors actor is current, so that a restarted sensors actor is picked
    /// up without re-wiring the thermostats. `None` when the sensors are disabled.
    fn handle(&mut self, message: GetSensorReadingMessage, _: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "CoordinatorActor::Handler<GetSensorReadingMessage>",
            message = GetSensorReadingMessage,
            parent = message.span_context,
            destination = "CoordinatorActor",
            sensor.location = %message.location,
        );
        let _enter = span.enter();

        let Some(mqtt_sensors_actor_addr) = self.mqtt_sensors_actor_addr.clone() else {
            return Box::pin(async { None });
        };

        let message = GetSensorReadingMessage {
            span_context: span.context(),
            location: message.location,
        };

        Box::pin(
            async move {
                match mqtt_sensors_actor_addr.send(message).await {
                    Ok(reading) => reading,
                    // Without a send timeout the only error is a closed mailbox: the sensors
                    // actor is stopping, either for a shutdown or because it has failed; the
                    // latter has already been reported and the health check recreates it.
                    Err(_) => {
                        warn!("MQTT Sensors Actor is gone, no sensor reading");
                        None
                    }
                }
            }
            .instrument(span.clone()),
        )
    }
}
