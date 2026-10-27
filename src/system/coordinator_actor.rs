use std::collections::HashMap;
use std::time::Duration;

use actix::clock::{interval, sleep};
use actix::dev::ToEnvelope;
use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture,
    ResponseFuture, WrapFuture,
};
use anyhow::Context as _;
use futures_util::future::join_all;
use tracing::{Instrument, info, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::settings::{Mqtt, Settings};
use crate::system::api::api_actor::ApiActor;
use crate::system::device_actor::DeviceActor;
use crate::system::devices::Devices;
use crate::system::messages::{
    DeviceUsageMessage, GetSensorReadingMessage, HealthCheckMessage, ProbeMessage, SensorReading,
    StopMessage,
};
use crate::system::mqtt_actor::MqttActor;
use crate::system::mqtt_sensors_actor::MqttSensorsActor;
use crate::system::thermostat::thermostat_actor::ThermostatActor;
use crate::telemetry::{consumer_span, producer_span, record_error};

#[derive(Debug)]
pub struct CoordinatorActor {
    settings: Settings,
    /// Shared by every actor that talks to a device, so that each device has a single session.
    devices: Devices,
    api_actor_addr: Addr<ApiActor>,
    mqtt_actor_addr: Addr<MqttActor>,
    /// `None` when `mqtt.sensors` is missing or disabled.
    mqtt_sensors_actor_addr: Option<Addr<MqttSensorsActor>>,
    device_actors: HashMap<String, Addr<DeviceActor>>,
    thermostat_actors: HashMap<String, Addr<ThermostatActor>>,
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

        let devices = Devices::new(settings.tapo.clone());

        // The API actor probes the coordinator, so it needs the address before the actor exists.
        Ok(Self::create(move |ctx| {
            let api_actor = ApiActor::new(
                settings.api.clone(),
                devices.clone(),
                ctx.address().recipient(),
            );
            let api_actor_addr = api_actor.start();

            Self {
                settings,
                devices,
                api_actor_addr,
                mqtt_actor_addr,
                mqtt_sensors_actor_addr,
                device_actors: HashMap::new(),
                thermostat_actors: HashMap::new(),
                shutting_down: false,
            }
        }))
    }

    /// Stops the coordinator and all of its child actors, returning once they have all stopped.
    pub async fn stop(addr: Addr<Self>) {
        stop_actor(addr).await;
    }
}

impl Actor for CoordinatorActor {
    type Context = Context<Self>;

    #[instrument(name = "CoordinatorActor::started", skip_all)]
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();

        let fut = async move {
            let mut interval = interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                let span = producer_span!(
                    name = "CoordinatorActor::IntervalTick",
                    message = HealthCheckMessage,
                    destination = "CoordinatorActor",
                );
                let _enter = span.enter();

                if let Err(e) = addr.try_send(HealthCheckMessage {
                    span_context: span.context(),
                }) {
                    record_error!(&e);
                }
            }
        }
        .into_actor(self);

        ctx.spawn(fut);
    }

    #[instrument(name = "CoordinatorActor::stopped", level = "error", skip_all)]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<HealthCheckMessage> for CoordinatorActor {
    type Result = ();

    fn handle(&mut self, message: HealthCheckMessage, ctx: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "CoordinatorActor::Handler<HealthCheckMessage>",
            message = HealthCheckMessage,
            parent = message.span_context,
            destination = "CoordinatorActor",
        );
        let _enter = span.enter();

        // don't restart the actors that are being stopped
        if self.shutting_down {
            return;
        }

        // check api
        if !self.api_actor_addr.connected() {
            warn!("API Actor is not connected, restarting...");
            let api_actor = ApiActor::new(
                self.settings.api.clone(),
                self.devices.clone(),
                ctx.address().recipient(),
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
            if let Ok(addr) = start_mqtt_sensors_actor(&self.settings.mqtt) {
                self.mqtt_sensors_actor_addr = addr;
            }
        }

        // check devices
        let addr = ctx.address();

        for device in &self.settings.devices {
            match self.device_actors.get(&device.ip_address) {
                // device actor is alive and well, nothing to do here
                Some(device_actor) if device_actor.connected() => continue,
                Some(_) => warn!(
                    device.name = device.name,
                    device.ip_address = device.ip_address,
                    "Device actor is not connected, restarting...",
                ),
                None => info!(
                    device.name = device.name,
                    device.ip_address = device.ip_address,
                    "Device actor not found, creating a new one...",
                ),
            }

            // device actor hasn't been created yet or has died -> (re)create
            let device_actor = DeviceActor::new(addr.clone(), self.devices.clone(), device.clone());
            let device_actor_addr = device_actor.start();

            self.device_actors
                .insert(device.ip_address.clone(), device_actor_addr);
        }

        // check thermostats
        for thermostat in &self.settings.thermostats {
            if !thermostat.enabled {
                continue;
            }

            match self.thermostat_actors.get(&thermostat.name) {
                Some(thermostat_actor) if thermostat_actor.connected() => continue,
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
            let thermostat_actor =
                ThermostatActor::new(addr.clone(), self.devices.clone(), thermostat.clone());
            let thermostat_actor_addr = thermostat_actor.start();

            self.thermostat_actors
                .insert(thermostat.name.clone(), thermostat_actor_addr);
        }
    }
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

        let thermostat_actors: Vec<_> = self
            .thermostat_actors
            .drain()
            .map(|(_, addr)| addr)
            .collect();
        let device_actors: Vec<_> = self.device_actors.drain().map(|(_, addr)| addr).collect();
        let api_actor_addr = self.api_actor_addr.clone();
        let mqtt_actor_addr = self.mqtt_actor_addr.clone();
        let mqtt_sensors_actor_addr = self.mqtt_sensors_actor_addr.take();

        // The device actors go first: stopping one drops the poll it has in flight, which frees
        // the plug for the thermostat about to switch it off, and the usage they have already
        // queued can still be published. The thermostats go next, before the sensors they read
        // through the coordinator are gone, and together, since each one switches its heaters
        // off on the way out. The API, MQTT and sensors drains overlap to stay inside the
        // shutdown timeout.
        Box::pin(
            async move {
                for device_actor in device_actors {
                    stop_actor(device_actor).await;
                }
                join_all(thermostat_actors.into_iter().map(stop_actor)).await;
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
            .map(|_, _, ctx| ctx.stop()),
        )
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
