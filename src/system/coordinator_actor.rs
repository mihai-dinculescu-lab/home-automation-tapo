use std::collections::HashMap;
use std::time::Duration;

use actix::clock::{interval, sleep};
use actix::dev::ToEnvelope;
use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture,
    WrapFuture,
};
use anyhow::Context as _;
use tracing::{info, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::settings::Settings;
use crate::system::api::api_actor::ApiActor;
use crate::system::device_actor::DeviceActor;
use crate::system::messages::{DeviceUsageMessage, HealthCheckMessage, StopMessage};
use crate::system::mqtt_actor::MqttActor;
use crate::telemetry::{consumer_span, producer_span, record_error};

#[derive(Debug)]
pub struct CoordinatorActor {
    settings: Settings,
    api_actor_addr: Addr<ApiActor>,
    mqtt_actor_addr: Addr<MqttActor>,
    device_actors: HashMap<String, Addr<DeviceActor>>,
    shutting_down: bool,
}

impl CoordinatorActor {
    #[instrument(name = "CoordinatorActor::new", skip_all)]
    pub fn new(settings: Settings) -> anyhow::Result<Self> {
        let mqtt_actor = MqttActor::new(settings.mqtt.clone())
            .inspect_err(|e| record_error!(e))
            .context("Failed to create the MQTT actor")?;
        let mqtt_actor_addr = mqtt_actor.start();

        let api_actor = ApiActor::new(settings.api.clone(), settings.tapo.clone());
        let api_actor_addr = api_actor.start();

        Ok(Self {
            settings,
            api_actor_addr,
            mqtt_actor_addr,
            device_actors: HashMap::new(),
            shutting_down: false,
        })
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
            let api_actor = ApiActor::new(self.settings.api.clone(), self.settings.tapo.clone());
            self.api_actor_addr = api_actor.start();
        }

        // check mqtt
        if !self.mqtt_actor_addr.connected() {
            warn!("MQTT Actor is not connected, restarting...");
            // retried on the next tick
            match MqttActor::new(self.settings.mqtt.clone()) {
                Ok(mqtt_actor) => self.mqtt_actor_addr = mqtt_actor.start(),
                Err(e) => record_error!(&e),
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
            let device_actor =
                DeviceActor::new(addr.clone(), self.settings.tapo.clone(), device.clone());
            let device_actor_addr = device_actor.start();

            self.device_actors
                .insert(device.ip_address.clone(), device_actor_addr);
        }
    }
}

impl Handler<StopMessage> for CoordinatorActor {
    type Result = ResponseActFuture<Self, ()>;

    #[instrument(name = "CoordinatorActor::Handler<StopMessage>", skip_all)]
    fn handle(&mut self, _: StopMessage, _: &mut Context<Self>) -> Self::Result {
        self.shutting_down = true;

        let device_actors: Vec<_> = self.device_actors.drain().map(|(_, addr)| addr).collect();
        let api_actor_addr = self.api_actor_addr.clone();
        let mqtt_actor_addr = self.mqtt_actor_addr.clone();

        // The device actors go first so that usage they have already queued can still be
        // published. The API and MQTT drains overlap to stay inside the shutdown timeout.
        Box::pin(
            async move {
                for device_actor in device_actors {
                    stop_actor(device_actor).await;
                }
                tokio::join!(stop_actor(api_actor_addr), stop_actor(mqtt_actor_addr));
            }
            .into_actor(self)
            .map(|_, _, ctx| ctx.stop()),
        )
    }
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
