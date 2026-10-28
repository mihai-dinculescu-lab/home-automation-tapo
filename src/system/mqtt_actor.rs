use std::time::Duration;

use actix::clock::timeout;
use actix::{Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, WrapFuture};
use paho_mqtt::{
    AsyncClient, ConnectOptions, ConnectOptionsBuilder, CreateOptionsBuilder,
    DisconnectOptionsBuilder, Message, QOS_1,
};
use serde_json::json;
use tapo::responses::DeviceUsageEnergyMonitoringResult;
use tracing::{Instrument, info, instrument, warn};

use crate::{
    settings::{Device, Mqtt},
    system::messages::{DeviceUsageMessage, MqttMessagePayload, StopMessage},
    telemetry::{consumer_span, record_error},
};

/// Paho's bound on a connect attempt.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Delay between connect attempts while the broker can't be reached at startup. Once connected,
/// paho reconnects on its own (see `connect_options`).
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Upper bound for a publish to be acknowledged. A stop waits for the publishes in flight, so
/// this also bounds the stop and has to stay well inside the actor shutdown timeout in `main.rs`.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a disconnect waits for the broker to acknowledge what is still in flight.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// How many messages paho queues while the broker is unreachable, oldest dropped first. They are
/// delivered once the connection is back and lost if the process exits before that.
const MAX_BUFFERED_MESSAGES: i32 = 100;

pub struct MqttActor {
    config: Mqtt,
    client: AsyncClient,
    publishes_in_flight: usize,
    stop_requested: bool,
}

impl MqttActor {
    #[instrument(name = "MqttActor::new", skip_all)]
    pub fn new(config: Mqtt) -> Result<Self, paho_mqtt::Error> {
        let options = CreateOptionsBuilder::new()
            .server_uri(&config.address)
            .send_while_disconnected(true)
            .allow_disconnected_send_at_anytime(true)
            .max_buffered_messages(MAX_BUFFERED_MESSAGES)
            .delete_oldest_messages(true)
            .finalize();

        let client = AsyncClient::new(options).inspect_err(|e| record_error!(e))?;

        Ok(Self {
            config,
            client,
            publishes_in_flight: 0,
            stop_requested: false,
        })
    }

    fn connect_options() -> ConnectOptions {
        ConnectOptionsBuilder::new()
            .connect_timeout(CONNECT_TIMEOUT)
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(30))
            .finalize()
    }

    /// Connects to the broker, retrying until it succeeds or the actor is asked to stop.
    ///
    /// The connection is owned here rather than made on demand by each publish: paho ignores a
    /// connect while one is in progress and only ever completes the latest one, so concurrent
    /// connects would lose publishes.
    #[instrument(name = "MqttActor::connect", skip_all)]
    fn connect(&mut self, ctx: &mut Context<Self>) {
        let client = self.client.clone();

        let fut = async move {
            client
                .connect(Self::connect_options())
                .await
                .inspect_err(|e| record_error!(e))
                .is_ok()
        }
        .instrument(tracing::Span::current())
        .into_actor(self)
        .map(|connected, act, ctx| {
            if !connected && !act.stop_requested {
                ctx.run_later(RECONNECT_DELAY, |act, ctx| act.connect(ctx));
            }
        });

        ctx.spawn(fut);
    }

    /// Disconnects, giving the broker a moment to acknowledge what is in flight, then stops.
    #[instrument(name = "MqttActor::stop", skip_all)]
    fn stop(&mut self, ctx: &mut Context<Self>) {
        let client = self.client.clone();

        let fut = async move {
            if !client.is_connected() {
                return;
            }

            let options = DisconnectOptionsBuilder::new()
                .timeout(DISCONNECT_TIMEOUT)
                .finalize();

            if let Err(e) = client.disconnect(options).await {
                warn!(error = %e, "Failed to disconnect from the MQTT broker");
            }
        }
        .instrument(tracing::Span::current())
        .into_actor(self)
        .map(|_, _, ctx| ctx.stop());

        ctx.wait(fut);
    }

    #[instrument(name = "MqttActor::send_mqtt_message", skip_all, fields(
        otel.kind = "producer",
        messaging.system = "mqtt",
        messaging.operation.type = "send",
        messaging.destination.name = %topic_name,
        device.name = %device.name,
        device.ip_address = %device.ip_address,
    ))]
    pub async fn send_mqtt_message(
        device: Device,
        device_usage: DeviceUsageEnergyMonitoringResult,
        client: AsyncClient,
        topic_name: String,
    ) {
        let payload: MqttMessagePayload = (device, device_usage).into();
        let payload = json!(payload).to_string();
        let message = Message::new(topic_name, payload.as_str(), QOS_1);

        if !client.is_connected() {
            // Queued by paho (see `MAX_BUFFERED_MESSAGES`); the acknowledgement would only come
            // after a reconnect, so it isn't awaited.
            match client.try_publish(message) {
                Ok(_) => warn!("MQTT broker disconnected, message buffered: {payload}"),
                Err(e) => record_error!(&e),
            }

            return;
        }

        // Awaited rather than `wait()`ed: all actors share one thread, so blocking here would
        // stall device polling and shutdown. The timeout turns a stalled broker into an error;
        // it means the publish is unconfirmed, not necessarily lost, as paho may still deliver it.
        let result = timeout(PUBLISH_TIMEOUT, client.publish(message))
            .await
            .unwrap_or(Err(paho_mqtt::Error::Timeout));

        match result {
            Ok(_) => info!("Sent MQTT message: {payload}"),
            Err(e) => record_error!(&e),
        }
    }
}

impl Actor for MqttActor {
    type Context = Context<Self>;

    #[instrument(name = "MqttActor::started", skip_all)]
    fn started(&mut self, ctx: &mut Self::Context) {
        self.connect(ctx);
    }

    #[instrument(name = "MqttActor::stopped", level = "error", skip_all)]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<StopMessage> for MqttActor {
    type Result = ();

    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        // The publishes in flight finish first (bounded by `PUBLISH_TIMEOUT`); the last one
        // stops the actor.
        self.stop_requested = true;

        if self.publishes_in_flight == 0 {
            self.stop(ctx);
        }
    }
}

impl Handler<DeviceUsageMessage> for MqttActor {
    type Result = ();

    fn handle(&mut self, message: DeviceUsageMessage, ctx: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "MqttActor::Handler<DeviceUsageMessage>",
            message = DeviceUsageMessage,
            parent = message.span_context,
            destination = "MqttActor",
            device.name = %message.device.name,
            device.ip_address = %message.device.ip_address,
        );
        let _enter = span.enter();

        // Publishing it would hold up the stop. The coordinator stops the device usage actors
        // first, so none should arrive this late.
        if self.stop_requested {
            warn!("Stopping, dropping the device usage");
            return;
        }

        let client = self.client.clone();
        let topic_name = self.config.topic_name.clone();

        self.publishes_in_flight += 1;

        let fut = Self::send_mqtt_message(message.device, message.device_usage, client, topic_name)
            .instrument(span.clone())
            .into_actor(self)
            .map(|_, act, ctx| {
                act.publishes_in_flight -= 1;

                if act.stop_requested && act.publishes_in_flight == 0 {
                    act.stop(ctx);
                }
            });

        ctx.spawn(fut);
    }
}
