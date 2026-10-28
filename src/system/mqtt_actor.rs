use std::time::Duration;

use actix::clock::timeout;
use actix::{Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, WrapFuture};
use paho_mqtt::{
    AsyncClient, ConnectOptions, ConnectOptionsBuilder, CreateOptionsBuilder,
    DisconnectOptionsBuilder, Message, QOS_1,
};
use serde_json::json;
use tracing::{Instrument, instrument, warn};

use crate::{
    settings::Mqtt,
    system::messages::{
        DeviceUsageMessage, MqttMessagePayload, StopMessage, ThermostatDecisionMessage,
        ThermostatDecisionPayload,
    },
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
const MAX_BUFFERED_MESSAGES: i32 = 256;

/// The coordinator relays the usage and the decisions with `try_send`, which drops a message when
/// the mailbox is full. Everything that reaches the coordinator's mailbox can be relayed in a
/// burst, so this is as large as the coordinator's own.
const MAILBOX_CAPACITY: usize = 128;

/// Publishes to the broker for every feature; each message names its own topic.
pub struct MqttActor {
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

    /// Publishes `payload` under `span`, counting it among the publishes a stop waits for.
    fn spawn_publish(
        &mut self,
        ctx: &mut Context<Self>,
        span: &tracing::Span,
        topic_name: String,
        payload: String,
        publisher: Publisher,
    ) {
        let client = self.client.clone();

        self.publishes_in_flight += 1;

        let fut = Self::publish(client, topic_name, payload, publisher)
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

    #[instrument(name = "MqttActor::publish", skip_all, fields(
        otel.kind = "producer",
        messaging.system = "mqtt",
        messaging.operation.type = "send",
        messaging.destination.name = %topic_name,
        device.name = tracing::field::Empty,
        device.ip_address = tracing::field::Empty,
        thermostat.name = tracing::field::Empty,
    ))]
    async fn publish(
        client: AsyncClient,
        topic_name: String,
        payload: String,
        publisher: Publisher,
    ) {
        let span = tracing::Span::current();
        match &publisher {
            Publisher::Device { name, ip_address } => {
                span.record("device.name", name.as_str());
                span.record("device.ip_address", ip_address.as_str());
            }
            Publisher::Thermostat { name } => {
                span.record("thermostat.name", name.as_str());
            }
        }

        // The messages are not logged: the usage and the decisions are published every few
        // minutes, and each decision is logged already when it is made.
        let message = Message::new(topic_name, payload.as_str(), QOS_1);

        if !client.is_connected() {
            // Queued by paho (see `MAX_BUFFERED_MESSAGES`); the acknowledgement would only come
            // after a reconnect, so it isn't awaited.
            match client.try_publish(message) {
                Ok(_) => warn!("MQTT broker disconnected, message buffered"),
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

        if let Err(e) = result {
            record_error!(&e);
        }
    }
}

/// What a message is about, recorded on its producer span so that a failed publish, whose error
/// sits on that span, can be searched for by device or thermostat.
enum Publisher {
    Device { name: String, ip_address: String },
    Thermostat { name: String },
}

impl Actor for MqttActor {
    type Context = Context<Self>;

    #[instrument(name = "MqttActor::started", skip_all)]
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(MAILBOX_CAPACITY);

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

        let publisher = Publisher::Device {
            name: message.device.name.clone(),
            ip_address: message.device.ip_address.clone(),
        };
        let payload: MqttMessagePayload = (message.device, message.device_usage).into();
        let payload = json!(payload).to_string();

        self.spawn_publish(ctx, &span, message.topic_name, payload, publisher);
    }
}

impl Handler<ThermostatDecisionMessage> for MqttActor {
    type Result = ();

    fn handle(
        &mut self,
        message: ThermostatDecisionMessage,
        ctx: &mut Context<Self>,
    ) -> Self::Result {
        let span = consumer_span!(
            name = "MqttActor::Handler<ThermostatDecisionMessage>",
            message = ThermostatDecisionMessage,
            parent = message.span_context,
            destination = "MqttActor",
            thermostat.name = %message.thermostat_name,
        );
        let _enter = span.enter();

        // Publishing it would hold up the stop. The coordinator stops the thermostats first, so
        // none should arrive this late.
        if self.stop_requested {
            warn!("Stopping, dropping the thermostat decision");
            return;
        }

        let publisher = Publisher::Thermostat {
            name: message.thermostat_name.clone(),
        };
        let payload = ThermostatDecisionPayload::new(
            message.thermostat_name,
            message.evaluation,
            message.heaters,
        );
        let payload = json!(payload).to_string();

        self.spawn_publish(ctx, &span, message.topic_name, payload, publisher);
    }
}
