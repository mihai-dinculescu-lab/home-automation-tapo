use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

use actix::{
    Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, StreamHandler, WrapFuture,
};
use chrono::Utc;
use paho_mqtt::{
    AsyncClient, AsyncReceiver, ConnectOptions, ConnectOptionsBuilder, CreateOptionsBuilder,
    DisconnectOptionsBuilder, Message, QOS_1,
};
use serde::Deserialize;
use tracing::{Instrument, info, instrument, warn};

use crate::{
    settings::Sensors,
    system::messages::{GetSensorReadingMessage, SensorReading, StopMessage},
    telemetry::{consumer_span, record_error},
};

/// Paho's bound on a connect attempt.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Delay between connect attempts while the broker can't be reached at startup, and between
/// subscribe attempts while the broker refuses the subscription. Once connected, paho reconnects
/// on its own (see `connect_options`).
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// How long a disconnect waits for the broker to acknowledge it.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// Readings arrive every few minutes per sensor, so a small buffer is plenty.
const STREAM_BUFFER_SIZE: usize = 100;

/// The fields read from the sensor payloads; other fields are ignored.
#[derive(Debug, Deserialize)]
struct SensorPayload {
    location: String,
    temperature: f64,
}

/// Sent from paho's thread after every (re)connect. The session is clean, so the subscription
/// has to be re-made each time.
#[derive(Debug, actix::Message)]
#[rtype(result = "()")]
struct ConnectedMessage;

/// Subscribes to the sensor readings published over MQTT and keeps the latest one per location.
///
/// Separate from `MqttActor`, which publishes with its own client, so that a publish failure
/// cannot take the readings cache down with it and the two never fight over one connection.
pub struct MqttSensorsActor {
    client: AsyncClient,
    topic_name: String,
    /// Taken by `started`, which attaches it to the actor. Pinned because the receiver is not
    /// `Unpin` and actors have to be.
    stream: Option<Pin<Box<AsyncReceiver<Option<Message>>>>>,
    /// Latest reading per `location`.
    readings: HashMap<String, SensorReading>,
    stop_requested: bool,
}

impl MqttSensorsActor {
    #[instrument(name = "MqttSensorsActor::new", skip_all)]
    pub fn new(address: String, config: Sensors) -> Result<Self, paho_mqtt::Error> {
        // No client id, so the broker assigns one: two instances (a rolling update, a local run
        // against the same broker) would otherwise keep evicting each other's session.
        let create_options = CreateOptionsBuilder::new_v3()
            .server_uri(address)
            .finalize();

        let mut client = AsyncClient::new(create_options).inspect_err(|e| record_error!(e))?;

        // Opened before connecting so that no message is missed.
        let stream = client.get_stream(STREAM_BUFFER_SIZE);

        Ok(Self {
            client,
            topic_name: config.topic_name,
            stream: Some(Box::pin(stream)),
            readings: HashMap::new(),
            stop_requested: false,
        })
    }

    fn connect_options() -> ConnectOptions {
        ConnectOptionsBuilder::new_v3()
            .connect_timeout(CONNECT_TIMEOUT)
            .clean_session(true)
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(30))
            .finalize()
    }

    /// Connects to the broker, retrying until it succeeds or the actor is asked to stop. Paho
    /// only reconnects on its own once a connection has been made.
    #[instrument(name = "MqttSensorsActor::connect", skip_all)]
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

    /// Subscribes to the sensor readings, retrying a refusal (a broker still loading its ACLs
    /// after a restart, a bad topic filter) until it succeeds or the actor is asked to stop. The
    /// readings cache is kept meanwhile.
    #[instrument(name = "MqttSensorsActor::subscribe", skip_all, fields(
        messaging.system = "mqtt",
        messaging.destination.name = %self.topic_name,
    ))]
    fn subscribe(&mut self, ctx: &mut Context<Self>) {
        // The connection was lost since the retry was scheduled; the reconnect subscribes anew.
        if !self.client.is_connected() {
            return;
        }

        let client = self.client.clone();
        let topic_name = self.topic_name.clone();

        info!("Subscribing to '{topic_name}'...");

        let fut = async move {
            client
                .subscribe(&topic_name, QOS_1)
                .await
                .inspect_err(|e| record_error!(e))
                .is_ok()
        }
        .instrument(tracing::Span::current())
        .into_actor(self)
        .map(|subscribed, act, ctx| {
            if !subscribed && !act.stop_requested {
                ctx.run_later(RECONNECT_DELAY, |act, ctx| act.subscribe(ctx));
            }
        });

        ctx.spawn(fut);
    }

    /// Disconnects, giving the broker a moment to acknowledge it, then stops.
    fn stop(&mut self, ctx: &mut Context<Self>) {
        self.stop_requested = true;

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
        .into_actor(self)
        .map(|_, _, ctx| ctx.stop());

        ctx.wait(fut);
    }

    fn store_reading(&mut self, message: &Message) {
        let span = tracing::Span::current();

        let payload = message.payload_str();

        let payload = match serde_json::from_str::<SensorPayload>(&payload) {
            Ok(payload) => payload,
            Err(e) => {
                warn!(
                    "Skipping MQTT message on '{}' without a sensor reading: {e} ({payload})",
                    message.topic()
                );
                return;
            }
        };

        span.record("sensor.location", &payload.location);
        span.record("sensor.temperature", payload.temperature);

        info!(
            "Received sensor reading for '{}': {} °C",
            payload.location, payload.temperature
        );

        self.readings.insert(
            payload.location.clone(),
            SensorReading {
                location: payload.location,
                temperature: payload.temperature,
                received_at: Utc::now(),
            },
        );
    }
}

impl Actor for MqttSensorsActor {
    type Context = Context<Self>;

    #[instrument(name = "MqttSensorsActor::started", skip_all)]
    fn started(&mut self, ctx: &mut Self::Context) {
        if let Some(stream) = self.stream.take() {
            ctx.add_stream(stream);
        }

        // The callback runs on paho's thread, which can't await the subscription, so it hands
        // over to the actor instead.
        let addr = ctx.address();
        self.client
            .set_connected_callback(move |_| addr.do_send(ConnectedMessage));

        self.connect(ctx);
    }

    #[instrument(name = "MqttSensorsActor::stopped", level = "error", skip_all)]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl StreamHandler<Option<Message>> for MqttSensorsActor {
    fn handle(&mut self, item: Option<Message>, _: &mut Context<Self>) {
        // `None` is a lost connection; paho logs it and reconnects on its own.
        let Some(message) = item else {
            return;
        };

        let span = tracing::info_span!(
            "MqttSensorsActor::StreamHandler<Message>",
            otel.kind = "consumer",
            messaging.system = "mqtt",
            messaging.operation.name = "receive",
            messaging.operation.type = "receive",
            messaging.destination.name = %message.topic(),
            sensor.location = tracing::field::Empty,
            sensor.temperature = tracing::field::Empty,
        );
        let _enter = span.enter();

        self.store_reading(&message);
    }

    // No `finished`: the stream's sender lives in the client's message callback and the client is
    // owned by the actor, so the stream can only end once the actor is already gone.
}

impl Handler<ConnectedMessage> for MqttSensorsActor {
    type Result = ();

    fn handle(&mut self, _: ConnectedMessage, ctx: &mut Context<Self>) -> Self::Result {
        info!("Connected to the MQTT broker");
        self.subscribe(ctx);
    }
}

impl Handler<StopMessage> for MqttSensorsActor {
    type Result = ();

    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        self.stop(ctx);
    }
}

impl Handler<GetSensorReadingMessage> for MqttSensorsActor {
    type Result = Option<SensorReading>;

    fn handle(&mut self, message: GetSensorReadingMessage, _: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "MqttSensorsActor::Handler<GetSensorReadingMessage>",
            message = GetSensorReadingMessage,
            parent = message.span_context,
            destination = "MqttSensorsActor",
            sensor.location = %message.location,
        );
        let _enter = span.enter();

        self.readings.get(&message.location).cloned()
    }
}
