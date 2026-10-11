use actix::{Actor, ActorContext, AsyncContext, Context, Handler, WrapFuture};
use paho_mqtt::{AsyncClient, Message, QOS_1};
use serde_json::json;
use tapo::responses::DeviceUsageEnergyMonitoringResult;
use tracing::{Instrument, info, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{
    settings::{Device, Mqtt},
    system::messages::{DeviceUsageMessage, MqttMessagePayload, StopMessage},
    telemetry::record_error,
};

pub struct MqttActor {
    config: Mqtt,
    client: AsyncClient,
}

impl MqttActor {
    #[instrument(name = "MqttActor::new", skip_all, fields(
        otel.status_code = tracing::field::Empty,
        exception.type = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        exception.stacktrace = tracing::field::Empty,
    ))]
    pub fn new(config: Mqtt) -> Result<Self, paho_mqtt::Error> {
        let span = tracing::Span::current();

        let client = AsyncClient::new(config.address.clone()).inspect_err(|e| {
            record_error(&span, &e);
        })?;

        Ok(Self { config, client })
    }

    pub async fn send_mqtt_message(
        device: Device,
        device_usage: DeviceUsageEnergyMonitoringResult,
        client: AsyncClient,
        topic_name: String,
    ) {
        let span = tracing::Span::current();

        let payload: MqttMessagePayload = (device, device_usage).into();
        let payload = json!(payload).to_string();

        let result = async {
            if !client.is_connected() {
                client.connect(None).await?;
            }

            let message = Message::new(topic_name, payload.clone(), QOS_1);
            let delivery_token = client.publish(message);
            delivery_token.wait()?;

            Ok::<_, paho_mqtt::Error>(())
        }
        .await;

        match result {
            Ok(_) => info!("Sent MQTT message: {payload}"),
            Err(e) => record_error(&span, &e),
        }
    }
}

impl Actor for MqttActor {
    type Context = Context<Self>;

    #[instrument(name = "MqttActor::started", skip_all)]
    fn started(&mut self, _: &mut Self::Context) {}

    #[instrument(name = "MqttActor::stopped", level = "error", skip_all)]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<StopMessage> for MqttActor {
    type Result = ();

    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
    }
}

impl Handler<DeviceUsageMessage> for MqttActor {
    type Result = ();

    #[instrument(
        name = "MqttActor::Handler<DeviceUsageMessage>",
        skip_all,
        fields(
            otel.kind = "consumer",
            messaging.message.id = "DeviceUsageMessage",
            messaging.operation.name = "poll",
            messaging.operation.type = "receive",
            messaging.destination.name = "MqttActor",
            device.name = %message.device.name,
            device.ip_address = %message.device.ip_address,
            otel.status_code = tracing::field::Empty,
            exception.type = tracing::field::Empty,
            exception.message = tracing::field::Empty,
            exception.stacktrace = tracing::field::Empty,
        )
    )]
    fn handle(&mut self, message: DeviceUsageMessage, ctx: &mut Context<Self>) -> Self::Result {
        let span = tracing::Span::current();
        let _ = span.set_parent(message.span_context);

        let client = self.client.clone();
        let topic_name = self.config.topic_name.clone();

        let fut = Self::send_mqtt_message(message.device, message.device_usage, client, topic_name)
            .instrument(span)
            .into_actor(self);

        ctx.spawn(fut);
    }
}
