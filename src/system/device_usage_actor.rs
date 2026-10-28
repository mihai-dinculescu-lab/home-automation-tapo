use std::time::Duration;

use actix::{
    Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, Recipient, WrapFuture,
};
use tracing::{Instrument, info_span, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{
    settings::Device,
    system::devices::{DeviceSessions, DeviceType, Error as DeviceError},
    system::messages::{DeviceUsageMessage, StopMessage},
    telemetry::record_error,
};

#[derive(Debug)]
pub struct DeviceUsageActor {
    usage_recipient: Recipient<DeviceUsageMessage>,
    device_sessions: DeviceSessions,
    /// How often the device's usage is polled.
    refresh_rate: Duration,
    /// The topic the usage is published on.
    topic_name: String,
    device: Device,
    /// Set while a usage poll is running. A tick that lands meanwhile is skipped rather than
    /// queued behind it, so that a plug that answers slowly gets one poll at a time instead of
    /// a backlog.
    polling: bool,
}

impl DeviceUsageActor {
    #[instrument(name = "DeviceUsageActor::new", skip_all, fields(
        device.name = %device.name,
        device.ip_address = %device.ip_address,
    ))]
    pub fn new(
        usage_recipient: Recipient<DeviceUsageMessage>,
        device_sessions: DeviceSessions,
        refresh_rate: Duration,
        topic_name: String,
        device: Device,
    ) -> Self {
        Self {
            usage_recipient,
            device_sessions,
            refresh_rate,
            topic_name,
            device,
            polling: false,
        }
    }

    /// Polls the device's usage, unless the previous poll is still running.
    fn tick(&mut self, ctx: &mut Context<Self>) {
        let span = info_span!(
            "DeviceUsageActor::tick",
            device.name = %self.device.name,
            device.ip_address = %self.device.ip_address,
        );
        let _enter = span.enter();

        if self.polling {
            warn!("The previous usage poll is still running, skipping this tick");
            return;
        }

        self.polling = true;

        let device = self.device.clone();
        let topic_name = self.topic_name.clone();
        let device_sessions = self.device_sessions.clone();
        let usage_recipient = self.usage_recipient.clone();

        let fut = Self::query_device_usage(device, topic_name, device_sessions, usage_recipient)
            .instrument(span.clone())
            .into_actor(self)
            .map(|_, act, _| act.polling = false);

        ctx.spawn(fut);
    }

    #[instrument(name = "DeviceUsageActor::query_device_usage", skip_all, fields(
        device.name = %device.name,
        device.ip_address = %device.ip_address,
    ))]
    async fn query_device_usage(
        device: Device,
        topic_name: String,
        device_sessions: DeviceSessions,
        usage_recipient: Recipient<DeviceUsageMessage>,
    ) {
        let span = tracing::Span::current();

        let result = device_sessions
            .device_usage(DeviceType::PlugEnergyMonitoring, &device.ip_address)
            .await;

        match result {
            Ok(device_usage) => {
                let result = usage_recipient.try_send(DeviceUsageMessage {
                    span_context: span.context(),
                    topic_name,
                    device,
                    device_usage,
                });

                if let Err(e) = result {
                    record_error!(&e);
                }
            }
            // A poll that lands while the service stops.
            Err(DeviceError::ShuttingDown) => warn!("Shutting down, the usage was not polled"),
            Err(e) => record_error!(&e),
        }
    }
}

impl Actor for DeviceUsageActor {
    type Context = Context<Self>;

    #[instrument(name = "DeviceUsageActor::started", skip_all, fields(
        device.name = %self.device.name,
        device.ip_address = %self.device.ip_address,
    ))]
    fn started(&mut self, ctx: &mut Self::Context) {
        let refresh_rate = self.refresh_rate;

        // `run_interval` waits a full interval before its first tick, so the first poll is
        // scheduled at once. It runs after `started` returns, so that its trace is its own rather
        // than part of this span.
        ctx.run_later(Duration::ZERO, |act, ctx| act.tick(ctx));
        ctx.run_interval(refresh_rate, |act, ctx| act.tick(ctx));
    }
    #[instrument(name = "DeviceUsageActor::stopped", level = "error", skip_all, fields(
        device.name = %self.device.name,
        device.ip_address = %self.device.ip_address,
    ))]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<StopMessage> for DeviceUsageActor {
    type Result = ();

    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
    }
}
