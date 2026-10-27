use std::time::Duration;

use actix::{
    Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, Recipient, WrapFuture,
};
use futures_util::FutureExt as _;
use tracing::{Instrument, info_span, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{
    settings::Device,
    system::devices::{DeviceType, Devices},
    system::messages::{DeviceUsageMessage, StopMessage},
    telemetry::record_error,
};

#[derive(Debug)]
pub struct DeviceActor {
    usage_recipient: Recipient<DeviceUsageMessage>,
    devices: Devices,
    device: Device,
    /// Set while a usage poll is running. A tick that lands meanwhile is skipped rather than
    /// queued behind it, so that a plug that answers slowly gets one poll at a time instead of
    /// a backlog.
    polling: bool,
}

impl DeviceActor {
    #[instrument(name = "DeviceActor::new", skip_all, fields(
        device.name = %device.name,
        device.ip_address = %device.ip_address,
    ))]
    pub fn new(
        usage_recipient: Recipient<DeviceUsageMessage>,
        devices: Devices,
        device: Device,
    ) -> Self {
        Self {
            usage_recipient,
            devices,
            device,
            polling: false,
        }
    }

    /// Polls the device's usage, unless the previous poll is still running.
    fn tick(&mut self, ctx: &mut Context<Self>) {
        let span = info_span!(
            "DeviceActor::tick",
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
        let devices = self.devices.clone();
        let usage_recipient = self.usage_recipient.clone();

        let fut = Self::query_device_usage(device, devices, usage_recipient)
            .instrument(span.clone())
            .into_actor(self)
            .map(|_, act, _| act.polling = false);

        ctx.spawn(fut);
    }

    #[instrument(name = "DeviceActor::query_device_usage", skip_all, fields(
        device.name = %device.name,
        device.ip_address = %device.ip_address,
    ))]
    async fn query_device_usage(
        device: Device,
        devices: Devices,
        usage_recipient: Recipient<DeviceUsageMessage>,
    ) {
        let span = tracing::Span::current();

        let result = devices
            .run(
                DeviceType::PlugEnergyMonitoring,
                &device.ip_address,
                |handler| handler.device_usage().boxed_local(),
            )
            .await;

        match result {
            Ok(device_usage) => {
                let result = usage_recipient.try_send(DeviceUsageMessage {
                    span_context: span.context(),
                    device,
                    device_usage,
                });

                if let Err(e) = result {
                    record_error!(&e);
                }
            }
            Err(e) => record_error!(&e),
        }
    }
}

impl Actor for DeviceActor {
    type Context = Context<Self>;

    #[instrument(name = "DeviceActor::started", skip_all, fields(
        device.name = %self.device.name,
        device.ip_address = %self.device.ip_address,
    ))]
    fn started(&mut self, ctx: &mut Self::Context) {
        let refresh_rate = Duration::from_secs(self.devices.config().refresh_rate_s);

        // `run_interval` waits a full interval before its first tick, so the first poll is
        // scheduled at once. It runs after `started` returns, so that its trace is its own rather
        // than part of this span.
        ctx.run_later(Duration::ZERO, |act, ctx| act.tick(ctx));
        ctx.run_interval(refresh_rate, |act, ctx| act.tick(ctx));
    }
    #[instrument(name = "DeviceActor::stopped", level = "error", skip_all, fields(
        device.name = %self.device.name,
        device.ip_address = %self.device.ip_address,
    ))]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<StopMessage> for DeviceActor {
    type Result = ();

    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
    }
}
