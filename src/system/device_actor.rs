use std::time::Duration;

use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, WrapFuture,
    clock::interval,
};
use futures_util::FutureExt as _;
use tracing::{Instrument, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{
    settings::Device,
    system::devices::{DeviceType, Devices},
    system::messages::{DeviceUsageMessage, GetDeviceDataMessage, StopMessage},
    telemetry::{consumer_span, producer_span, record_error},
};

use super::coordinator_actor::CoordinatorActor;

#[derive(Debug)]
pub struct DeviceActor {
    coordinator_actor_addr: Addr<CoordinatorActor>,
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
        coordinator_actor_addr: Addr<CoordinatorActor>,
        devices: Devices,
        device: Device,
    ) -> Self {
        Self {
            coordinator_actor_addr,
            devices,
            device,
            polling: false,
        }
    }

    #[instrument(name = "DeviceActor::query_device_usage", skip_all, fields(
        device.name = %device.name,
        device.ip_address = %device.ip_address,
    ))]
    async fn query_device_usage(
        device: Device,
        devices: Devices,
        coordinator_actor_addr: Addr<CoordinatorActor>,
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
                let result = coordinator_actor_addr.try_send(DeviceUsageMessage {
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
        let addr = ctx.address();
        let refresh_rate = Duration::from_secs(self.devices.config().refresh_rate_s);

        let device = self.device.clone();

        let fut = async move {
            let mut interval = interval(refresh_rate);

            loop {
                interval.tick().await;

                let span = producer_span!(
                    name = "DeviceActor::IntervalTick",
                    message = GetDeviceDataMessage,
                    destination = "DeviceActor",
                    device.name = %device.name,
                    device.ip_address = %device.ip_address,
                );
                let _enter = span.enter();

                if let Err(e) = addr.try_send(GetDeviceDataMessage {
                    span_context: span.context(),
                }) {
                    record_error!(&e);
                }
            }
        }
        .into_actor(self);

        ctx.spawn(fut);
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

impl Handler<GetDeviceDataMessage> for DeviceActor {
    type Result = ();

    fn handle(&mut self, message: GetDeviceDataMessage, ctx: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "DeviceActor::Handler<GetDeviceDataMessage>",
            message = GetDeviceDataMessage,
            parent = message.span_context,
            destination = "DeviceActor",
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
        let coordinator_actor_addr = self.coordinator_actor_addr.clone();

        let fut = Self::query_device_usage(device, devices, coordinator_actor_addr)
            .instrument(span.clone())
            .into_actor(self)
            .map(|_, act, _| act.polling = false);

        ctx.spawn(fut);
    }
}
