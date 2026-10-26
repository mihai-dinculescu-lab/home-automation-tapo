use std::time::Duration;

use actix::{
    Actor, ActorContext, Addr, AsyncContext, Context, Handler, WrapFuture, clock::interval,
};
use tapo::ApiClient;
use tracing::{Instrument, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{
    settings::{Device, Tapo},
    system::messages::{DeviceUsageMessage, GetDeviceDataMessage, StopMessage},
    telemetry::{consumer_span, producer_span, record_error},
};

use super::coordinator_actor::CoordinatorActor;

#[derive(Debug)]
pub struct DeviceActor {
    coordinator_actor_addr: Addr<CoordinatorActor>,
    config: Tapo,
    device: Device,
}

impl DeviceActor {
    #[instrument(name = "DeviceActor::new", skip_all, fields(
        device.name = %device.name,
        device.ip_address = %device.ip_address,
    ))]
    pub fn new(
        coordinator_actor_addr: Addr<CoordinatorActor>,
        config: Tapo,
        device: Device,
    ) -> Self {
        Self {
            coordinator_actor_addr,
            config,
            device,
        }
    }

    #[instrument(name = "DeviceActor::query_device_usage", skip_all, fields(
        device.name = %device.name,
        device.ip_address = %device.ip_address,
    ))]
    async fn query_device_usage(
        device: Device,
        client: ApiClient,
        coordinator_actor_addr: Addr<CoordinatorActor>,
    ) {
        let span = tracing::Span::current();

        let result = async {
            let handler = client.p110(device.ip_address.clone()).await?;

            let device_usage = handler.get_device_usage().await?;

            Ok::<_, tapo::Error>(device_usage)
        }
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
        let refresh_rate = Duration::from_secs(self.config.refresh_rate_s);

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

        let device = self.device.clone();
        let client = self.config.client();
        let coordinator_actor_addr = self.coordinator_actor_addr.clone();

        let fut = Self::query_device_usage(device, client, coordinator_actor_addr)
            .instrument(span.clone())
            .into_actor(self);

        ctx.spawn(fut);
    }
}
