use std::mem;

use actix::{
    Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, SpawnHandle, WrapFuture,
};
use tracing::{Instrument, debug, info, instrument};

use crate::{
    system::dimplex::bluetooth,
    system::messages::{HeaterPoweredOffMessage, HeaterPoweredOnMessage, StopMessage},
    telemetry::{consumer_span, record_error},
};

/// Puts one Dimplex heater in Manual after its plug has powered it on, since it starts in
/// Standby and would not heat. It never touches the plug.
#[derive(Debug)]
pub struct DimplexActor {
    heater_name: String,
    bluetooth_address: String,
    /// The setpoint in °C the heater is put in Manual at.
    temperature: u8,
    state: State,
    /// How often the plug has been reported off. An attempt only counts if this is what it was
    /// when the attempt began: actix polls a cancelled future once more, so one that completes
    /// just as it is cancelled would otherwise confirm a heater that has lost its power.
    power_offs: u64,
}

/// What is known of the heater since the thermostat last decided to switch its plug off.
#[derive(Debug)]
enum State {
    /// Nothing: the next "powered on" starts an attempt.
    Unconfirmed,
    /// An attempt is in flight. The thermostat reports the plug on every tick that switches it
    /// on, and one attempt at a time is enough.
    Attempting(SpawnHandle),
    /// The heater was seen in Manual at the setpoint. It is not connected to again, so that the
    /// link is left to the Remo app.
    Confirmed,
}

impl DimplexActor {
    #[instrument(name = "DimplexActor::new", skip_all, fields(
        heater.name = %heater_name,
        heater.bluetooth_address = %bluetooth_address,
    ))]
    pub fn new(heater_name: String, bluetooth_address: String, temperature: u8) -> Self {
        Self {
            heater_name,
            bluetooth_address,
            temperature,
            state: State::Unconfirmed,
            power_offs: 0,
        }
    }

    /// One attempt, whose failure is recorded here and tried again on the next tick that
    /// switches the plug on. Returns whether the heater is now known to be in Manual at the
    /// setpoint.
    #[instrument(name = "DimplexActor::ensure_manual", skip_all, fields(
        heater.name = %heater_name,
        heater.bluetooth_address = %bluetooth_address,
        heater.temperature = temperature,
        heater.follow_up_outcome = tracing::field::Empty,
    ))]
    async fn ensure_manual(
        heater_name: String,
        bluetooth_address: String,
        temperature: u8,
    ) -> bool {
        match bluetooth::ensure_manual(&bluetooth_address, temperature).await {
            Ok(outcome) => {
                tracing::Span::current()
                    .record("heater.follow_up_outcome", tracing::field::display(outcome));
                info!("Heater '{heater_name}' is in Manual at {temperature} °C ({outcome})");
                true
            }
            Err(e) => {
                record_error!(&e);
                false
            }
        }
    }
}

impl Actor for DimplexActor {
    type Context = Context<Self>;

    #[instrument(name = "DimplexActor::started", skip_all, fields(
        heater.name = %self.heater_name,
    ))]
    fn started(&mut self, _: &mut Self::Context) {}

    #[instrument(name = "DimplexActor::stopped", level = "error", skip_all, fields(
        heater.name = %self.heater_name,
    ))]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<HeaterPoweredOnMessage> for DimplexActor {
    type Result = ();

    /// Starts an attempt, unless one is running or the heater has been confirmed since its plug
    /// was last switched off.
    fn handle(&mut self, message: HeaterPoweredOnMessage, ctx: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "DimplexActor::Handler<HeaterPoweredOnMessage>",
            message = HeaterPoweredOnMessage,
            parent = message.span_context,
            destination = "DimplexActor",
            heater.name = %self.heater_name,
        );
        let _enter = span.enter();

        match self.state {
            State::Unconfirmed => {}
            State::Attempting(_) => {
                debug!("The previous attempt is still running");
                return;
            }
            State::Confirmed => {
                debug!("The heater is already confirmed in Manual");
                return;
            }
        }

        let power_offs = self.power_offs;

        // The future is polled outside the handler's span, so it is instrumented with it to be
        // traced under it.
        let fut = Self::ensure_manual(
            self.heater_name.clone(),
            self.bluetooth_address.clone(),
            self.temperature,
        )
        .instrument(span.clone())
        .into_actor(self)
        .map(move |confirmed, act, _| {
            if act.power_offs == power_offs {
                act.state = if confirmed {
                    State::Confirmed
                } else {
                    State::Unconfirmed
                };
            }
        });

        self.state = State::Attempting(ctx.spawn(fut));
    }
}

impl Handler<HeaterPoweredOffMessage> for DimplexActor {
    type Result = ();

    /// Forgets the confirmation, and cancels the attempt in flight, which would otherwise
    /// confirm a heater that has since lost its power.
    fn handle(
        &mut self,
        message: HeaterPoweredOffMessage,
        ctx: &mut Context<Self>,
    ) -> Self::Result {
        let span = consumer_span!(
            name = "DimplexActor::Handler<HeaterPoweredOffMessage>",
            message = HeaterPoweredOffMessage,
            parent = message.span_context,
            destination = "DimplexActor",
            heater.name = %self.heater_name,
        );
        let _enter = span.enter();

        if let State::Attempting(attempt) = mem::replace(&mut self.state, State::Unconfirmed) {
            debug!("The plug was switched off, cancelling the attempt in flight");
            ctx.cancel_future(attempt);
        }

        self.power_offs += 1;
    }
}

impl Handler<StopMessage> for DimplexActor {
    type Result = ();

    /// Stops without touching the heater; an attempt in flight is abandoned.
    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
    }
}
