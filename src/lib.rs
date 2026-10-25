mod domain;
mod migration;
use serde_json::{json, Value};
use tezbar_extension_sdk::{BoxFuture, CoreResult, Extension, ExtensionContext};
const SLUG: &str = "timers";
#[derive(Default)]
pub struct Adapter;
impl Extension for Adapter {
    fn id(&self) -> &'static str {
        "com.tezbar.native.timers"
    }
    fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    fn commands(&self) -> &'static [&'static str] {
        domain::commands()
    }
    fn initialize<'a>(&'a self, ctx: &'a ExtensionContext) -> BoxFuture<'a, CoreResult> {
        Box::pin(async move {
            migration::import_native(ctx).await?;
            domain::prepare_restart(ctx).await?;
            domain::initialize(ctx)?;
            Ok(json!({"ready":true}))
        })
    }
    fn background_activate<'a>(
        &'a self,
        ctx: &'a ExtensionContext,
        params: &'a Value,
    ) -> BoxFuture<'a, CoreResult> {
        Box::pin(async move {
            if params["key"] != SLUG {
                return Err("Unknown background key".into());
            }
            domain::activate(ctx).await
        })
    }
    fn handle<'a>(
        &'a self,
        ctx: &'a ExtensionContext,
        command: &'a str,
        operation: &'a str,
        payload: &'a Value,
        state: &'a mut Value,
    ) -> BoxFuture<'a, CoreResult> {
        Box::pin(domain::dispatch(ctx, command, operation, payload, state))
    }
    fn shutdown<'a>(&'a self, ctx: &'a ExtensionContext) -> BoxFuture<'a, CoreResult> {
        Box::pin(async move {
            domain::shutdown(ctx).await;
            Ok(json!({"stopped":true}))
        })
    }
}
