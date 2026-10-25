//! Native port of installed `timers` package.json and .sc-build/{manageTimers,
//! manageStopwatches,startCustomTimer,timersMenuBar,stopwatchMenuBar,
//! configureMenubarPresets,previewAlertSounds,stopRunningTimer,dismissTimerAlert}.js.
//! Owns scoped storage and explicitly imports host-granted native-v2 state; no JS execution.
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};
use std::time::{Duration, Instant};
use tezbar_extension_sdk as v2_sdk;
use tezbar_extension_sdk::{CoreResult, ExtensionContext as CoreContext};

const SLUG: &str = "timers";
const MAX_SECONDS: u64 = 365 * 24 * 3600;
const PRESETS: &[(&str, u64)] = &[
    ("2M", 120),
    ("5M", 300),
    ("10M", 600),
    ("15M", 900),
    ("30M", 1800),
    ("45M", 2700),
    ("60M", 3600),
    ("90M", 5400),
];
const SOUNDS: &[(&str, &str)] = &[
    ("alarmClock.wav", "Alarm Clock"),
    ("dismemberedWoodpecker.wav", "Dismembered Woodpecker"),
    ("fluteRiff.wav", "Flute Riff"),
    ("levelUp.wav", "Level Up"),
    ("pianoChime.wav", "Piano Chime"),
    ("terminator.wav", "Terminator"),
    ("speak_timer_name", "Speak Timer Name"),
];
const ALL: &[&str] = &[
    "start2MinuteTimer",
    "start5MinuteTimer",
    "start10MinuteTimer",
    "start15MinuteTimer",
    "start25MinuteTimer",
    "start30MinuteTimer",
    "start45MinuteTimer",
    "start60MinuteTimer",
    "start90MinuteTimer",
    "startCustomTimer",
    "manageTimers",
    "timersMenuBar",
    "startStopwatch",
    "manageStopwatches",
    "dismissTimerAlert",
    "stopwatchMenuBar",
    "previewAlertSounds",
    "stopRunningTimer",
    "configureMenubarPresets",
];
const SUBMIT: &str = "timers:submit";
const START_SW: &str = "timers:new-stopwatch";
const DISMISS: &str = "timers:dismiss";
const NEW_TIMER: &str = "timers:new-timer";

pub fn commands() -> &'static [&'static str] {
    #[cfg(any(target_os = "macos", target_os = "windows"))]
    {
        ALL
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        &[]
    }
}

#[derive(Clone, Copy)]
struct Anchor {
    at: Instant,
    elapsed_ms: u64,
}
struct Runtime {
    ctx: CoreContext,
    wake: tokio::sync::watch::Sender<bool>,
    anchors: Mutex<HashMap<String, Anchor>>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    gate: tokio::sync::Mutex<()>,
    retained: AtomicBool,
}
static RUNTIMES: OnceLock<Mutex<HashMap<PathBuf, Arc<Runtime>>>> = OnceLock::new();
fn runtimes() -> &'static Mutex<HashMap<PathBuf, Arc<Runtime>>> {
    RUNTIMES.get_or_init(Default::default)
}
fn profile(ctx: &CoreContext) -> PathBuf {
    ctx.paths.data_dir.join(SLUG)
}
fn runtime(ctx: &CoreContext) -> Option<Arc<Runtime>> {
    runtimes().lock().ok()?.get(&profile(ctx)).cloned()
}
/// Notifications are host-generation resources. A restored ringing alarm must
/// acquire its new generation's notification rather than trust a stale sent bit.
pub async fn prepare_restart(ctx: &CoreContext) -> CoreResult {
    let data = v2_sdk::read_store(ctx, SLUG).await?;
    if !data["timers"].as_array().is_some_and(|a| {
        a.iter()
            .any(|t| t["status"] == "ringing" && t["notificationSent"] == true)
    }) {
        return Ok(data);
    }
    v2_sdk::update_store(ctx, SLUG, |store| {
        for timer in array_mut(store, "timers")? {
            if timer["status"] == "ringing" {
                timer["notificationSent"] = json!(false);
                timer["lastNotificationAttempt"] = json!(0);
            }
        }
        Ok(())
    })
    .await
}
/// Registration is idle: no scheduler is started merely for listing a view.
pub fn initialize(ctx: &CoreContext) -> Result<(), String> {
    let mut all = runtimes().lock().map_err(|e| e.to_string())?;
    all.entry(profile(ctx)).or_insert_with(|| {
        let (wake, _) = tokio::sync::watch::channel(false);
        Arc::new(Runtime {
            ctx: ctx.clone(),
            wake,
            anchors: Mutex::default(),
            worker: Mutex::default(),
            gate: tokio::sync::Mutex::new(()),
            retained: AtomicBool::new(false),
        })
    });
    Ok(())
}
fn pending_timer(t: &Value) -> bool {
    t["status"] == "running"
        || t["status"] == "ringing"
        || (t["status"] == "finished"
            && (t["notificationSent"] != true || t["soundPlayed"] != true))
}
fn timer_work(store: &Value) -> bool {
    store["timers"]
        .as_array()
        .is_some_and(|a| a.iter().any(pending_timer))
        || store["pendingNotificationRemovals"]
            .as_array()
            .is_some_and(|a| !a.is_empty())
}
fn active_work(store: &Value) -> bool {
    timer_work(store)
        || store["stopwatches"]
            .as_array()
            .is_some_and(|a| a.iter().any(|s| s["status"] == "running"))
}
struct RetainRegistration {
    runtime: Option<Arc<Runtime>>,
    armed: bool,
}
impl Drop for RetainRegistration {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(rt) = self.runtime.take() {
            let base = rt.ctx.clone();
            let _=base.clone().spawn(async move {
                loop {
                    let guard=rt.gate.lock().await;
                    let result=async {
                        let store=v2_sdk::read_store(&base,SLUG).await?;
                        if active_work(&store) {reconcile(&base).await} else {
                            release(&base).await?;
                            rt.retained.store(false,Ordering::Release);Ok(())
                        }
                    }.await;
                    drop(guard);
                    if result.is_ok(){break;}
                    tokio::select!{_=base.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(1))=>{}}
                }
            });
        }
    }
}
async fn retain(ctx: &CoreContext) -> Result<(), String> {
    let rt = runtime(ctx);
    if rt
        .as_ref()
        .is_some_and(|r| r.retained.load(Ordering::Acquire))
    {
        return Ok(());
    }
    let mut registration = RetainRegistration {
        runtime: rt.clone(),
        armed: true,
    };
    let ack = ctx
        .host("host.background.retain", json!({"key":SLUG,"durable":true}))
        .await?;
    if ack == false
        || ack.get("ok") == Some(&json!(false))
        || ack.get("retained") == Some(&json!(false))
    {
        return Err("Durable ownership was not acknowledged".into());
    }
    registration.armed = false;
    if let Some(rt) = rt {
        rt.retained.store(true, Ordering::Release);
    }
    Ok(())
}
async fn release(ctx: &CoreContext) -> Result<(), String> {
    let ack = ctx
        .host("host.background.release", json!({"key":SLUG}))
        .await?;
    if ack == false
        || ack.get("ok") == Some(&json!(false))
        || ack.get("retained") == Some(&json!(true))
    {
        return Err("Background release was not acknowledged".into());
    }
    Ok(())
}
/// Retry process-owned reconciliation even when no timer scheduler exists (for
/// example, pausing the last stopwatch after a transient lease-release failure).
async fn reconcile_owned(ctx: &CoreContext, rt: &Runtime) {
    loop {
        let result = {
            let _guard = rt.gate.lock().await;
            reconcile(ctx).await
        };
        match result {
            Ok(()) => break,
            Err(error) => log::warn!("Timer transaction reconciliation: {error}"),
        }
        tokio::select! {
            _ = ctx.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }
    }
}
/// Called under the profile gate, after every mutation, before its acknowledgement.
async fn reconcile(ctx: &CoreContext) -> Result<(), String> {
    let Some(rt) = runtime(ctx) else {
        return Ok(());
    };
    let data = v2_sdk::read_store(ctx, SLUG).await?;
    if active_work(&data) {
        retain(ctx).await?;
    } else if rt.retained.load(Ordering::Acquire) {
        release(ctx).await?;
        rt.retained.store(false, Ordering::Release);
    }
    if timer_work(&data) {
        let mut worker = rt.worker.lock().map_err(|e| e.to_string())?;
        if worker.as_ref().is_none_or(|w| w.is_finished()) {
            let other = rt.clone();
            let base = rt.ctx.clone();
            let rx = rt.wake.subscribe();
            // Explicitly process-owned. shutdown always stops/joins this handle;
            // audio children are kill-on-drop even if shutdown's grace expires.
            *worker = Some(tokio::spawn(
                async move { worker_loop(base, other, rx).await },
            ));
        }
    }
    rt.wake.send_replace(false);
    Ok(())
}
pub async fn activate(ctx: &CoreContext) -> CoreResult {
    initialize(ctx)?;
    crate::migration::import_native(ctx).await?;
    let rt = runtime(ctx).ok_or("Runtime missing")?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let base = rt.ctx.clone();
    base.clone().spawn(async move {
        let _guard = rt.gate.lock().await;
        let result = async {
            // Explicitly remove stale durable activations with no remaining work.
            let store = v2_sdk::read_store(&base, SLUG).await?;
            if active_work(&store) {
                retain(&base).await?;
            } else {
                release(&base).await?;
                rt.retained.store(false, Ordering::Release);
            }
            reconcile(&base).await?;
            Ok(json!({"ready":true}))
        }
        .await;
        let _ = tx.send(result);
    })?;
    rx.await
        .map_err(|_| "Timer activation cancelled".to_string())?
}
pub async fn dispatch(
    ctx: &CoreContext,
    command: &str,
    operation: &str,
    payload: &Value,
    state: &mut Value,
) -> CoreResult {
    if operation == "extension:dispose-session" {
        return Ok(v2_sdk::unchanged());
    }
    let rt = runtime(ctx).ok_or("Timers process not initialized")?;
    let _guard = if operation == "extension:refresh-session" {
        match rt.gate.try_lock() {
            Ok(guard) => guard,
            Err(_) => return Ok(v2_sdk::unchanged()),
        }
    } else {
        rt.gate.lock().await
    };
    let result = handle(ctx, command, operation, payload, state).await;
    reconcile(ctx).await?;
    if result.as_ref().is_ok_and(|v| v["mode"] == "no-view")
        && prefs(payload)["closeWindowOnTimerStart"] == true
        && command.starts_with("start")
    {
        ctx.host("host.window.visibility", json!({"visible":false}))
            .await?;
    }
    result
}
pub async fn shutdown(ctx: &CoreContext) {
    let rt = runtimes()
        .lock()
        .ok()
        .and_then(|mut all| all.remove(&profile(ctx)));
    if let Some(rt) = rt {
        rt.wake.send_replace(true);
        let task = rt.worker.lock().ok().and_then(|mut w| w.take());
        if let Some(mut task) = task {
            if tokio::time::timeout(Duration::from_millis(800), &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
            }
        }
        // Keep durable registration if persisted work remains: normal app shutdown
        // is followed by background.activate on the next host start.
        if let Ok(store) = v2_sdk::read_store(ctx, SLUG).await {
            if !active_work(&store) && rt.retained.load(Ordering::Acquire) {
                let _ = ctx
                    .host("host.background.release", json!({"key":SLUG}))
                    .await;
            }
        }
    }
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
fn anchor(rt: &Runtime, id: &str, elapsed_ms: u64) {
    if let Ok(mut anchors) = rt.anchors.lock() {
        anchors.insert(
            id.to_owned(),
            Anchor {
                at: Instant::now(),
                elapsed_ms,
            },
        );
    }
}
fn unanchor(rt: &Runtime, id: &str) {
    if let Ok(mut anchors) = rt.anchors.lock() {
        anchors.remove(id);
    }
}
fn elapsed(rt: Option<&Runtime>, id: &str, persisted: u64, since: u64, now: u64) -> u64 {
    if let Some(rt) = rt {
        if let Ok(mut anchors) = rt.anchors.lock() {
            let a = anchors.entry(id.to_owned()).or_insert_with(|| Anchor {
                at: Instant::now(),
                elapsed_ms: persisted.saturating_add(now.saturating_sub(since)),
            });
            return a
                .elapsed_ms
                .saturating_add(a.at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
        }
    }
    persisted.saturating_add(now.saturating_sub(since))
}
fn remaining(t: &Value, rt: Option<&Runtime>, now: u64) -> u64 {
    if t["status"] == "ringing" || t["status"] == "finished" {
        return 0;
    }
    let saved = t["elapsedMs"].as_u64().unwrap_or(0);
    let spent = if t["status"] == "paused" {
        saved
    } else {
        elapsed(
            rt,
            t["id"].as_str().unwrap_or(""),
            saved,
            t["resumedAt"].as_u64().unwrap_or(now),
            now,
        )
    };
    t["durationMs"].as_u64().unwrap_or(0).saturating_sub(spent)
}
fn stopwatch_elapsed(sw: &Value, rt: Option<&Runtime>, now: u64) -> u64 {
    let saved = sw["elapsedMs"].as_u64().unwrap_or(0);
    if sw["status"] == "paused" {
        saved
    } else {
        elapsed(
            rt,
            sw["id"].as_str().unwrap_or(""),
            saved,
            sw["resumedAt"].as_u64().unwrap_or(now),
            now,
        )
    }
}
fn checked_duration(hours: &Value, minutes: &Value, seconds: &Value) -> Result<u64, String> {
    fn part(v: &Value) -> Result<u64, String> {
        let input = match v {
            Value::Null => "0".to_owned(),
            Value::String(s) if s.trim().is_empty() => "0".to_owned(),
            Value::String(s) => s.trim().to_owned(),
            Value::Number(n) => n.to_string(),
            _ => return Err("Timer duration must contain whole numbers".into()),
        };
        if !input.bytes().all(|b| b.is_ascii_digit()) {
            return Err("Timer duration must contain nonnegative whole numbers".into());
        }
        input
            .parse::<u64>()
            .map_err(|_| "Timer duration is too large".to_owned())
    }
    let (hours, minutes, seconds) = (part(hours)?, part(minutes)?, part(seconds)?);
    let duration = hours
        .checked_mul(3600)
        .and_then(|v| v.checked_add(minutes.checked_mul(60)?))
        .and_then(|v| v.checked_add(seconds))
        .ok_or("Timer duration is too large")?;
    if duration == 0 || duration > MAX_SECONDS {
        return Err("Timer duration must be between 1 second and 365 days".into());
    }
    Ok(duration)
}
fn title(v: &Value, default: &str) -> Result<String, String> {
    let name = v.as_str().unwrap_or("");
    if name.len() > 200 {
        return Err("Name must be at most 200 characters".into());
    }
    Ok(if name.trim().is_empty() {
        default.to_string()
    } else {
        name.to_owned()
    })
}
fn sound(v: &Value) -> Result<String, String> {
    let name = v.as_str().unwrap_or("default");
    if name == "default" || SOUNDS.iter().any(|(key, _)| key == &name) {
        Ok(name.to_owned())
    } else {
        Err("Unknown timer sound".into())
    }
}
fn selected_sound(preferences: &Value, selection: &str) -> Result<String, String> {
    if selection != "default" {
        return Ok(selection.into());
    }
    // Installed manifest's historic default is "Submarine", absent from its assets.
    let preference = preferences["selectedSound"]
        .as_str()
        .unwrap_or("alarmClock.wav");
    let preference = if preference == "Submarine" {
        "alarmClock.wav"
    } else {
        preference
    };
    sound(&json!(preference)).map(|value| {
        if value == "default" {
            "alarmClock.wav".into()
        } else {
            value
        }
    })
}
fn validate_volume(preferences: &Value) -> Result<f32, String> {
    let raw = preferences["volumeSetting"]
        .as_str()
        .unwrap_or("1")
        .replace(',', ".");
    let volume = raw
        .parse::<f32>()
        .map_err(|_| "Timer alert volume must be a number".to_owned())?;
    if !volume.is_finite() || !(0.0..=5.0).contains(&volume) {
        return Err("Timer alert volume must be between 0 and 5".into());
    }
    Ok(volume)
}
fn array_mut<'a>(store: &'a mut Value, key: &str) -> Result<&'a mut Vec<Value>, String> {
    let obj = store.as_object_mut().ok_or("Invalid timer storage")?;
    let value = obj.entry(key).or_insert_with(|| json!([]));
    value
        .as_array_mut()
        .ok_or_else(|| format!("Corrupt timers storage: {key}"))
}
fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn stamp() -> u64 {
    now_ms()
}
fn prefs(payload: &Value) -> &Value {
    &payload["preferences"]
}
fn args(payload: &Value) -> &Value {
    &payload["argumentValues"]
}
fn form(payload: &Value) -> &Value {
    &payload["formValues"]
}
fn input_id(action: &str, prefix: &str) -> Option<String> {
    action
        .strip_prefix(prefix)
        .filter(|id| !id.is_empty() && id.len() <= 100)
        .map(str::to_owned)
}
fn format_time(milliseconds: u64) -> String {
    let seconds = milliseconds / 1000;
    let h = seconds / 3600;
    if h > 0 {
        format!("{h}:{:02}:{:02}", seconds / 60 % 60, seconds % 60)
    } else {
        format!("{:02}:{:02}", seconds / 60, seconds % 60)
    }
}
fn item(id: &str, title: &str, subtitle: &str, section: &str, ids: &[String]) -> Value {
    json!({"type":"List.Section","props":{"title":section},"children":[{"type":"List.Item","props":{"id":id,"title":title,"subtitle":subtitle,"actionIds":ids}}]})
}
fn action(id: String, title: &str) -> Value {
    json!({"id":id,"title":title})
}
fn list(title: &str, children: Vec<Value>, actions: Vec<Value>) -> Value {
    v2_sdk::view(title, json!({"type":"List","children":children}), actions)
}
fn fields(names: &[(&str, &str, &str, Value)]) -> Vec<Value> {
    names.iter().map(|(id, label, hint, value)| json!({"type":"Form.TextField","props":{"id":id,"title":label,"placeholder":hint,"value":value}})).collect()
}
fn dropdown() -> Value {
    let mut options =
        vec![json!({"type":"Form.Dropdown.Item","props":{"title":"Default","value":"default"}})];
    for (name, label) in SOUNDS {
        options.push(json!({"type":"Form.Dropdown.Item","props":{"title":label,"value":name}}));
    }
    json!({"type":"Form.Dropdown","props":{"id":"selectedSound","title":"Sound","value":"default"},"children":options})
}
fn custom_form(payload: &Value, state: &Value) -> Value {
    let mut input = fields(&[
        (
            "hours",
            "Hours",
            "0",
            state["hours"]
                .as_str()
                .or_else(|| args(payload)["hours"].as_str())
                .unwrap_or("")
                .into(),
        ),
        (
            "minutes",
            "Minutes",
            "00",
            state["minutes"]
                .as_str()
                .or_else(|| args(payload)["minutes"].as_str())
                .unwrap_or("")
                .into(),
        ),
        (
            "seconds",
            "Seconds",
            "00",
            state["seconds"]
                .as_str()
                .or_else(|| args(payload)["seconds"].as_str())
                .unwrap_or("")
                .into(),
        ),
    ]);
    if prefs(payload)["newTimerInputOrder"] == "ssmmhh" {
        input.reverse();
    }
    input.push(dropdown());
    input.extend(fields(&[("name", "Name", "Pour Tea", json!(""))]));
    input.push(json!({"type":"Form.Checkbox","props":{"id":"willBeSaved","title":"Save as preset","label":"Save as preset","value":false}}));
    v2_sdk::view(
        "Start Custom Timer",
        json!({"type":"Form","children":input}),
        vec![json!({"id":SUBMIT,"title":"Start Custom Timer","kind":"submit-form"})],
    )
}
fn new_timer_form(payload: &Value, state: &Value) -> Value {
    let mut result = custom_form(payload, state);
    if let Some(actions) = result["actions"].as_array_mut() {
        actions.push(action("timers:back".into(), "Back to Timers"));
    }
    result
}

/// Once durable ownership is acknowledged, a store transaction and its scheduler
/// reconciliation belong to the process, even if the originating view disappears.
/// The response is never replayed; cancellation can only abandon its receiver.
async fn update_owned<F, A>(ctx: &CoreContext, change: F, after: A) -> CoreResult
where
    F: FnOnce(&mut Value) -> Result<(), String> + Send + 'static,
    A: FnOnce(&Runtime) + Send + 'static,
{
    if let Some(rt) = runtime(ctx) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let base = rt.ctx.clone();
        let other = rt.clone();
        base.clone().spawn(async move {
            let result = v2_sdk::update_store(&base, SLUG, change).await;
            if result.is_ok() {
                after(&other);
                other.wake.send_replace(false);
            }
            let _ = tx.send(result);
            reconcile_owned(&base, &other).await;
        })?;
        rx.await
            .map_err(|_| "Timer process transaction cancelled".to_string())?
    } else {
        v2_sdk::update_store(ctx, SLUG, change).await
    }
}

async fn create_timer(
    ctx: &CoreContext,
    duration: u64,
    name: String,
    selection: &str,
    preferences: &Value,
    save: bool,
) -> Result<String, String> {
    let volume = validate_volume(preferences)?;
    let selected = selected_sound(preferences, selection)?;
    if selected != "speak_timer_name" {
        sound_path(ctx, &selected)?;
    }
    #[cfg(target_os = "windows")]
    if volume != 1.0 {
        return Err("Windows sound playback cannot set the requested volume".into());
    }
    retain(ctx).await?; // durable acknowledgement precedes the first active store write
    let id = new_id();
    let now = stamp();
    let ringing = preferences["ringContinuously"].as_bool().unwrap_or(false);
    let timer = json!({"id":id,"name":name,"durationMs":duration*1000,"elapsedMs":0,"resumedAt":now,"startedAt":now,"status":"running","sound":selected,"volume":volume,"ringContinuously":ringing});
    let preset = json!({"id":new_id(),"name":name,"durationMs":duration*1000,"sound":selection,"visible":true});
    let anchored_id = id.clone();
    update_owned(
        ctx,
        move |store| {
            array_mut(store, "timers")?.push(timer);
            if save {
                array_mut(store, "presets")?.push(preset);
            }
            Ok(())
        },
        move |rt| anchor(rt, &anchored_id, 0),
    )
    .await?;
    Ok(format!(
        "Timer \"{name}\" started for {}",
        format_time(duration * 1000)
    ))
}
async fn create_stopwatch(ctx: &CoreContext, name: String) -> Result<String, String> {
    retain(ctx).await?;
    let id = new_id();
    let now = stamp();
    let sw = json!({"id":id,"name":name,"startedAt":now,"resumedAt":now,"elapsedMs":0,"status":"running"});
    let anchored_id = id.clone();
    update_owned(
        ctx,
        move |store| {
            array_mut(store, "stopwatches")?.push(sw);
            Ok(())
        },
        move |rt| anchor(rt, &anchored_id, 0),
    )
    .await?;
    Ok(format!("Stopwatch \"{name}\" started"))
}

pub async fn handle(
    ctx: &CoreContext,
    command: &str,
    operation: &str,
    payload: &Value,
    state: &mut Value,
) -> CoreResult {
    if !commands().contains(&command) {
        return Err("Timers command is unavailable on this platform".into());
    }
    match operation {
        "extension:run-command" => {
            if let Some(minutes) = command
                .strip_prefix("start")
                .and_then(|s| s.strip_suffix("MinuteTimer"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                if ![2, 5, 10, 15, 25, 30, 45, 60, 90].contains(&minutes) {
                    return Err("Unknown timer preset".into());
                }
                return Ok(v2_sdk::no_view(
                    &create_timer(
                        ctx,
                        minutes * 60,
                        "Untitled".into(),
                        "default",
                        prefs(payload),
                        false,
                    )
                    .await?,
                ));
            }
            match command {
                "startStopwatch" => Ok(v2_sdk::no_view(
                    &create_stopwatch(ctx, title(&args(payload)["name"], "Untitled")?).await?,
                )),
                "stopRunningTimer" => {
                    let data = v2_sdk::read_store(ctx, SLUG).await?;
                    let now = stamp();
                    let rt = runtime(ctx);
                    let target = data["timers"]
                        .as_array()
                        .and_then(|arr| arr.iter().min_by_key(|t| remaining(t, rt.as_deref(), now)))
                        .ok_or("No timers found")?;
                    let name = target["name"].as_str().unwrap_or("Untitled").to_string();
                    mutate(
                        ctx,
                        "timers",
                        target["id"].as_str().ok_or("Invalid timer")?,
                        "stop",
                        None,
                    )
                    .await?;
                    Ok(v2_sdk::no_view(&format!("Timer \"{name}\" stopped")))
                }
                "dismissTimerAlert" => {
                    let data = v2_sdk::read_store(ctx, SLUG).await?;
                    let target = data["timers"]
                        .as_array()
                        .and_then(|arr| arr.iter().find(|t| t["status"] == "ringing"))
                        .ok_or("No ringing timer alerts found")?;
                    mutate(
                        ctx,
                        "timers",
                        target["id"].as_str().ok_or("Invalid timer")?,
                        "dismiss",
                        None,
                    )
                    .await?;
                    Ok(v2_sdk::no_view("Ringing timer alert dismissed"))
                }
                "startCustomTimer" => {
                    let has_args = ["hours", "minutes", "seconds"]
                        .iter()
                        .any(|key| args(payload)[key].as_str().is_some_and(|v| !v.is_empty()));
                    if has_args && prefs(payload)["customTimerFormBypass"] == true {
                        let seconds = checked_duration(
                            &args(payload)["hours"],
                            &args(payload)["minutes"],
                            &args(payload)["seconds"],
                        )?;
                        return Ok(v2_sdk::no_view(
                            &create_timer(
                                ctx,
                                seconds,
                                "Untitled".into(),
                                "default",
                                prefs(payload),
                                false,
                            )
                            .await?,
                        ));
                    }
                    Ok(custom_form(payload, state))
                }
                _ => render(ctx, command, payload, state).await,
            }
        }
        "extension:invoke-action" => {
            let action = payload["actionId"].as_str().ok_or("Missing timer action")?;
            if command == "startCustomTimer" && action == SUBMIT {
                let f = form(payload);
                let seconds = checked_duration(&f["hours"], &f["minutes"], &f["seconds"])?;
                let name = title(&f["name"], "Untitled")?;
                let sound = sound(&f["selectedSound"])?;
                let save = f["willBeSaved"].as_bool().unwrap_or(false);
                return Ok(v2_sdk::no_view(
                    &create_timer(ctx, seconds, name, &sound, prefs(payload), save).await?,
                ));
            }
            if !matches!(
                command,
                "manageTimers"
                    | "timersMenuBar"
                    | "manageStopwatches"
                    | "stopwatchMenuBar"
                    | "previewAlertSounds"
                    | "configureMenubarPresets"
            ) {
                return Err("This command has no actions".into());
            }
            if action == "timers:back"
                && matches!(
                    command,
                    "manageTimers" | "timersMenuBar" | "manageStopwatches" | "stopwatchMenuBar"
                )
            {
                *state = json!({});
                return render(ctx, command, payload, state).await;
            }
            if action == NEW_TIMER && matches!(command, "manageTimers" | "timersMenuBar") {
                *state = json!({"screen":"new-timer"});
                return Ok(new_timer_form(payload, state));
            }
            if state["screen"] == "rename" && action == "timers:submit-rename" {
                let collection = state["collection"]
                    .as_str()
                    .ok_or("Missing rename target")?;
                let id = state["target"].as_str().ok_or("Missing rename target")?;
                mutate(ctx, collection, id, "rename", form(payload).get("newName")).await?;
                *state = json!({});
                return render(ctx, command, payload, state).await;
            }
            if state["screen"] == "new-timer" && action == SUBMIT {
                let f = form(payload);
                let secs = checked_duration(&f["hours"], &f["minutes"], &f["seconds"])?;
                create_timer(
                    ctx,
                    secs,
                    title(&f["name"], "Untitled")?,
                    &sound(&f["selectedSound"])?,
                    prefs(payload),
                    f["willBeSaved"] == true,
                )
                .await?;
                *state = json!({});
                return render(ctx, command, payload, state).await;
            }
            if let Some(name) = action.strip_prefix("timers:fixed:") {
                if !matches!(command, "timersMenuBar") {
                    return Err("Action is unavailable in this command".into());
                }
                let seconds = PRESETS
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, seconds)| *seconds)
                    .ok_or("Unknown preset")?;
                create_timer(
                    ctx,
                    seconds,
                    "Untitled".into(),
                    "default",
                    prefs(payload),
                    false,
                )
                .await?;
            } else if action == START_SW
                && matches!(command, "manageStopwatches" | "stopwatchMenuBar")
            {
                create_stopwatch(ctx, "Untitled".into()).await?;
            } else if action == DISMISS && matches!(command, "manageTimers" | "timersMenuBar") {
                let data = v2_sdk::read_store(ctx, SLUG).await?;
                let target = data["timers"]
                    .as_array()
                    .and_then(|arr| arr.iter().find(|t| t["status"] == "ringing"))
                    .ok_or("No ringing timer alerts found")?;
                mutate(
                    ctx,
                    "timers",
                    target["id"].as_str().ok_or("Invalid timer")?,
                    "dismiss",
                    None,
                )
                .await?;
            } else if let Some(id) = input_id(action, "timers:start-preset:") {
                if !matches!(command, "manageTimers" | "timersMenuBar") {
                    return Err("Preset action is unavailable here".into());
                }
                let data = v2_sdk::read_store(ctx, SLUG).await?;
                let preset = data["presets"]
                    .as_array()
                    .and_then(|arr| arr.iter().find(|p| p["id"] == id))
                    .ok_or("Preset no longer exists")?;
                let seconds = preset["durationMs"].as_u64().ok_or("Invalid preset")? / 1000;
                if seconds == 0 || seconds > MAX_SECONDS {
                    return Err("Invalid preset duration".into());
                }
                create_timer(
                    ctx,
                    seconds,
                    title(&preset["name"], "Untitled")?,
                    &sound(&preset["sound"])?,
                    prefs(payload),
                    false,
                )
                .await?;
            } else if let Some(id) = input_id(action, "timers:play:") {
                if command != "previewAlertSounds" {
                    return Err("Sound preview is unavailable here".into());
                }
                let selected = sound(&json!(id))?;
                play_once(
                    ctx,
                    &selected,
                    "Untitled Timer",
                    validate_volume(prefs(payload))?,
                )
                .await?;
            } else if let Some(id) = input_id(action, "timers:toggle-fixed:") {
                if command != "configureMenubarPresets"
                    || !PRESETS.iter().any(|(key, _)| *key == id)
                {
                    return Err("Invalid default preset".into());
                }
                v2_sdk::update_store(ctx, SLUG, move |store| {
                    let o = store.as_object_mut().ok_or("Invalid timer storage")?;
                    let flags = o
                        .entry("presetVisibility")
                        .or_insert_with(|| json!({}))
                        .as_object_mut()
                        .ok_or("Invalid preset visibility")?;
                    let current = flags.get(&id).and_then(Value::as_bool).unwrap_or(true);
                    flags.insert(id, json!(!current));
                    Ok(())
                })
                .await?;
            } else {
                let mut matched = false;
                for (prefix, collection, change) in [
                    ("timers:pause:", "timers", "pause"),
                    ("timers:resume:", "timers", "resume"),
                    ("timers:stop:", "timers", "stop"),
                    ("timers:dismiss:", "timers", "dismiss"),
                    ("timers:save:", "timers", "save"),
                    ("timers:rename:", "timers", "rename"),
                    ("timers:sw-pause:", "stopwatches", "pause"),
                    ("timers:sw-resume:", "stopwatches", "resume"),
                    ("timers:sw-stop:", "stopwatches", "stop"),
                    ("timers:sw-restart:", "stopwatches", "restart"),
                    ("timers:sw-rename:", "stopwatches", "rename"),
                    ("timers:sw-copy:", "stopwatches", "copy"),
                    ("timers:delete-preset:", "presets", "delete"),
                    ("timers:rename-preset:", "presets", "rename"),
                    ("timers:toggle-preset:", "presets", "toggle"),
                ] {
                    if let Some(id) = input_id(action, prefix) {
                        let allowed = match collection {
                            "stopwatches" => {
                                matches!(command, "manageStopwatches" | "stopwatchMenuBar")
                            }
                            "presets" if change == "toggle" => command == "configureMenubarPresets",
                            "presets" => command == "manageTimers",
                            _ => matches!(command, "manageTimers" | "timersMenuBar"),
                        };
                        if !allowed {
                            return Err("Action is unavailable in this command".into());
                        }
                        if change == "rename" {
                            let data = v2_sdk::read_store(ctx, SLUG).await?;
                            if !data[collection]
                                .as_array()
                                .is_some_and(|items| items.iter().any(|v| v["id"] == id))
                            {
                                return Err("Rename target no longer exists".into());
                            }
                            *state = json!({"screen":"rename","collection":collection,"target":id});
                            return render(ctx, command, payload, state).await;
                        }
                        let copied = mutate(ctx, collection, &id, change, None).await?;
                        if let Some(text) = copied {
                            if change == "copy" || prefs(payload)["copyOnSwStop"] == true {
                                v2_sdk::copy(ctx, &text).await?;
                            }
                        }
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return Err("Unknown timer action".into());
                }
            }
            render(ctx, command, payload, state).await
        }
        "extension:refresh-session" | "extension:search-text-changed" | "extension:load-more" => {
            if command == "startCustomTimer" {
                Ok(custom_form(payload, state))
            } else if state["screen"] == "new-timer" {
                Ok(new_timer_form(payload, state))
            } else {
                render(ctx, command, payload, state).await
            }
        }
        "extension:dispose-session" => Ok(v2_sdk::unchanged()),
        _ => Err("Unsupported timers operation".into()),
    }
}

async fn mutate(
    ctx: &CoreContext,
    collection: &str,
    id: &str,
    change: &str,
    new_name: Option<&Value>,
) -> Result<Option<String>, String> {
    let key = collection.to_owned();
    let target = id.to_owned();
    let verb = change.to_owned();
    let requested_name = if verb == "rename" {
        let name = title(new_name.unwrap_or(&Value::Null), "")?;
        if name.trim().is_empty() {
            return Err("Enter a new name".into());
        }
        Some(name)
    } else {
        None
    };
    let now = stamp();
    let rt = runtime(ctx);
    let before = v2_sdk::read_store(ctx, SLUG).await?;
    let original = before[collection]
        .as_array()
        .and_then(|items| items.iter().find(|item| item["id"] == target))
        .ok_or("Timer, stopwatch or preset no longer exists")?;
    let spent = if collection == "stopwatches" {
        stopwatch_elapsed(original, rt.as_deref(), now)
    } else {
        original["durationMs"]
            .as_u64()
            .unwrap_or(0)
            .saturating_sub(remaining(original, rt.as_deref(), now))
    };
    let copied = if collection == "stopwatches" && ["copy", "stop", "restart"].contains(&change) {
        Some(format_time(spent))
    } else {
        None
    };
    if change == "copy" {
        return Ok(copied);
    }
    let old_name = original["name"].as_str().unwrap_or("Untitled").to_string();
    let sound = original["sound"].as_str().unwrap_or("default").to_string();
    let duration = original["durationMs"].as_u64().unwrap_or(0);
    let needs_removal = collection == "timers"
        && (original["notificationSent"] == true
            || original["status"] == "ringing"
            || original["status"] == "finished");
    if matches!(change, "resume" | "pause")
        && original["status"]
            != (if change == "pause" {
                "running"
            } else {
                "paused"
            })
    {
        return Err("Timer is not in a state that can be changed".into());
    }
    if change == "save" && (collection != "timers" || duration == 0) {
        return Err("Cannot save timer preset".into());
    }
    if change == "dismiss" && original["status"] != "ringing" {
        return Err("Timer is not ringing".into());
    }
    if collection == "presets" && !["delete", "rename", "toggle"].contains(&change) {
        return Err("Unsupported preset action".into());
    }
    if matches!(change, "resume" | "restart")
        || (needs_removal && matches!(change, "stop" | "delete" | "dismiss"))
    {
        retain(ctx).await?;
    }
    let replacement_id = new_id();
    let replacement_id_for_store = replacement_id.clone();
    let preset_id = new_id();
    let anchor_id = id.to_owned();
    let anchor_verb = change.to_owned();
    update_owned(ctx, move |store| {
        if needs_removal && matches!(verb.as_str(),"stop"|"delete"|"dismiss") {
            array_mut(store,"pendingNotificationRemovals")?.push(json!({"id":target,"lastAttempt":0}));
        }
        if verb == "save" {
            array_mut(store, "presets")?.push(json!({"id":preset_id,"name":old_name,"durationMs":duration,"sound":sound,"visible":true}));
            return Ok(());
        }
        let items = array_mut(store, &key)?;
        let position = items.iter().position(|item| item["id"] == target).ok_or("Timer, stopwatch or preset no longer exists")?;
        match verb.as_str() {
            "stop" | "delete" | "dismiss" => { items.remove(position); }
            "rename" => { items[position]["name"] = json!(requested_name); }
            "toggle" => { let visible = items[position]["visible"].as_bool().unwrap_or(true); items[position]["visible"] = json!(!visible); }
            "pause" => { items[position]["elapsedMs"] = json!(spent); items[position]["status"] = json!("paused"); }
            "resume" => { items[position]["resumedAt"] = json!(now); items[position]["status"] = json!("running"); }
            "restart" => {
                let name = items[position]["name"].clone();
                items.remove(position);
                items.push(json!({"id":replacement_id_for_store,"name":name,"startedAt":now,"resumedAt":now,"elapsedMs":0,"status":"running"}));
            }
            _ => return Err("Unsupported timer action".into()),
        }
        Ok(())
    },move |rt| {
        if matches!(anchor_verb.as_str(),"stop"|"delete"|"pause"|"restart"|"dismiss") {unanchor(rt,&anchor_id);}
        if anchor_verb=="resume" {anchor(rt,&anchor_id,spent);}
        if anchor_verb=="restart" {anchor(rt,&replacement_id,0);}
    }).await?;
    if needs_removal && matches!(change, "stop" | "delete" | "dismiss") {
        remove_notifications(ctx, stamp()).await?;
    }
    Ok(copied)
}

async fn render(ctx: &CoreContext, command: &str, payload: &Value, state: &Value) -> CoreResult {
    if state["screen"] == "rename" {
        return Ok(v2_sdk::view(
            "Rename",
            json!({"type":"Form","children":fields(&[("newName","New name","Enter name",json!(""))])}),
            vec![
                json!({"id":"timers:submit-rename","title":"Rename","kind":"submit-form"}),
                action("timers:back".into(), "Back to List"),
            ],
        ));
    }
    let data = v2_sdk::read_store(ctx, SLUG).await?;
    let rt = runtime(ctx);
    let now = stamp();
    let mut children = Vec::new();
    let mut actions = Vec::new();
    match command {
        "manageTimers" | "timersMenuBar" => {
            let mut timers = data["timers"].as_array().cloned().unwrap_or_default();
            timers.sort_by_key(|t| remaining(t, rt.as_deref(), now));
            for timer in &timers {
                let Some(id) = timer["id"].as_str() else {
                    continue;
                };
                let status = timer["status"].as_str().unwrap_or("running");
                let remaining_ms = remaining(timer, rt.as_deref(), now);
                let mut row_actions = Vec::new();
                if status == "running" || status == "paused" {
                    let (verb, label) = if status == "running" {
                        ("pause", "Pause Timer")
                    } else {
                        ("resume", "Resume Timer")
                    };
                    let key = format!("timers:{verb}:{id}");
                    row_actions.push(key.clone());
                    actions.push(action(key, label));
                }
                if status == "ringing" {
                    let key = format!("timers:dismiss:{id}");
                    row_actions.push(key.clone());
                    actions.push(action(key, "Dismiss Alert"));
                }
                let key = format!("timers:stop:{id}");
                row_actions.push(key.clone());
                actions.push(action(key, "Stop Timer"));
                if command == "manageTimers" {
                    let key = format!("timers:save:{id}");
                    row_actions.push(key.clone());
                    actions.push(action(key, "Save as Preset"));
                    let key = format!("timers:rename:{id}");
                    row_actions.push(key.clone());
                    actions.push(action(key, "Rename Timer"));
                }
                let label = match status {
                    "paused" => "Paused",
                    "ringing" => "Ringing",
                    "finished" => "Finished",
                    _ => "Running",
                };
                children.push(item(
                    id,
                    timer["name"].as_str().unwrap_or("Untitled"),
                    &format!("{} left · {label}", format_time(remaining_ms)),
                    "Timers",
                    &row_actions,
                ));
            }
            let mut presets = data["presets"].as_array().cloned().unwrap_or_default();
            presets.sort_by_key(|p| p["durationMs"].as_u64().unwrap_or(0));
            for preset in &presets {
                let Some(id) = preset["id"].as_str() else {
                    continue;
                };
                if command == "timersMenuBar" && preset["visible"] == false {
                    continue;
                }
                let mut ids = vec![format!("timers:start-preset:{id}")];
                actions.push(action(ids[0].clone(), "Start Timer"));
                if command == "manageTimers" {
                    for (verb, label) in [
                        ("rename-preset", "Rename Preset"),
                        ("delete-preset", "Delete Preset"),
                    ] {
                        let key = format!("timers:{verb}:{id}");
                        ids.push(key.clone());
                        actions.push(action(key, label));
                    }
                }
                children.push(item(
                    id,
                    preset["name"].as_str().unwrap_or("Untitled"),
                    &format_time(preset["durationMs"].as_u64().unwrap_or(0)),
                    "Saved Presets",
                    &ids,
                ));
            }
            if command == "timersMenuBar" {
                for (key, seconds) in PRESETS {
                    if data["presetVisibility"][*key].as_bool() == Some(false) {
                        continue;
                    }
                    let action_id = format!("timers:fixed:{key}");
                    actions.push(action(action_id.clone(), "Start Timer"));
                    children.push(item(
                        key,
                        &format!("{} Minute Timer", seconds / 60),
                        &format_time(seconds * 1000),
                        "Default Presets",
                        &[action_id],
                    ));
                }
            }
            actions.push(action(NEW_TIMER.into(), "New Custom Timer"));
            if timers.iter().any(|t| t["status"] == "ringing") {
                actions.push(action(DISMISS.into(), "Dismiss Ringing Timer"));
            }
        }
        "manageStopwatches" | "stopwatchMenuBar" => {
            let mut watches = data["stopwatches"].as_array().cloned().unwrap_or_default();
            watches.sort_by_key(|sw| stopwatch_elapsed(sw, rt.as_deref(), now));
            for sw in &watches {
                let Some(id) = sw["id"].as_str() else {
                    continue;
                };
                let (verb, label) = if sw["status"] == "paused" {
                    ("resume", "Resume Stopwatch")
                } else {
                    ("pause", "Pause Stopwatch")
                };
                let mut ids = Vec::new();
                for (verb, label) in [
                    (verb, label),
                    ("copy", "Copy Current Time"),
                    ("restart", "Restart Stopwatch"),
                    ("stop", "Stop Stopwatch"),
                    ("rename", "Rename Stopwatch"),
                ] {
                    if command == "stopwatchMenuBar" && verb == "rename" {
                        continue;
                    }
                    let key = format!("timers:sw-{verb}:{id}");
                    ids.push(key.clone());
                    actions.push(action(key, label));
                }
                let status = if sw["status"] == "paused" {
                    "Paused"
                } else {
                    "Running"
                };
                children.push(item(
                    id,
                    sw["name"].as_str().unwrap_or("Untitled"),
                    &format!(
                        "{} elapsed · {status}",
                        format_time(stopwatch_elapsed(sw, rt.as_deref(), now))
                    ),
                    "Stopwatches",
                    &ids,
                ));
            }
            actions.push(action(START_SW.into(), "Start Stopwatch"));
        }
        "previewAlertSounds" => {
            for (name, label) in SOUNDS {
                let key = format!("timers:play:{name}");
                children.push(item(
                    name,
                    label,
                    if payload["preferences"]["selectedSound"] == *name {
                        "Selected sound"
                    } else {
                        ""
                    },
                    "Sounds",
                    std::slice::from_ref(&key),
                ));
                actions.push(action(key, "Play Sound"));
            }
        }
        "configureMenubarPresets" => {
            for (key, seconds) in PRESETS {
                let visible = data["presetVisibility"][*key].as_bool().unwrap_or(true);
                let id = format!("timers:toggle-fixed:{key}");
                actions.push(action(
                    id.clone(),
                    if visible {
                        "Hide in Menu Bar"
                    } else {
                        "Show in Menu Bar"
                    },
                ));
                children.push(item(
                    key,
                    &format!("{} Minute Timer", seconds / 60),
                    if visible { "Visible" } else { "Hidden" },
                    "Default Presets",
                    &[id],
                ));
            }
            for preset in data["presets"].as_array().into_iter().flatten() {
                let Some(id) = preset["id"].as_str() else {
                    continue;
                };
                let visible = preset["visible"].as_bool().unwrap_or(true);
                let key = format!("timers:toggle-preset:{id}");
                actions.push(action(
                    key.clone(),
                    if visible {
                        "Hide in Menu Bar"
                    } else {
                        "Show in Menu Bar"
                    },
                ));
                children.push(item(
                    id,
                    preset["name"].as_str().unwrap_or("Untitled"),
                    if visible { "Visible" } else { "Hidden" },
                    "Custom Presets",
                    &[key],
                ));
            }
        }
        _ => return Err("Unknown timers view".into()),
    }
    let label = match command {
        "manageTimers" => "Manage Timers",
        "timersMenuBar" => "Timers Menu Bar Controls",
        "manageStopwatches" => "Manage Stopwatches",
        "stopwatchMenuBar" => "Stopwatch Menu Bar Controls",
        "previewAlertSounds" => "Preview Alert Sounds",
        _ => "Configure Menu Bar Presets",
    };
    Ok(list(label, children, actions))
}

async fn worker_wait(
    ctx: &CoreContext,
    wake: &mut tokio::sync::watch::Receiver<bool>,
    delay: Duration,
) -> bool {
    tokio::select! {
        _=ctx.cancelled()=>true,
        _=tokio::time::sleep(delay)=>false,
        changed=wake.changed()=>changed.is_err() || *wake.borrow(),
    }
}
/// Single app-lifetime scheduler for one Native V2 profile; views never own workers.
async fn worker_loop(
    ctx: CoreContext,
    rt: Arc<Runtime>,
    mut wake: tokio::sync::watch::Receiver<bool>,
) {
    let mut playing: Option<(String, SoundChild, bool)> = None;
    loop {
        if *wake.borrow() {
            break;
        }
        let gate = rt.gate.lock().await;
        let next = match tick(&ctx, &rt).await {
            Ok(delay) => delay,
            Err(error) => {
                log::warn!("Native timers scheduler: {error}");
                Duration::from_secs(15)
            }
        };
        // At most one sound process exists per profile, including continuous alerts.
        let data = match v2_sdk::read_store(&ctx, SLUG).await {
            Ok(data) => data,
            Err(error) => {
                log::warn!("Timer storage unavailable; retaining pending ownership: {error}");
                drop(gate);
                if worker_wait(&ctx, &mut wake, Duration::from_secs(15)).await {
                    break;
                }
                continue;
            }
        };
        let timers = data["timers"].as_array();
        let ringing = timers.and_then(|items| items.iter().find(|t| t["status"] == "ringing"));
        let pending_single = timers.and_then(|items| {
            items.iter().find(|t| {
                t["status"] == "finished"
                    && t["soundPlayed"] != true
                    && stamp().saturating_sub(t["lastSoundAttempt"].as_u64().unwrap_or(0)) >= 30_000
            })
        });
        let mut outcome = None;
        let mut just_finished: Option<String> = None;
        if let Some((id, child, once)) = playing.as_mut() {
            let exists = timers.is_some_and(|items| {
                items.iter().any(|t| {
                    t["id"] == *id && (t["status"] == "ringing" || t["status"] == "finished")
                })
            });
            if !exists {
                let _ = child.start_kill();
                playing = None;
            } else {
                outcome = match child.try_wait() {
                    Ok(Some(status)) => Some((*once, status)),
                    Err(error) => {
                        log::warn!("Timer sound process failed: {error}");
                        Some((*once, false))
                    }
                    Ok(None) => None,
                };
            }
        }
        if let Some((once, successful)) = outcome {
            if let Some((id, _, _)) = playing.take() {
                just_finished = Some(id.clone());
                if once {
                    mark_sound_attempt(&ctx, id, successful).await;
                } else if !successful {
                    mark_sound_attempt(&ctx, id, false).await;
                }
            }
        }
        let ringing_for_audio = ringing.filter(|t| {
            t["soundFailed"] != true
                || stamp().saturating_sub(t["lastSoundAttempt"].as_u64().unwrap_or(0)) >= 30_000
        });
        let pending_single =
            pending_single.filter(|t| just_finished.as_deref() != t["id"].as_str());
        let ringing_for_audio =
            ringing_for_audio.filter(|t| just_finished.as_deref() != t["id"].as_str());
        if playing.is_none() {
            if let Some(timer) = pending_single.or(ringing_for_audio) {
                let sound = timer["sound"].as_str().unwrap_or("alarmClock.wav");
                let name = timer["name"].as_str().unwrap_or("Untitled");
                let timer_id = timer["id"].as_str().unwrap_or("").to_owned();
                let once = timer["status"] == "finished";
                match spawn_sound(&ctx, sound, name, timer_volume(timer)) {
                    Ok(child) => playing = Some((timer_id, child, once)),
                    Err(e) => {
                        log::warn!("Timer alert sound unavailable: {e}");
                        mark_sound_attempt(&ctx, timer_id, false).await;
                    }
                }
            }
        }
        let latest = match v2_sdk::read_store(&ctx, SLUG).await {
            Ok(data) => data,
            Err(error) => {
                log::warn!("Timer storage unavailable; retaining pending ownership: {error}");
                drop(gate);
                if worker_wait(&ctx, &mut wake, Duration::from_secs(15)).await {
                    break;
                }
                continue;
            }
        };
        if !timer_work(&latest) && playing.is_none() {
            if !active_work(&latest) && rt.retained.load(Ordering::Acquire) {
                if release(&ctx).await.is_ok() {
                    rt.retained.store(false, Ordering::Release);
                } else {
                    drop(gate);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
            // Mark worker absent while holding the gate; a simultaneous new timer
            // cannot see an exiting handle as a live scheduler and lose its wakeup.
            rt.worker.lock().unwrap().take();
            break;
        }
        drop(gate);
        let delay = if playing.is_some() || ringing.is_some() || pending_single.is_some() {
            next.min(Duration::from_secs(2))
        } else {
            next
        };
        if worker_wait(&ctx, &mut wake, delay).await {
            break;
        }
    }
    if let Some((_, mut child, _)) = playing {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

async fn mark_sound_attempt(ctx: &CoreContext, id: String, successful: bool) {
    let attempt = stamp();
    if let Err(error) = v2_sdk::update_store(ctx, SLUG, move |store| {
        if let Some(t) = array_mut(store, "timers")?
            .iter_mut()
            .find(|t| t["id"] == id)
        {
            t["soundFailed"] = json!(!successful);
            t["lastSoundAttempt"] = json!(attempt);
            if successful && t["status"] == "finished" {
                t["soundPlayed"] = json!(true);
            }
        }
        Ok(())
    })
    .await
    {
        log::warn!("Could not record timer alert sound result: {error}");
    }
}

fn expire_items(items: &mut [Value], due: &[String]) {
    for item in items {
        if due.iter().any(|id| item["id"] == *id) && item["status"] == "running" {
            item["status"] = json!(if item["ringContinuously"] == true {
                "ringing"
            } else {
                "finished"
            });
            item["elapsedMs"] = item["durationMs"].clone();
            item["notificationSent"] = json!(false);
            item["lastNotificationAttempt"] = json!(0);
            item["soundPlayed"] = json!(false);
            item["lastSoundAttempt"] = json!(0);
        }
    }
}

fn timer_volume(timer: &Value) -> f32 {
    timer["volume"].as_f64().unwrap_or(1.0) as f32
}

async fn tick(ctx: &CoreContext, rt: &Runtime) -> Result<Duration, String> {
    remove_notifications(ctx, stamp()).await?;
    let notification_ctx = ctx.clone();
    tick_with_notify(ctx, rt, move |id, name| {
        let ctx = notification_ctx.clone();
        async move { notify(&ctx, &id, &name).await }
    })
    .await
}

async fn tick_with_notify<F, Fut>(
    ctx: &CoreContext,
    rt: &Runtime,
    notify_fn: F,
) -> Result<Duration, String>
where
    F: Fn(String, String) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    tick_at(ctx, rt, stamp(), notify_fn).await
}
async fn tick_at<F, Fut>(
    ctx: &CoreContext,
    rt: &Runtime,
    now: u64,
    notify_fn: F,
) -> Result<Duration, String>
where
    F: Fn(String, String) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let data = v2_sdk::read_store(ctx, SLUG).await?;
    let mut due = Vec::new();
    let mut wait = Duration::from_secs(60);
    for t in data["timers"].as_array().into_iter().flatten() {
        if t["status"] == "running" {
            let remaining_ms = remaining(t, Some(rt), now);
            if remaining_ms == 0 {
                if let Some(id) = t["id"].as_str() {
                    due.push(id.to_owned());
                }
            } else {
                wait = wait.min(Duration::from_millis(remaining_ms));
            }
        }
    }
    if !due.is_empty() {
        let ids = due.clone();
        v2_sdk::update_store(ctx, SLUG, move |store| {
            expire_items(array_mut(store, "timers")?, &ids);
            Ok(())
        })
        .await?;
        for id in &due {
            unanchor(rt, id);
        }
        let _ = ctx.emit("background.changed", json!({"key":SLUG,"completed":due}));
    }
    let data = if due.is_empty() {
        data
    } else {
        v2_sdk::read_store(ctx, SLUG).await?
    };
    for t in data["timers"].as_array().into_iter().flatten() {
        if t["status"] != "ringing" && t["status"] != "finished" {
            continue;
        }
        if t["soundPlayed"] != true {
            let last = t["lastSoundAttempt"].as_u64().unwrap_or(0);
            wait = wait.min(Duration::from_millis(
                30_000_u64.saturating_sub(now.saturating_sub(last)).max(100),
            ));
        }
        if t["notificationSent"] == true {
            continue;
        }
        let last = t["lastNotificationAttempt"].as_u64().unwrap_or(0);
        if now.saturating_sub(last) < 30_000 {
            wait = wait.min(Duration::from_millis(30_000 - now.saturating_sub(last)));
            continue;
        }
        let Some(id) = t["id"].as_str() else {
            continue;
        };
        // The platform API is a real OS notification; an error leaves this record
        // pending, not a manufactured success. Retry after 30 seconds.
        let result = notify_fn(
            id.to_owned(),
            t["name"].as_str().unwrap_or("Untitled").to_owned(),
        )
        .await;
        let sent = result.is_ok();
        if let Err(error) = result {
            log::warn!("Timer notification delivery failed: {error}");
            let _ = ctx.emit("background.error", json!({"id":id,"message":error}));
        }
        let target = id.to_owned();
        let _ = v2_sdk::update_store(ctx, SLUG, move |store| {
            if let Some(item) = array_mut(store, "timers")?
                .iter_mut()
                .find(|item| item["id"] == target)
            {
                item["notificationSent"] = json!(sent);
                item["lastNotificationAttempt"] = json!(now);
            }
            Ok(())
        })
        .await?;
        if !sent {
            wait = wait.min(Duration::from_secs(30));
        }
    }
    Ok(wait.max(Duration::from_millis(100)))
}

/// Native timers ship their own whitelisted sounds, independent of legacy installs.
fn sound_path(ctx: &CoreContext, sound: &str) -> Result<PathBuf, String> {
    if !SOUNDS.iter().any(|(name, _)| *name == sound) || sound == "speak_timer_name" {
        return Err("Unknown sound file".into());
    }
    let path = ctx.paths.package_dir.join("resources/tones").join(sound);
    if !path.is_file() {
        return Err(format!("Bundled timer sound is missing: {sound}"));
    }
    Ok(path)
}
// The scheduler owns every sound handle. Test builds substitute a deterministic
// silent backend; release builds always launch the real platform player.
enum SoundChild {
    #[cfg(not(test))]
    Native(tokio::process::Child),
    #[cfg(test)]
    Mock { polls: u8 },
}
#[cfg(test)]
static MOCK_AUDIO_ACTIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
impl Drop for SoundChild {
    fn drop(&mut self) {
        MOCK_AUDIO_ACTIVE.fetch_sub(1, Ordering::SeqCst);
    }
}
impl SoundChild {
    fn try_wait(&mut self) -> std::io::Result<Option<bool>> {
        match self {
            #[cfg(not(test))]
            Self::Native(child) => child.try_wait().map(|s| s.map(|s| s.success())),
            #[cfg(test)]
            Self::Mock { polls } => {
                *polls = polls.saturating_sub(1);
                Ok(if *polls == 0 { Some(true) } else { None })
            }
        }
    }
    fn start_kill(&mut self) -> std::io::Result<()> {
        match self {
            #[cfg(not(test))]
            Self::Native(child) => child.start_kill(),
            #[cfg(test)]
            Self::Mock { polls } => {
                *polls = 0;
                Ok(())
            }
        }
    }
    async fn wait(&mut self) -> std::io::Result<bool> {
        match self {
            #[cfg(not(test))]
            Self::Native(child) => child.wait().await.map(|s| s.success()),
            #[cfg(test)]
            Self::Mock { polls } => {
                *polls = 0;
                Ok(true)
            }
        }
    }
}
#[cfg(test)]
fn spawn_sound(
    _ctx: &CoreContext,
    _sound: &str,
    _name: &str,
    _volume: f32,
) -> Result<SoundChild, String> {
    MOCK_AUDIO_ACTIVE.fetch_add(1, Ordering::SeqCst);
    Ok(SoundChild::Mock { polls: 2 })
}
#[cfg(all(target_os = "macos", not(test)))]
fn spawn_sound(
    ctx: &CoreContext,
    sound: &str,
    name: &str,
    volume: f32,
) -> Result<SoundChild, String> {
    use std::process::Stdio;
    let mut cmd = if sound == "speak_timer_name" {
        let mut c = tokio::process::Command::new("/usr/bin/say");
        c.arg(name);
        c
    } else {
        let mut c = tokio::process::Command::new("/usr/bin/afplay");
        c.arg(sound_path(ctx, sound)?)
            .arg("--volume")
            .arg(volume.to_string());
        c
    };
    cmd.kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn()
        .map(SoundChild::Native)
        .map_err(|e| format!("Cannot play timer sound: {e}"))
}
#[cfg(all(target_os = "windows", not(test)))]
fn spawn_sound(
    ctx: &CoreContext,
    sound: &str,
    name: &str,
    _volume: f32,
) -> Result<SoundChild, String> {
    use std::process::Stdio;
    let script = if sound == "speak_timer_name" {
        format!("Add-Type -AssemblyName System.Speech; $s=New-Object System.Speech.Synthesis.SpeechSynthesizer; $s.Speak('{}')", ps_string(name))
    } else {
        let path = sound_path(ctx, sound)?;
        format!(
            "$p=New-Object System.Media.SoundPlayer('{}'); $p.PlaySync()",
            ps_string(&path.to_string_lossy())
        )
    };
    tokio::process::Command::new(
        PathBuf::from(std::env::var_os("SystemRoot").unwrap_or_else(|| r"C:\Windows".into()))
            .join("System32/WindowsPowerShell/v1.0/powershell.exe"),
    )
    .args(["-NoProfile", "-NonInteractive", "-Command", &script])
    .kill_on_drop(true)
    .creation_flags(0x08000000)
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .spawn()
    .map(SoundChild::Native)
    .map_err(|e| format!("Cannot play timer sound: {e}"))
}
#[cfg(all(not(any(target_os = "macos", target_os = "windows")), not(test)))]
fn spawn_sound(
    _ctx: &CoreContext,
    _sound: &str,
    _name: &str,
    _volume: f32,
) -> Result<SoundChild, String> {
    Err("Timer audio is unsupported on this platform".into())
}
#[cfg(all(target_os = "windows", not(test)))]
fn ps_string(s: &str) -> String {
    s.replace('\'', "''")
}

async fn play_once(ctx: &CoreContext, sound: &str, name: &str, volume: f32) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    if volume != 1.0 {
        return Err("Windows sound playback cannot set the requested volume".into());
    }
    let mut child = spawn_sound(ctx, sound, name, volume)?;
    match tokio::time::timeout(Duration::from_secs(20), child.wait()).await {
        Ok(Ok(true)) => Ok(()),
        Ok(Ok(false)) => Err("Sound player exited unsuccessfully".into()),
        Ok(Err(e)) => Err(format!("Sound player failed: {e}")),
        Err(_) => {
            let _ = child.start_kill();
            Err("Sound preview timed out".into())
        }
    }
}
async fn notify(ctx: &CoreContext, id: &str, name: &str) -> Result<(), String> {
    let value = tokio::time::timeout(
        Duration::from_secs(5),
        ctx.host(
            "host.notifications.show",
            json!({"id":id,"title":"Ding!","body":format!("Timer \"{name}\" complete")}),
        ),
    )
    .await
    .map_err(|_| "Host notification timed out")??;
    if value == false || value.get("ok") == Some(&json!(false)) {
        return Err("Host notification was not acknowledged".into());
    }
    Ok(())
}
async fn remove_notifications(ctx: &CoreContext, now: u64) -> Result<(), String> {
    let store = v2_sdk::read_store(ctx, SLUG).await?;
    for entry in store["pendingNotificationRemovals"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let id = entry["id"]
            .as_str()
            .ok_or("Invalid pending notification id")?;
        if now.saturating_sub(entry["lastAttempt"].as_u64().unwrap_or(0)) < 30_000 {
            continue;
        }
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            ctx.host("host.notifications.remove", json!({"id":id})),
        )
        .await
        .map_err(|_| "Host notification removal timed out".to_string())
        .and_then(|result| result);
        let success = outcome
            .as_ref()
            .is_ok_and(|v| v != &json!(false) && v.get("ok") != Some(&json!(false)));
        let target = id.to_owned();
        v2_sdk::update_store(ctx, SLUG, move |store| {
            let entries = array_mut(store, "pendingNotificationRemovals")?;
            if success {
                entries.retain(|e| e["id"] != target);
            } else {
                for e in entries {
                    if e["id"] == target {
                        e["lastAttempt"] = json!(now);
                    }
                }
            }
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use v2_sdk::{ExtensionPaths, TestContext};
    struct Fixture {
        ctx: CoreContext,
        test: TestContext,
        dir: PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("tezbar-timers-test-{}", new_id()));
            let test = TestContext::new(
                ExtensionPaths {
                    data_dir: dir.join("data"),
                    temp_dir: dir.join("tmp"),
                    package_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
                },
                &["background", "notifications", "clipboard", "window"],
            );
            Self {
                ctx: test.context.clone(),
                test,
                dir,
            }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    fn empty_runtime() -> Runtime {
        let f = Fixture::new();
        let (wake, _) = tokio::sync::watch::channel(false);
        Runtime {
            ctx: f.ctx.clone(),
            wake,
            anchors: Mutex::default(),
            worker: Mutex::default(),
            gate: tokio::sync::Mutex::new(()),
            retained: AtomicBool::new(false),
        }
    }

    #[tokio::test]
    async fn clean_native_install_can_start_default_timer_with_bundled_sounds() {
        let f = Fixture::new();
        assert!(!f.ctx.paths.data_dir.join("extensions/timers").exists());
        f.test
            .run(create_timer(
                &f.ctx,
                5,
                "Native timer".into(),
                "default",
                &json!({}),
                false,
            ))
            .await
            .expect("A native timer must not require a legacy extension installation");
        for (name, _) in SOUNDS.iter().filter(|(name, _)| name.ends_with(".wav")) {
            let path = sound_path(&f.ctx, name).unwrap();
            assert!(path.starts_with(&f.ctx.paths.package_dir));
            let bytes = std::fs::read(path).unwrap();
            assert_eq!(&bytes[..4], b"RIFF");
            assert_eq!(&bytes[8..12], b"WAVE");
        }
        assert!(sound_path(&f.ctx, "../alarmClock.wav").is_err());
    }

    #[test]
    fn custom_duration_rejects_negative_fractional_zero_and_overflow() {
        assert_eq!(
            checked_duration(&json!("1"), &json!("30"), &json!("7")).unwrap(),
            5407
        );
        for (h, m, s) in [
            (json!(""), json!(""), json!("")),
            (json!("-1"), json!("0"), json!("0")),
            (json!("0.5"), json!("0"), json!("0")),
            (json!("999999999999999999999999999"), json!("0"), json!("0")),
            (json!("8761"), json!("0"), json!("0")),
        ] {
            assert!(checked_duration(&h, &m, &s).is_err());
        }
        assert!(sound(&json!("../user.wav")).is_err());
        assert!(validate_volume(&json!({"volumeSetting":"6"})).is_err());
    }

    #[test]
    fn active_elapsed_uses_monotonic_clock_paused_and_restarted_uses_wall_time() {
        let rt = empty_runtime();
        rt.anchors.lock().unwrap().insert(
            "one".into(),
            Anchor {
                at: Instant::now() - Duration::from_secs(4),
                elapsed_ms: 3_000,
            },
        );
        let active = elapsed(Some(&rt), "one", 3_000, 9_000, 10_000);
        assert!((7_000..8_000).contains(&active));
        assert_eq!(elapsed(None, "one", 3_000, 9_000, 10_000), 4_000);
        assert_eq!(
            remaining(
                &json!({"id":"one","durationMs":10000,"elapsedMs":3200,"status":"paused"}),
                Some(&rt),
                10_000
            ),
            6800
        );
    }

    #[tokio::test]
    async fn records_persist_and_controls_pause_resume_stop_and_presets() {
        let f = Fixture::new();
        f.test
            .run(create_timer(
                &f.ctx,
                120,
                " Focus ".into(),
                "default",
                &json!({}),
                true,
            ))
            .await
            .unwrap();
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        let id = store["timers"][0]["id"].as_str().unwrap();
        assert_eq!(store["presets"].as_array().unwrap().len(), 1);
        assert_eq!(store["timers"][0]["name"], " Focus ");
        f.test
            .run(mutate(&f.ctx, "timers", id, "pause", None))
            .await
            .unwrap();
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_eq!(store["timers"][0]["status"], "paused");
        f.test
            .run(mutate(&f.ctx, "timers", id, "resume", None))
            .await
            .unwrap();
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_eq!(store["timers"][0]["status"], "running");
        f.test
            .run(mutate(&f.ctx, "timers", id, "stop", None))
            .await
            .unwrap();
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert!(store["timers"].as_array().unwrap().is_empty());
        assert_eq!(store["presets"].as_array().unwrap().len(), 1);
        assert!(f.ctx.paths.data_dir.join("timers/storage.json").exists());
    }

    #[tokio::test]
    async fn stopwatch_pause_resume_rename_restart_copy_and_delete_persist() {
        let f = Fixture::new();
        f.test
            .run(create_stopwatch(&f.ctx, "Draft".into()))
            .await
            .unwrap();
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        let id = store["stopwatches"][0]["id"].as_str().unwrap().to_owned();
        f.test
            .run(mutate(&f.ctx, "stopwatches", &id, "pause", None))
            .await
            .unwrap();
        let paused = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_eq!(paused["stopwatches"][0]["status"], "paused");
        f.test
            .run(mutate(&f.ctx, "stopwatches", &id, "resume", None))
            .await
            .unwrap();
        f.test
            .run(mutate(
                &f.ctx,
                "stopwatches",
                &id,
                "rename",
                Some(&json!("Tea Break")),
            ))
            .await
            .unwrap();
        let copied = f
            .test
            .run(mutate(&f.ctx, "stopwatches", &id, "copy", None))
            .await
            .unwrap()
            .unwrap();
        assert!(
            copied == "00:00" || copied == "00:01",
            "Expected initial stopwatch copy to be near zero, got {copied}"
        );
        f.test
            .run(mutate(&f.ctx, "stopwatches", &id, "restart", None))
            .await
            .unwrap();
        let restarted = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_ne!(restarted["stopwatches"][0]["id"], id);
        assert_eq!(restarted["stopwatches"][0]["name"], "Tea Break");
        let new_id = restarted["stopwatches"][0]["id"].as_str().unwrap();
        f.test
            .run(mutate(&f.ctx, "stopwatches", new_id, "stop", None))
            .await
            .unwrap();
        assert!(
            v2_sdk::read_store(&f.ctx, SLUG).await.unwrap()["stopwatches"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn fake_notification_recorder_delivers_due_timer_once_without_real_os_side_effects() {
        let f = Fixture::new();
        let rt = empty_runtime();
        let past = stamp() - 5_000;
        v2_sdk::update_store(&f.ctx, SLUG, move |store| {
            array_mut(store,"timers")?.push(json!({"id":"due","name":"Tea","durationMs":1000,"elapsedMs":0,"resumedAt":past,"status":"running","ringContinuously":false}));
            Ok(())
        }).await.unwrap();
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        for _ in 0..2 {
            let calls = calls.clone();
            tick_with_notify(&f.ctx, &rt, move |_id, name| {
                let calls = calls.clone();
                async move {
                    calls.lock().unwrap().push(name);
                    Ok(())
                }
            })
            .await
            .unwrap();
        }
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_eq!(store["timers"][0]["status"], "finished");
        assert_eq!(store["timers"][0]["notificationSent"], true);
        assert_eq!(*calls.lock().unwrap(), vec!["Tea"]);
    }

    #[tokio::test]
    async fn row_dismiss_targets_only_its_ringing_timer() {
        let f = Fixture::new();
        v2_sdk::update_store(&f.ctx, SLUG, |store| {
            let timers = array_mut(store,"timers")?;
            for id in ["first","second"] {
                timers.push(json!({"id":id,"name":id,"durationMs":1000,"elapsedMs":1000,"status":"ringing"}));
            }
            Ok(())
        }).await.unwrap();
        let view = f
            .test
            .run(render(&f.ctx, "manageTimers", &json!({}), &json!({})))
            .await
            .unwrap();
        let id = "timers:dismiss:second";
        assert!(view["actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["id"] == id));
        f.test
            .run(mutate(
                &f.ctx,
                "timers",
                &input_id(id, "timers:dismiss:").unwrap(),
                "dismiss",
                None,
            ))
            .await
            .unwrap();
        let stored = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_eq!(stored["timers"].as_array().unwrap().len(), 1);
        assert_eq!(stored["timers"][0]["id"], "first");
    }

    #[tokio::test]
    async fn fake_failed_notification_remains_pending_and_ringing_can_be_dismissed() {
        let f = Fixture::new();
        let rt = empty_runtime();
        let past = stamp() - 5_000;
        v2_sdk::update_store(&f.ctx, SLUG, move |store| {
            array_mut(store,"timers")?.push(json!({"id":"ring","name":"Tea","durationMs":1000,"elapsedMs":0,"resumedAt":past,"status":"running","ringContinuously":true}));
            Ok(())
        }).await.unwrap();
        tick_with_notify(&f.ctx, &rt, |_, _| async {
            Err("fake notification denied".into())
        })
        .await
        .unwrap();
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_eq!(store["timers"][0]["status"], "ringing");
        assert_eq!(store["timers"][0]["notificationSent"], false);
        f.test
            .run(mutate(&f.ctx, "timers", "ring", "dismiss", None))
            .await
            .unwrap();
        assert!(v2_sdk::read_store(&f.ctx, SLUG).await.unwrap()["timers"]
            .as_array()
            .unwrap()
            .is_empty());
    }
    #[tokio::test]
    async fn durable_lease_denial_precedes_active_state_creation() {
        let f = Fixture::new();
        f.test.reply(Err("durable registration denied".into()));
        assert!(f
            .test
            .run(create_timer(
                &f.ctx,
                120,
                "Denied".into(),
                "default",
                &json!({}),
                false
            ))
            .await
            .is_err());
        assert!(v2_sdk::read_store(&f.ctx, SLUG)
            .await
            .unwrap()
            .get("timers")
            .is_none());
    }

    #[tokio::test]
    async fn idle_view_has_no_worker_and_active_alarm_survives_dispose_until_shutdown() {
        let f = Fixture::new();
        initialize(&f.ctx).unwrap();
        f.test
            .run(dispatch(
                &f.ctx,
                "manageTimers",
                "extension:run-command",
                &json!({}),
                &mut json!({}),
            ))
            .await
            .unwrap();
        let rt = runtime(&f.ctx).unwrap();
        assert!(rt.worker.lock().unwrap().is_none());
        assert!(!rt.retained.load(Ordering::Acquire));
        f.test
            .run(dispatch(
                &f.ctx,
                "start2MinuteTimer",
                "extension:run-command",
                &json!({}),
                &mut json!({}),
            ))
            .await
            .unwrap();
        assert!(rt.worker.lock().unwrap().is_some());
        assert!(rt.retained.load(Ordering::Acquire));
        f.test
            .run(dispatch(
                &f.ctx,
                "start2MinuteTimer",
                "extension:dispose-session",
                &json!({}),
                &mut json!({}),
            ))
            .await
            .unwrap();
        assert!(rt.worker.lock().unwrap().is_some());
        f.test.run(shutdown(&f.ctx)).await;
        assert!(rt.worker.lock().unwrap().is_none());
        assert_eq!(MOCK_AUDIO_ACTIVE.load(Ordering::SeqCst), 0);
        assert!(
            !f.test
                .calls()
                .iter()
                .any(|m| m["method"] == "host.background.release"),
            "durable active state must resume after host restart"
        );
    }

    #[tokio::test]
    async fn worker_uses_silent_owned_audio_and_exits_after_acknowledged_alarm() {
        let f = Fixture::new();
        initialize(&f.ctx).unwrap();
        v2_sdk::update_store(&f.ctx,SLUG,|s|{s["timers"]=json!([{"id":"due","name":"Tea","status":"finished","sound":"alarmClock.wav","soundPlayed":false,"notificationSent":false}]);Ok(())}).await.unwrap();
        f.test.run(activate(&f.ctx)).await.unwrap();
        let rt = runtime(&f.ctx).unwrap();
        f.test
            .run(async {
                tokio::time::timeout(Duration::from_secs(6), async {
                    loop {
                        let data = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
                        if data["timers"][0]["soundPlayed"] == true
                            && rt.worker.lock().unwrap().is_none()
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
            })
            .await;
        assert_eq!(MOCK_AUDIO_ACTIVE.load(Ordering::SeqCst), 0);
        let calls = f.test.calls();
        assert_eq!(calls[0]["method"], "host.background.retain");
        assert!(calls
            .iter()
            .any(|m| m["method"] == "host.notifications.show"));
        assert_eq!(calls.last().unwrap()["method"], "host.background.release");
        f.test.run(shutdown(&f.ctx)).await;
    }

    #[tokio::test]
    async fn notification_and_sound_retry_bookkeeping_uses_injected_wall_clock() {
        let f = Fixture::new();
        let rt = empty_runtime();
        v2_sdk::update_store(&f.ctx,SLUG,|s|{s["timers"]=json!([{"id":"retry","status":"finished","soundPlayed":true,"notificationSent":false,"lastNotificationAttempt":0}]);Ok(())}).await.unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recorder = |fail: bool| {
            let calls = calls.clone();
            move |_: String, _: String| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if fail {
                        Err("denied".into())
                    } else {
                        Ok(())
                    }
                }
            }
        };
        tick_at(&f.ctx, &rt, 100_000, recorder(true)).await.unwrap();
        tick_at(&f.ctx, &rt, 100_001, recorder(false))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tick_at(&f.ctx, &rt, 130_000, recorder(false))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        mark_sound_attempt(&f.ctx, "retry".into(), false).await;
        assert_eq!(
            v2_sdk::read_store(&f.ctx, SLUG).await.unwrap()["timers"][0]["soundFailed"],
            true
        );
        mark_sound_attempt(&f.ctx, "retry".into(), true).await;
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_eq!(store["timers"][0]["soundFailed"], false);
        assert_eq!(store["timers"][0]["soundPlayed"], true);
    }

    #[tokio::test]
    async fn concurrent_command_sessions_preserve_every_timer_and_preset() {
        let f = Fixture::new();
        initialize(&f.ctx).unwrap();
        let mut one = json!({});
        let mut two = json!({});
        let payload = json!({});
        let (a, b) = f
            .test
            .run(async {
                tokio::join!(
                    dispatch(
                        &f.ctx,
                        "start2MinuteTimer",
                        "extension:run-command",
                        &payload,
                        &mut one
                    ),
                    dispatch(
                        &f.ctx,
                        "start5MinuteTimer",
                        "extension:run-command",
                        &payload,
                        &mut two
                    )
                )
            })
            .await;
        a.unwrap();
        b.unwrap();
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_eq!(store["timers"].as_array().unwrap().len(), 2);
        assert_ne!(store["timers"][0]["id"], store["timers"][1]["id"]);
        assert_eq!(
            f.test
                .calls()
                .iter()
                .filter(|m| m["method"] == "host.background.retain")
                .count(),
            1
        );
        f.test.run(shutdown(&f.ctx)).await;
    }

    #[tokio::test]
    async fn cancelled_response_does_not_abandon_a_committed_timer_transaction() {
        let f = Fixture::new();
        initialize(&f.ctx).unwrap();
        f.test.run(retain(&f.ctx)).await.unwrap();
        f.test.run(async {
            let transaction=update_owned(&f.ctx,|store| {
                std::thread::sleep(Duration::from_millis(30));
                store["timers"]=json!([{"id":"committed","status":"running","durationMs":120000,"resumedAt":stamp(),"sound":"alarmClock.wav"}]);Ok(())
            },|_|{});
            assert!(tokio::time::timeout(Duration::from_millis(5),transaction).await.is_err());
            tokio::time::timeout(Duration::from_secs(2),async {
                loop {if runtime(&f.ctx).unwrap().worker.lock().unwrap().is_some(){break;}tokio::time::sleep(Duration::from_millis(5)).await;}
            }).await.unwrap();
        }).await;
        assert_eq!(
            v2_sdk::read_store(&f.ctx, SLUG).await.unwrap()["timers"][0]["id"],
            "committed"
        );
        f.test.run(shutdown(&f.ctx)).await;
    }

    #[tokio::test]
    async fn shutdown_kills_owned_silent_audio_while_view_dispose_keeps_ringing() {
        let f = Fixture::new();
        initialize(&f.ctx).unwrap();
        v2_sdk::update_store(&f.ctx,SLUG,|s|{s["timers"]=json!([{"id":"ring","status":"ringing","sound":"alarmClock.wav","notificationSent":true}]);Ok(())}).await.unwrap();
        f.test.run(activate(&f.ctx)).await.unwrap();
        f.test
            .run(async {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while MOCK_AUDIO_ACTIVE.load(Ordering::SeqCst) == 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
            })
            .await;
        f.test
            .run(dispatch(
                &f.ctx,
                "manageTimers",
                "extension:dispose-session",
                &json!({}),
                &mut json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(MOCK_AUDIO_ACTIVE.load(Ordering::SeqCst), 1);
        f.test.run(shutdown(&f.ctx)).await;
        assert_eq!(MOCK_AUDIO_ACTIVE.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn fresh_process_requeues_ringing_notifications_for_the_new_host_generation() {
        let f = Fixture::new();
        v2_sdk::update_store(&f.ctx,SLUG,|s|{s["future"]=json!(42);s["timers"]=json!([
            {"id":"ring","status":"ringing","notificationSent":true,"lastNotificationAttempt":123},
            {"id":"done","status":"finished","notificationSent":true,"soundPlayed":true}
        ]);Ok(())}).await.unwrap();
        prepare_restart(&f.ctx).await.unwrap();
        let data = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert_eq!(data["timers"][0]["notificationSent"], false);
        assert_eq!(data["timers"][0]["lastNotificationAttempt"], 0);
        assert_eq!(data["timers"][1]["notificationSent"], true);
        assert_eq!(data["future"], 42);
    }
    #[tokio::test]
    async fn idle_transaction_retries_failed_and_negative_lease_release() {
        let f = Fixture::new();
        initialize(&f.ctx).unwrap();
        f.test.run(retain(&f.ctx)).await.unwrap();
        f.test.reply(Ok(json!(false)));
        f.test.reply(Err("transient release failure".into()));
        f.test
            .run(update_owned(
                &f.ctx,
                |store| {
                    store["stopwatches"] = json!([{"id":"paused","status":"paused"}]);
                    Ok(())
                },
                |_| {},
            ))
            .await
            .unwrap();
        let rt = runtime(&f.ctx).unwrap();
        f.test
            .run(async {
                tokio::time::timeout(Duration::from_secs(4), async {
                    while rt.retained.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap();
            })
            .await;
        assert!(rt.worker.lock().unwrap().is_none());
        assert_eq!(
            f.test
                .calls()
                .iter()
                .filter(|m| m["method"] == "host.background.release")
                .count(),
            3
        );
        f.test.run(shutdown(&f.ctx)).await;
    }

    #[tokio::test]
    async fn stop_removes_notification_even_if_show_acknowledgement_was_lost() {
        let f = Fixture::new();
        v2_sdk::update_store(&f.ctx, SLUG, |store| {
            store["timers"] = json!([{"id":"uncertain","status":"finished","notificationSent":false,"soundPlayed":true}]);
            Ok(())
        }).await.unwrap();
        f.test
            .run(mutate(&f.ctx, "timers", "uncertain", "stop", None))
            .await
            .unwrap();
        assert!(f.test.calls().iter().any(
            |m| m["method"] == "host.notifications.remove" && m["params"]["id"] == "uncertain"
        ));
        let store = v2_sdk::read_store(&f.ctx, SLUG).await.unwrap();
        assert!(store["timers"].as_array().unwrap().is_empty());
        assert!(store["pendingNotificationRemovals"]
            .as_array()
            .unwrap()
            .is_empty());
    }
}
