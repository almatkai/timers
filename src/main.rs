#[tokio::main(worker_threads = 2)]
async fn main() {
    if let Err(error) = tezbar_extension_sdk::serve(tezbar_timers::Adapter).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
