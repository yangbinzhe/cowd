use std::time::Instant;

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "cached".to_string());
    let cached = match mode.as_str() {
        "cached" => true,
        "uncached" => false,
        _ => {
            eprintln!("usage: route-openapi-bench <cached|uncached>");
            std::process::exit(2);
        }
    };
    let iterations = 50;
    let started = Instant::now();
    let checksum = gateway::route_openapi_benchmark(iterations, cached);
    println!(
        "{{\"mode\":\"{mode}\",\"iterations\":{iterations},\"elapsed_ns\":{},\"checksum\":{checksum}}}",
        started.elapsed().as_nanos()
    );
}
