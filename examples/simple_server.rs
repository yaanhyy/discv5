//! Demonstrates how to run a basic Discovery v5 Service.
//!
//! This example simply starts a discovery server and listens to events that the server emits.
//!
//!
//! It can be bootstrapped to a DHT by providing an ENR to add to its DHT.
//!
//! To run this example simply run:
//! ```
//! $ cargo run --example simple_server <BASE64ENR>
//! ```

use discv5::{enr, enr::CombinedKey, Discv5, Discv5Config, Discv5Event};
use std::net::SocketAddr;
use std::io::Write;
use chrono::Local;
use std::net::Ipv4Addr;
#[tokio::main]
async fn main() {
    // allows detailed logging with the RUST_LOG env variable
    //env_logger::init();
    let env = env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV,"trace");
    env_logger::Builder::from_env(env).format(|buf,record|{
        writeln!(
            buf,
            "{} {} [{}:{}] {} {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level(),
            record.module_path().unwrap_or("<unnamed>"),
            record.line().unwrap_or(0),
            record.target(),
            &record.args()
        )
    }).init();

    // listening address and port
    let listen_addr = "0.0.0.0:9000".parse::<SocketAddr>().unwrap();

    let enr_key = CombinedKey::generate_secp256k1();
    // construct a local ENR
    let ip = Ipv4Addr::new(0,0,0,0);
    let enr = enr::EnrBuilder::new("v4").ip(ip.into()).udp(9000).build(&enr_key).unwrap();

    println!("enr:{:?}", enr.to_base64() );
    // default configuration
    let config = Discv5Config::default();

    // construct the discv5 server
    let mut discv5 = Discv5::new(enr, enr_key, config).unwrap();

    // if we know of another peer's ENR, add it known peers
    if let Some(base64_enr) = std::env::args().nth(1) {
        match base64_enr.parse::<enr::Enr<enr::CombinedKey>>() {
            Ok(enr) => {
                println!(
                    "ENR Read. ip: {:?}, udp_port {:?}, tcp_port: {:?}",
                    enr.ip(),
                    enr.udp(),
                    enr.tcp()
                );
                if let Err(e) = discv5.add_enr(enr) {
                    println!("ENR was not added: {}", e);
                }
            }
            Err(e) => panic!("Decoding ENR failed: {}", e),
        }
    }

    // start the discv5 service
    discv5.start(listen_addr);
    println!("Server started");

    // get an event stream
    let mut event_stream = discv5.event_stream().await.unwrap();

    loop {
        match event_stream.recv().await {
            Some(Discv5Event::SocketUpdated(addr)) => {
                println!("Nodes ENR socket address has been updated to: {:?}", addr);
            }
            Some(Discv5Event::Discovered(enr)) => {
                println!("A peer has been discovered: {}", enr.node_id());
            }
            _ => {}
        }
    }
}
