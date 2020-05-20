//! The base UDP layer of the Discv5 service.
//!
//! The [`Transport`] opens a UDP socket and handles the encoding/decoding of raw Discv5
//! messages. These messages are defined in the [`Packet`] module.
//!
//! [`Transport`]: transport/struct.Transport.html
//! [`Packet`]: ../packet/index.html

use super::packet::{Packet, MAGIC_LENGTH};
use log::warn;
use std::{io, net::SocketAddr};
use tokio::net::UdpSocket;

pub(crate) const MAX_PACKET_SIZE: usize = 1280;

/// The main service that handles the transport. Specifically the UDP sockets and packet
/// encoding/decoding.
pub(crate) struct Transport {
    /// The UDP socket for interacting over UDP.
    socket: UdpSocket,
    /// The buffer to accept inbound datagrams.
    recv_buffer: [u8; MAX_PACKET_SIZE],
    /// WhoAreYou Magic Value. Used to decode raw WHOAREYOU packets.
    whoareyou_magic: [u8; MAGIC_LENGTH],
}

impl Transport {
    /// Initializes the UDP socket, can fail when binding the socket.
    pub(crate) fn new(
        socket_addr: SocketAddr,
        whoareyou_magic: [u8; MAGIC_LENGTH],
    ) -> io::Result<Self> {
        // set up the UDP socket
        let socket = {
            #[cfg(unix)]
            fn platform_specific(s: &net2::UdpBuilder) -> io::Result<()> {
                net2::unix::UnixUdpBuilderExt::reuse_port(s, true)?;
                Ok(())
            }
            #[cfg(not(unix))]
            fn platform_specific(_: &net2::UdpBuilder) -> io::Result<()> {
                Ok(())
            }
            let builder = net2::UdpBuilder::new_v4()?;
            builder.reuse_address(true)?;
            platform_specific(&builder)?;
            builder.bind(socket_addr)?
        };
        let socket = UdpSocket::from_std(socket)?;

        Ok(Transport {
            socket,
            recv_buffer: [0; MAX_PACKET_SIZE],
            whoareyou_magic,
        })
    }

    /// Add packets to the send queue.
    pub(crate) async fn send(&mut self, dst: SocketAddr, packet: Packet) {
        match self.socket.send_to(&packet.encode(), &dst).await {
            Err(e) => warn!("Discv5 packet not sent: {}", e),
            Ok(x) if x == 0 => warn!("No bytes written to udp socket"),
            Ok(_) => {} // packet sent
        }
    }

    /// Receives and decodes packets from the UDP socket.
    pub async fn recv(&mut self) -> Result<(SocketAddr, Packet), String> {
        match self.socket.recv_from(&mut self.recv_buffer).await {
            Ok((length, src)) => {
                match Packet::decode(&self.recv_buffer[..length], &self.whoareyou_magic) {
                    Ok(p) => Ok((src, p)),
                    Err(e) => Err(format!("Could not decode discv5 packet: {:?}", e)),
                }
            }
            Err(e) => Err(format!("Could not read discv5 packet: {}", e)),
        }
    }
}
