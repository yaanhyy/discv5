//! The Discovery v5 protocol. See `lib.rs` for further details.
//!

use self::ip_vote::IpVote;
use self::query_info::{QueryInfo, QueryType};
use crate::error::Discv5Error;
use crate::handler::{Handler, HandlerRequest, HandlerResponse};
use crate::kbucket::{self, EntryRefView, KBucketsTable, NodeStatus};
use crate::node_info::{NodeAddress, NodeContact};
use crate::query_pool::{
    FindNodeQueryConfig, PredicateQueryConfig, QueryId, QueryPool, QueryPoolState, ReturnPeer,
};
use crate::rpc;
use crate::socket::MAX_PACKET_SIZE;
use crate::Discv5Config;
use crate::Enr;
use enr::{CombinedKey, EnrError, EnrKey, NodeId};
use fnv::FnvHashMap;
use futures::prelude::*;
use log::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use rpc::*;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Interval;

pub struct Discv5 {
    service: Service,
}

impl Drop for Discv5 {
    fn drop(&mut self) {
        self.shutdown()
    }
}

impl Discv5 {
    /// Builds the `Discv5` main struct.
    pub fn new(
        local_enr: Enr,
        enr_key: CombinedKey,
        config: Discv5Config,
    ) -> Result<Self, Discv5Error> {
        Discv5 {
            service: Service::new(local_enr, enr_key, config),
        }
    }

    pub fn start(&mut self, listen_socket: SocketAddr) {
        self.service.start();
    }

    pub fn shutdown(&mut self) {
        self.service.shutdown();
    }

    /// Adds a known ENR of a peer participating in Discv5 to the
    /// routing table.
    ///
    /// This allows pre-populating the Kademlia routing table with known
    /// addresses, so that they can be used immediately in following DHT
    /// operations involving one of these peers, without having to dial
    /// them upfront.
    pub async fn add_enr(&mut self, enr: Enr) -> Result<(), &'static str> {
        self.service.add_enr(enr).await
    }

    /// Removes a `node_id` from the routing table.
    ///
    /// This allows applications, for whatever reason, to remove nodes from the local routing
    /// table. Returns `true` if the node was in the table and `false` otherwise.
    pub fn remove_node(&mut self, node_id: &NodeId) -> bool {
        self.service.remove_node(node_id);
    }

    /// Returns the number of connected peers the service knows about.
    pub fn connected_peers(&self) -> usize {
        self.service.connected_peers(&self)
    }

    pub fn active_sessions(&self) -> usize {
        self.service.active_sessions()
    }

    /// Returns the local ENR of the node.
    pub fn local_enr(&self) -> Enr {
        self.local_enr.read().clone()
    }

    /// Allows the application layer to update the local ENR's UDP socket. The second parameter
    /// determines whether the port is a TCP port. If this parameter is false, this is
    /// interpreted as a UDP `SocketAddr`.
    pub fn update_local_enr_socket(&mut self, socket_addr: SocketAddr, is_tcp: bool) -> bool {
        if is_tcp {
            if self.local_enr().tcp_socket() == Some(socket_addr) {
                // nothing to do, not updated
                return false;
            }
            match self
                .local_enr
                .write()
                .set_tcp_socket(socket_addr, &self.enr_key.read())
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Could not update the ENR IP address. Error: {:?}", e);
                    return false;
                }
            }
        } else {
            if self.local_enr().udp_socket() == Some(socket_addr) {
                // nothing to do, not updated
                return false;
            }
            match self
                .local_enr
                .write()
                .set_udp_socket(socket_addr, &self.enr_key.read())
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Could not update the ENR IP address. Error: {:?}", e);
                    return false;
                }
            }
        }
        // notify peers of the update
        self.ping_connected_peers();
        true
    }

    /// Allows application layer to insert an arbitrary field into the local ENR.
    pub fn enr_insert(&mut self, key: &str, value: Vec<u8>) -> Result<Option<Vec<u8>>, EnrError> {
        let result = self
            .local_enr
            .write()
            .insert(key, value, &self.enr_key.read());
        if result.is_ok() {
            self.ping_connected_peers();
        }
        result
    }

    /// Returns an iterator over all ENR node IDs of nodes currently contained in a bucket
    /// of the Kademlia routing table.
    pub fn kbuckets_entries(&mut self) -> impl Iterator<Item = &NodeId> {
        self.kbuckets.iter().map(|entry| entry.node.key.preimage())
    }

    /// Returns an iterator over all the ENR's of nodes currently contained in a bucket of
    /// the Kademlia routing table.
    pub fn enr_entries(&mut self) -> impl Iterator<Item = &Enr> {
        self.kbuckets.iter().map(|entry| entry.node.value)
    }

    /// Starts an iterative `FIND_NODE` request.
    ///
    /// This will eventually produce an event containing the nodes of the DHT closest to the
    /// requested `PeerId`.
    pub fn find_node(&mut self, target_node: NodeId) -> QueryId {
        self.start_findnode_query(target_node)
    }

    /// Starts a `FIND_NODE` request.
    ///
    /// This will eventually produce an event containing <= `num` nodes which satisfy the
    /// `predicate` with passed `value`.
    pub fn find_node_predicate<F>(
        &mut self,
        node_id: NodeId,
        predicate: F,
        num_nodes: usize,
    ) -> QueryId
    where
        F: Fn(&Enr) -> bool + Send + Clone + 'static,
    {
        self.start_predicate_query(node_id, predicate, num_nodes)
    }

    /// Returns an ENR if one is known for the given NodeId.
    pub fn find_enr(&mut self, node_id: &NodeId) -> Option<Enr> {
        // check if we know this node id in our routing table
        let key = kbucket::Key::from(*node_id);
        if let kbucket::Entry::Present(mut entry, _) = self.kbuckets.entry(&key) {
            return Some(entry.value().clone());
        }
        // check the untrusted addresses for ongoing queries
        for query in self.queries.iter() {
            if let Some(enr) = query
                .target()
                .untrusted_enrs
                .iter()
                .find(|v| v.node_id() == *node_id)
            {
                return Some(enr.clone());
            }
        }
        None
    }

    // private functions //

    /// Processes an RPC request from a peer. Requests respond to the received socket address,
    /// rather than the IP of the known ENR.
    async fn handle_rpc_request(&mut self, node_address: NodeAddress, req: Request) {
        let id = req.id;
        match req.body {
            RequestBody::FindNode { distance } => {
                // if the distance is 0 send our local ENR
                if distance == 0 {
                    let response = Response {
                        id,
                        body: ResponseBody::Nodes {
                            total: 1,
                            nodes: vec![self.local_enr().clone()],
                        },
                    };
                    debug!("Sending our ENR to node: {}", node_address);
                    self.send_to_handler(HandlerRequest::Response(node_address, response))
                        .await;
                } else {
                    self.send_nodes_response(node_address, id, distance).await;
                }
            }
            RequestBody::Ping { enr_seq } => {
                // check if we need to update the known ENR
                match self.kbuckets.entry(&node_address.node_id.into()) {
                    kbucket::Entry::Present(ref mut entry, _) => {
                        if entry.value().seq() < enr_seq {
                            let enr = entry.value().clone();
                            self.request_enr(enr.into()).await;
                        }
                    }
                    kbucket::Entry::Pending(ref mut entry, _) => {
                        if entry.value().seq() < enr_seq {
                            let enr = entry.value().clone();
                            self.request_enr(enr.into()).await;
                        }
                    }
                    // don't know of the ENR, request the update
                    _ => {
                        // The ENR is no longer in our table, we stop responding to PING's
                        return;
                    }
                }

                // build the PONG response
                let src = node_address.socket_addr.clone();
                let response = Response {
                    id,
                    body: ResponseBody::Ping {
                        enr_seq: self.local_enr().seq(),
                        ip: src.ip(),
                        port: src.port(),
                    },
                };
                debug!("Sending PONG response to {}", node_address);
                self.send_to_handler(HandlerRequest::Response(node_address, response))
                    .await;
            }
            _ => {} //TODO: Implement all RPC methods
        }
    }

    /// Processes an RPC response from a peer.
    async fn handle_rpc_response(&mut self, response: Response) {
        // verify we know of the rpc_id
        let id = response.id;
        if let Some((query_id, request, node_address)) = self.active_rpc_requests.remove(&id) {
            if !response.match_request(&request) {
                warn!(
                    "Node gave an incorrect response type. Ignoring response from: {}",
                    node_address
                );
                return;
            }
            match response.body {
                ResponseBody::Nodes { total, mut nodes } => {
                    // Currently a maximum of 16 peers can be returned. Datagrams have a max
                    // size of 1280 and ENR's have a max size of 300 bytes. There should be no
                    // more than 5 responses, to return 16 peers.
                    if total > 5 {
                        warn!("NodesResponse has a total larger than 5, nodes will be truncated");
                    }

                    // filter out any nodes that are not of the correct distance
                    // TODO: If a swarm peer reputation is built - downvote the peer if all
                    // peers do not have the correct distance.
                    let peer_key: kbucket::Key<NodeId> = node_address.node_id.into();
                    let distance_requested = match request {
                        RequestBody::FindNode { distance } => distance,
                        _ => unreachable!(),
                    };
                    if distance_requested != 0 {
                        nodes.retain(|enr| {
                            peer_key.log2_distance(&enr.node_id().clone().into())
                                == Some(distance_requested)
                        });
                    } else {
                        // requested an ENR update
                        nodes.retain(|enr| {
                            peer_key
                                .log2_distance(&enr.node_id().clone().into())
                                .is_none()
                        });
                    }

                    // handle the case that there is more than one response
                    if total > 1 {
                        let mut current_response = self
                            .active_nodes_responses
                            .remove(&node_address.node_id)
                            .unwrap_or_default();

                        debug!(
                            "Nodes Response: {} of {} received",
                            current_response.count, total
                        );
                        // if there are more requests coming, store the nodes and wait for
                        // another response
                        if current_response.count < 5 && (current_response.count as u64) < total {
                            current_response.count += 1;

                            current_response.received_nodes.append(&mut nodes);
                            self.active_nodes_responses
                                .insert(node_address.node_id, current_response);
                            self.active_rpc_requests
                                .insert(id, (query_id, request, node_address));
                            return;
                        }

                        // have received all the Nodes responses we are willing to accept
                        // ignore duplicates here as they will be handled when adding
                        // to the DHT
                        current_response.received_nodes.append(&mut nodes);
                        nodes = current_response.received_nodes;
                    }

                    debug!(
                        "Received a nodes response of len: {}, total: {}, from: {}",
                        nodes.len(),
                        total,
                        node_address
                    );
                    // note: If a peer sends an initial NODES response with a total > 1 then
                    // in a later response sends a response with a total of 1, all previous nodes
                    // will be ignored.
                    // ensure any mapping is removed in this rare case
                    self.active_nodes_responses.remove(&node_address.node_id);

                    self.discovered(&node_address.node_id, nodes, query_id);
                }
                ResponseBody::Ping { enr_seq, ip, port } => {
                    let socket = SocketAddr::new(ip, port);
                    // perform ENR majority-based update if required.
                    let local_socket = self.local_enr().udp_socket();
                    if let Some(ref mut ip_votes) = self.ip_votes {
                        ip_votes.insert(node_address.node_id, socket.clone());
                        let majority_socket = ip_votes.majority();
                        if majority_socket.is_some() && majority_socket != local_socket {
                            let majority_socket = majority_socket.expect("is some");
                            info!("Local UDP socket updated to: {}", majority_socket);
                            self.discv5_events
                                .push_back(Discv5Event::SocketUpdated(majority_socket));
                            if self.update_local_enr_socket(majority_socket, false) {
                                // alert known peers to our updated enr
                                self.ping_connected_peers();
                            }
                        }
                    }

                    // check if we need to request a new ENR
                    if let Some(enr) = self.find_enr(&node_address.node_id) {
                        if enr.seq() < enr_seq {
                            // request an ENR update
                            debug!("Requesting an ENR update from: {}", node_address);
                            let request_body = RequestBody::FindNode { distance: 0 };
                            self.send_rpc_request(&enr.node_id(), request_body, None)
                                .await;
                        }
                        self.connection_updated(
                            node_address.node_id,
                            Some(enr),
                            NodeStatus::Connected,
                        )
                    }
                }
                _ => {} //TODO: Implement all RPC methods
            }
        } else {
            warn!("Received an RPC response which doesn't match a request");
        }
    }

    // Send RPC Requests //

    /// Sends a PING request to a node.
    // TODO: Clean up connected peers. Keep track of ENR
    fn send_ping(&mut self, node_id: &NodeId) {
        let req = RequestBody::Ping {
            enr_seq: self.local_enr().seq(),
        };
        // TODO: Type a HandlerEvent
        self.handler_events.push_back((*node_id, req));
    }

    fn ping_connected_peers(&mut self) {
        // maintain the ping interval
        let connected_nodes: Vec<NodeId> = self.connected_peers.keys().cloned().collect();
        for node_id in connected_nodes {
            self.send_ping(&node_id);
        }
    }

    /// Request an external node's ENR.
    async fn request_enr(&mut self, contact: NodeContact) {
        // Generate a random rpc_id which is matched per node id
        let id: u64 = rand::random();
        let request = Request {
            id,
            body: RequestBody::FindNode { distance: 0 },
        };

        if let Ok(node_address) = contact.node_address() {
            debug!("Sending ENR request to: {}", contact.node_id());

            self.active_rpc_requests
                .insert(id, (None, request.body.clone(), node_address));

            self.send_to_handler(HandlerRequest::Request(contact, request))
                .await;
        }
    }

    async fn send_to_handler(&mut self, handler_request: HandlerRequest) {
        if let Some(send) = self.handler_send.as_mut() {
            send.send(handler_request).await.unwrap_or_else(|_| ());
        } else {
            warn!("Handler shutdown, request not sent");
        }
    }

    /// Sends a NODES response, given a list of found ENR's. This function splits the nodes up
    /// into multiple responses to ensure the response stays below the maximum packet size.
    async fn send_nodes_response(&mut self, node_address: NodeAddress, rpc_id: u64, distance: u64) {
        let nodes: Vec<EntryRefView<'_, NodeId, Enr>> = self
            .kbuckets
            .nodes_by_distance(distance)
            .into_iter()
            .filter(|entry| entry.node.key.preimage() != &node_address.node_id)
            .collect();
        // if there are no nodes, send an empty response
        if nodes.is_empty() {
            let response = Response {
                id: rpc_id,
                body: ResponseBody::Nodes {
                    total: 1u64,
                    nodes: Vec::new(),
                },
            };
            trace!(
                "Sending empty FINDNODES response to: {}",
                node_address.node_id
            );
            self.send_to_handler(HandlerRequest::Response(node_address, response))
                .await;
        } else {
            // build the NODES response
            let mut to_send_nodes: Vec<Vec<Enr>> = Vec::new();
            let mut total_size = 0;
            let mut rpc_index = 0;
            to_send_nodes.push(Vec::new());
            for entry in nodes.into_iter() {
                let entry_size = entry.node.value.clone().encode().len();
                // Responses assume that a session is established. Thus, on top of the encoded
                // ENR's the packet should be a regular message. A regular message has a tag (32
                // bytes), and auth_tag (12 bytes) and the NODES response has an ID (8 bytes) and a total (8 bytes).
                // The encryption adds the HMAC (16 bytes) and can be at most 16 bytes larger so the total packet size can be at most 92 (given AES_GCM).
                if entry_size + total_size < MAX_PACKET_SIZE - 92 {
                    total_size += entry_size;
                    trace!("Adding ENR, Valid: {}", entry.node.value.verify());
                    trace!("Enr: {}", entry.node.value.clone());
                    to_send_nodes[rpc_index].push(entry.node.value.clone());
                } else {
                    total_size = entry_size;
                    to_send_nodes.push(vec![entry.node.value.clone()]);
                    rpc_index += 1;
                }
            }

            let responses: Vec<Response> = to_send_nodes
                .into_iter()
                .map(|nodes| Response {
                    id: rpc_id,
                    body: ResponseBody::Nodes {
                        total: (rpc_index + 1) as u64,
                        nodes,
                    },
                })
                .collect();

            for response in responses {
                trace!(
                    "Sending FINDNODES response to: {}. Response: {} ",
                    node_address,
                    response
                );
                self.send_to_handler(HandlerRequest::Response(node_address.clone(), response))
                    .await;
            }
        }
    }

    /// Constructs and sends a request RPC to the session service given a `QueryInfo`.
    async fn send_rpc_query(
        &mut self,
        query_id: QueryId,
        query_info: QueryInfo,
        return_peer: &ReturnPeer<NodeId>,
    ) {
        let node_id = return_peer.node_id;
        trace!(
            "Sending query. Iteration: {}, NodeId: {}",
            return_peer.iteration,
            node_id
        );

        let req = match query_info.into_rpc_request(return_peer) {
            Ok(r) => r,
            Err(e) => {
                //dst node is local_key, report failure
                error!("Send RPC: {}", e);
                if let Some(query) = self.queries.get_mut(query_id) {
                    query.on_failure(&node_id);
                }
                return;
            }
        };

        self.send_rpc_request(&node_id, req, Some(query_id)).await;
    }

    /// Sends generic RPC requests. Each request gets added to known outputs, awaiting a response.
    // TODO: Avoid looking up the ENR
    // TODO: Add NodeContact
    async fn send_rpc_request(
        &mut self,
        node_id: &NodeId,
        body: RequestBody,
        query_id: Option<QueryId>,
    ) {
        // find the destination ENR
        if let Some(dst_enr) = self.find_enr(&node_id) {
            // Generate a random rpc_id which is matched per node id
            let request: Request = Request {
                id: rand::random(),
                body,
            };
            debug!("Sending RPC {} to node: {}", request, dst_enr.node_id());
            self.send_to_handler(HandlerRequest::Request(dst_enr.into(), request))
                .await;
        } else {
            warn!(
                "Request not sent. Failed to find ENR for Node: {:?}",
                node_id
            );
            if let Some(query_id) = query_id {
                // If this part of query mark it as failed
                if let Some(query) = self.queries.get_mut(query_id) {
                    query.on_failure(&node_id);
                }
            }
        }
    }

    /// Internal function that starts a query.
    fn start_findnode_query(&mut self, target_node: NodeId) -> QueryId {
        let target = QueryInfo {
            query_type: QueryType::FindNode(target_node),
            untrusted_enrs: Default::default(),
        };

        // How many times to call the rpc per node.
        // FINDNODE requires multiple iterations as it requests a specific distance.
        let query_iterations = target.iterations();

        let target_key: kbucket::Key<QueryInfo> = target.clone().into();

        let known_closest_peers = self.kbuckets.closest_keys(&target_key);
        let query_config = FindNodeQueryConfig::new_from_config(&self.config);
        self.queries
            .add_findnode_query(query_config, target, known_closest_peers, query_iterations)
    }

    /// Internal function that starts a query.
    fn start_predicate_query<F>(
        &mut self,
        target_node: NodeId,
        predicate: F,
        num_nodes: usize,
    ) -> QueryId
    where
        F: Fn(&Enr) -> bool + Send + Clone + 'static,
    {
        let target = QueryInfo {
            query_type: QueryType::FindNode(target_node),
            untrusted_enrs: Default::default(),
        };

        // How many times to call the rpc per node.
        // FINDNODE requires multiple iterations as it requests a specific distance.
        let query_iterations = target.iterations();

        let target_key: kbucket::Key<QueryInfo> = target.clone().into();

        let known_closest_peers = self
            .kbuckets
            .closest_keys_predicate(&target_key, predicate.clone());

        let mut query_config = PredicateQueryConfig::new_from_config(&self.config);
        query_config.num_results = num_nodes;
        self.queries.add_predicate_query(
            query_config,
            target,
            known_closest_peers,
            query_iterations,
            predicate,
        )
    }

    /// Processes discovered peers from a query.
    fn discovered(&mut self, source: &NodeId, enrs: Vec<Enr>, query_id: Option<QueryId>) {
        let local_id = self.local_enr().node_id();
        let other_enr_iter = enrs.iter().filter(|p| p.node_id() != local_id);

        for enr_ref in other_enr_iter.clone() {
            // If any of the discovered nodes are in the routing table, and there contains an older ENR, update it.
            self.discv5_events
                .push_back(Discv5Event::Discovered(enr_ref.clone()));

            // ignore peers that don't pass the able filter
            if (self.config.table_filter)(enr_ref) {
                let key = kbucket::Key::from(enr_ref.node_id());
                if !self.config.ip_limit
                    || self
                        .kbuckets
                        .check(&key, enr_ref, { |v, o, l| ip_limiter(v, &o, l) })
                {
                    match self.kbuckets.entry(&key) {
                        kbucket::Entry::Present(mut entry, _) => {
                            if entry.value().seq() < enr_ref.seq() {
                                trace!("Enr updated: {}", enr_ref);
                                *entry.value() = enr_ref.clone();
                            }
                        }
                        kbucket::Entry::Pending(mut entry, _) => {
                            if entry.value().seq() < enr_ref.seq() {
                                trace!("Enr updated: {}", enr_ref);
                                *entry.value() = enr_ref.clone();
                            }
                        }
                        kbucket::Entry::Absent(_entry) => {}
                        _ => {}
                    }
                }
            }
        }

        // if this is part of a query, update the query
        if let Some(query_id) = query_id {
            if let Some(query) = self.queries.get_mut(query_id) {
                let mut peer_count = 0;
                for enr_ref in other_enr_iter.clone() {
                    if query
                        .target_mut()
                        .untrusted_enrs
                        .iter()
                        .position(|e| e.node_id() == enr_ref.node_id())
                        .is_none()
                    {
                        query.target_mut().untrusted_enrs.push(enr_ref.clone());
                    }
                    peer_count += 1;
                }
                debug!("{} peers found for query id {:?}", peer_count, query_id);
                query.on_success(source, &other_enr_iter.cloned().collect::<Vec<_>>())
            }
        }
    }

    /// Update the connection status of a node in the routing table.
    fn connection_updated(
        &mut self,
        node_id: NodeId,
        enr: Option<Enr>,
        mut new_status: NodeStatus,
    ) {
        let key = kbucket::Key::from(node_id);
        if let Some(enr) = enr.as_ref() {
            // ignore peers that don't pass the table filter
            if !(self.config.table_filter)(enr) {
                return;
            }

            // should the ENR be inserted or updated to a value that would exceed the IP limit ban
            if self.config.ip_limit
                && !self
                    .kbuckets
                    .check(&key, enr, { |v, o, l| ip_limiter(v, &o, l) })
            {
                // if the node status is connected and it would exceed the ip ban, consider it
                // disconnected to be pruned.
                new_status = NodeStatus::Disconnected;
            }
        }

        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(mut entry, old_status) => {
                if let Some(enr) = enr {
                    *entry.value() = enr;
                }
                if old_status != new_status {
                    entry.update(new_status);
                }
            }

            kbucket::Entry::Pending(mut entry, old_status) => {
                if let Some(enr) = enr {
                    *entry.value() = enr;
                }
                if old_status != new_status {
                    entry.update(new_status);
                }
            }

            kbucket::Entry::Absent(entry) => {
                if new_status == NodeStatus::Connected {
                    // Note: If an ENR is not provided, no record is added
                    debug_assert!(enr.is_some());
                    if let Some(enr) = enr {
                        match entry.insert(enr, new_status) {
                            kbucket::InsertResult::Inserted => {
                                let event = Discv5Event::NodeInserted {
                                    node_id,
                                    replaced: None,
                                };
                                self.discv5_events.push_back(event);
                            }
                            kbucket::InsertResult::Full => (),
                            kbucket::InsertResult::Pending { disconnected } => {
                                debug_assert!(!self
                                    .connected_peers
                                    .contains_key(disconnected.preimage()));
                                self.send_ping(&disconnected.into_preimage());
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// The equivalent of libp2p `inject_connected()` for a udp session. We have no stream, but a
    /// session key-pair has been negotiated.
    fn inject_session_established(&mut self, enr: Enr) {
        let node_id = enr.node_id();
        debug!("Session established with Node: {}", node_id);
        self.connection_updated(node_id.clone(), Some(enr), NodeStatus::Connected);
        // send an initial ping and start the ping interval
        self.send_ping(&node_id);
        let instant = Instant::now() + self.config.ping_interval;
        self.connected_peers.insert(node_id, instant);
    }

    /// A session could not be established or an RPC request timed-out (after a few retries, if
    /// specified).
    fn rpc_failure(&mut self, id: RequestId) {
        if let Some((query_id_option, request, node_address)) = self.active_rpc_requests.remove(&id)
        {
            let node_id = node_address.node_id;
            match request {
                // if a failed FindNodes request, ensure we haven't partially received packets. If
                // so, process the partially found nodes
                RequestBody::FindNode { .. } => {
                    if let Some(nodes_response) = self.active_nodes_responses.remove(&node_id) {
                        if !nodes_response.received_nodes.is_empty() {
                            warn!(
                                "NODES Response failed, but was partially processed from: {}",
                                node_address
                            );
                            // if it's a query mark it as success, to process the partial
                            // collection of peers
                            self.discovered(
                                &node_id,
                                nodes_response.received_nodes,
                                query_id_option,
                            );
                        }
                    } else {
                        // there was no partially downloaded nodes inform the query of the failure
                        // if it's part of a query
                        if let Some(query_id) = query_id_option {
                            if let Some(query) = self.queries.get_mut(query_id) {
                                query.on_failure(&node_id);
                            }
                        } else {
                            debug!("Failed RPC request: {}: {} ", request, node_address);
                        }
                    }
                }
                // for all other requests, if any are queries, mark them as failures.
                _ => {
                    if let Some(query_id) = query_id_option {
                        if let Some(query) = self.queries.get_mut(query_id) {
                            debug!(
                                "Failed query request: {} for query: {} and {} ",
                                request, *query_id, node_address
                            );
                            query.on_failure(&node_id);
                        }
                    } else {
                        debug!("Failed RPC request: {:?} for node: {} ", request, node_id);
                    }
                }
            }

            self.connection_updated(node_id, None, NodeStatus::Disconnected);
            if self.connected_peers.remove(&node_id).is_some() {
                // report the node as being disconnected
                debug!("Session dropped with {}", node_address);
            }
        }
    }

    pub async fn next_event(&mut self) -> Result<Discv5Event, &'static str> {
        loop {
            if self.handler_recv.is_none() {
                return Err("Discv5 is shutdown");
            }

            tokio::select! {
                Some(event) = self.handler_recv.as_mut().unwrap().next(), if self.handler_recv.is_some() => {
                    match event {
                        HandlerResponse::Established(enr) => {
                            self.inject_session_established(enr);
                        }
                        HandlerResponse::Request(node_address, request) => {
                                self.handle_rpc_request(node_address, request).await;
                            }
                        HandlerResponse::Response(_, response) => {
                                self.handle_rpc_response(response).await;
                            }
                        HandlerResponse::WhoAreYou(whoareyou_ref) => {
                            // check what our latest known ENR is for this node.
                            if let Some(known_enr) = self.find_enr(&whoareyou_ref.0.node_id) {
                                self.send_to_handler(HandlerRequest::WhoAreYou(whoareyou_ref, Some(known_enr))).await;
                            } else {
                                // do not know of this peer
                                debug!("NodeId unknown, requesting ENR. {}", whoareyou_ref.0);
                                self.send_to_handler(HandlerRequest::WhoAreYou(whoareyou_ref, None)).await;
                            }
                        }
                        HandlerResponse::RequestFailed(request_id, error) => {
                            trace!("RPC Request failed: id: {}, error {:?}", request_id, error);
                            self.rpc_failure(request_id);
                        }
                    }
                }
                out_event = Discv5::discv5_event_poll(&mut self.discv5_events, &mut self.kbuckets) => {
                    return Ok(out_event);
                }
                (node_id, request) = Discv5::handler_event_poll(&mut self.handler_events) => {
                    self.send_rpc_request(&node_id, request, None).await
                }
                query_event = Discv5::query_event_poll(&mut self.queries) => {
                    match query_event {
                        QueryEvent::Waiting(query_id, target, return_peer) => {
                            self.send_rpc_query(query_id, target, &return_peer).await;
                        }
                        QueryEvent::Finished(query) => {
                    let query_id = query.id();
                    let result = query.into_result();

                    match result.target.query_type {
                        QueryType::FindNode(node_id) => {
                            return Ok(Discv5Event::FindNodeResult {
                                key: node_id,
                                closer_peers: result
                                    .closest_peers
                                    .filter_map(|p| self.find_enr(&p))
                                    .collect(),
                                query_id,
                            });
                        }
                    }
                    }
                    }
                }
                _ = self.ping_heartbeat.next() => {
                    // check for ping intervals
                    let mut to_send_ping = Vec::new();
                    for (node_id, instant) in self.connected_peers.iter_mut() {
                        if instant.checked_duration_since(Instant::now()).is_none() {
                            *instant = Instant::now() + self.config.ping_interval;
                            to_send_ping.push(node_id.clone());
                        }
                    }
                    for id in to_send_ping.into_iter() {
                        debug!("Sending PING to: {}", id);
                        self.send_ping(&id);
                    }
                }
            }
        }
    }

    async fn handler_event_poll(
        handler_events: &mut VecDeque<(NodeId, RequestBody)>,
    ) -> (NodeId, RequestBody) {
        future::poll_fn(move |_cx| {
            if let Some(event) = handler_events.pop_front() {
                Poll::Ready(event)
            } else {
                Poll::Pending
            }
        })
        .await
    }

    async fn discv5_event_poll(
        events: &mut VecDeque<Discv5Event>,
        kbuckets: &mut KBucketsTable<NodeId, Enr>,
    ) -> Discv5Event {
        future::poll_fn(move |_cx| {
            // Drain queued events
            if let Some(event) = events.pop_front() {
                return Poll::Ready(event);
            }

            // Drain applied pending entries from the routing table.
            if let Some(entry) = kbuckets.take_applied_pending() {
                let event = Discv5Event::NodeInserted {
                    node_id: entry.inserted.into_preimage(),
                    replaced: entry.evicted.map(|n| n.key.into_preimage()),
                };
                return Poll::Ready(event);
            }
            Poll::Pending
        })
        .await
    }

    async fn query_event_poll(queries: &mut QueryPool<QueryInfo, NodeId, Enr>) -> QueryEvent {
        future::poll_fn(move |_cx| match queries.poll() {
            QueryPoolState::Finished(query) => Poll::Ready(QueryEvent::Finished(query)),
            QueryPoolState::Waiting(Some((query, return_peer))) => Poll::Ready(
                QueryEvent::Waiting(query.id(), query.target().clone(), return_peer),
            ),
            QueryPoolState::Timeout(query) => {
                warn!("Query id: {:?} timed out", query.id());
                Poll::Ready(QueryEvent::Finished(query))
            }
            QueryPoolState::Waiting(None) | QueryPoolState::Idle => Poll::Pending,
        })
        .await
    }
}

enum QueryEvent {
    Waiting(QueryId, QueryInfo, ReturnPeer<NodeId>),
    Finished(crate::query_pool::Query<QueryInfo, NodeId, Enr>),
}

/// Takes an `enr` to insert and a list of other `enrs` to compare against.
/// Returns `true` if `enr` can be inserted and `false` otherwise.
/// `enr` can be inserted if the count of enrs in `others` in the same /24 subnet as `enr`
/// is less than `limit`.
fn ip_limiter(enr: &Enr, others: &[&Enr], limit: usize) -> bool {
    let mut allowed = true;
    if let Some(ip) = enr.ip() {
        let count = others.iter().flat_map(|e| e.ip()).fold(0, |acc, x| {
            if x.octets()[0..3] == ip.octets()[0..3] {
                acc + 1
            } else {
                acc
            }
        });
        if count >= limit {
            allowed = false;
        }
    };
    allowed
}

/// Event that can be produced by the `Discv5` service.
#[derive(Debug)]
pub enum Discv5Event {
    /// A node has been discovered from a FINDNODES request.
    ///
    /// The ENR of the node is returned. Various properties can be derived from the ENR.
    /// - `NodeId`: enr.node_id()
    /// - `SeqNo`: enr.seq_no()
    /// - `Ip`: enr.ip()
    Discovered(Enr),
    /// A new ENR was added to the routing table.
    EnrAdded { enr: Enr, replaced: Option<Enr> },
    /// A new node has been added to the routing table.
    NodeInserted {
        node_id: NodeId,
        replaced: Option<NodeId>,
    },
    /// Our local ENR IP address has been updated.
    SocketUpdated(SocketAddr),
    /// Result of a `FIND_NODE` iterative query.
    FindNodeResult {
        /// The key that we looked for in the query.
        key: NodeId,
        /// List of peers ordered from closest to furthest away.
        closer_peers: Vec<Enr>,
        /// Id of the query this result fulfils
        query_id: QueryId,
    },
}
