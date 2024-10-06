use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{atomic::AtomicUsize, Arc, Weak},
    time::Duration,
};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::Request,
    http::{HeaderName, HeaderValue, Uri},
    response::{IntoResponse, Response},
};
use flagset::{flags, FlagSet};
use futures::{
    future::{self, BoxFuture},
    FutureExt,
};
use hyper::{
    client::conn::http2::{self, SendRequest},
    header::CONTENT_TYPE,
    Method,
};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use parking_lot::{lock_api::MappedRwLockReadGuard, Mutex, RawRwLock, RwLock, RwLockReadGuard};
use serde::{de::DeserializeOwned, Serialize};
use socket2::TcpKeepalive;
use tokio::{
    net::TcpStream,
    select,
    sync::{watch, Notify},
    time::MissedTickBehavior,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, trace};

use crate::Node;

flags! {
    pub enum Options: u16 {
        SameAz,
    }
}

#[derive(Clone, Debug)]
pub struct Http2Client {
    name: String,
    from: HeaderValue,
    connections: Arc<RwLock<Vec<Http2Connection>>>,
    round_robin_counter: Arc<AtomicUsize>,
    connections_change: Arc<Notify>,
}

impl Http2Client {
    const MAX_JSON_BODY: usize = 5 * 1024 * 1024;

    pub const X_LBC_FROM: HeaderName = HeaderName::from_static("x-lbc-from");
    pub const X_LBC_TO: HeaderName = HeaderName::from_static("x-lbc-to");

    pub async fn open(
        name: impl Into<String>,
        from: Node,
        mut to: watch::Receiver<Vec<Node>>,
        options: impl Into<FlagSet<Options>>,
    ) -> Result<Self> {
        let name = name.into();
        let options = options.into();
        let round_robin_counter = Arc::new(AtomicUsize::new(0));

        let mut connections = vec![];
        for node in to.borrow_and_update().iter() {
            let connection = match Http2Connection::open(name.to_owned(), node.clone()).await {
                Ok(connection) => connection,
                Err(err) => {
                    debug!(?err, "failed to open http2 connection");
                    continue;
                }
            };
            connections.push(connection);
        }
        debug!(?connections, "initial http2 connections");
        let connections = Arc::new(RwLock::new(connections));

        let connections_change = Arc::new(Notify::new());
        tokio::spawn(Self::reconnect_in_background(
            name.clone(),
            from.az,
            to,
            Arc::downgrade(&connections),
            connections_change.clone(),
            options,
        ));

        let from = HeaderValue::from_str(&from.uuid.to_string())?;

        Ok(Self {
            name,
            from,
            connections,
            round_robin_counter,
            connections_change,
        })
    }

    async fn reconnect_in_background(
        client: String,
        from_az: String,
        mut to: watch::Receiver<Vec<Node>>,
        connections: Weak<RwLock<Vec<Http2Connection>>>,
        connections_change: Arc<Notify>,
        options: FlagSet<Options>,
    ) {
        let mut ticks = tokio::time::interval(Http2Connection::KEEP_ALIVE);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        while let Some(connections) = connections.upgrade() {
            select! {
                maybe_err = to.changed() => {
                    if maybe_err.is_err() {
                        break;
                    }
                }
                _ = ticks.tick() => {}
            }

            let mut changed = false;
            let nodes_to_reconnect = {
                let mut connections = connections.write();
                let mut connections_by_node_uuid = HashMap::new();
                for connection in connections.drain(0..) {
                    connections_by_node_uuid.insert(connection.node.uuid, connection);
                }
                let mut to_reconnect = Vec::new();
                for node in to.borrow_and_update().iter() {
                    // AZ affinity
                    if options.contains(Options::SameAz) && node.az != from_az {
                        continue;
                    }

                    if let Some(existing_connection) = connections_by_node_uuid.remove(&node.uuid) {
                        if existing_connection.is_closed() {
                            to_reconnect.push(node.clone());
                        } else {
                            connections.push(existing_connection);
                        }
                    } else {
                        to_reconnect.push(node.clone());
                    }
                }
                if !connections_by_node_uuid.is_empty() {
                    changed = true;
                    drop(connections_by_node_uuid); // remaining connections to old nodes
                }

                to_reconnect // new nodes or closed connections to existing nodes
            };

            for node in nodes_to_reconnect {
                let connection = match Http2Connection::open(client.to_owned(), node).await {
                    Ok(connection) => connection,
                    Err(err) => {
                        debug!(?client, ?err, "failed to open http2 connection");
                        continue;
                    }
                };
                connections.write().push(connection);
                changed = true;
            }

            if changed {
                connections_change.notify_waiters();
                debug!(?client, connections = ?*connections.read(), "http2 connections updated");
            }
        }
    }

    pub(crate) async fn send_request(
        &self,
        req: Request<Body>,
        retry: bool,
    ) -> Result<Response, (anyhow::Error, Option<Request<Body>>)> {
        let mut req = Some(req);
        let mut exponential_backoff = Duration::from_secs(1);
        loop {
            let res = match (self.get_connection(), req.take()) {
                (Some(connection), Some(mut req)) => {
                    req.headers_mut()
                        .insert(Self::X_LBC_FROM, self.from.clone());
                    req.headers_mut()
                        .insert(Self::X_LBC_TO, connection.to.clone());
                    Some(connection.send(req))
                }
                (_, old_req) => {
                    req = old_req;
                    None
                }
            };
            if let Some(res) = res {
                match res.await {
                    Ok(res) => return Ok(res),
                    Err((err, Some(old_req))) => {
                        req = Some(old_req);
                        debug!(?err, "request failed, retrying");
                        tokio::time::sleep(exponential_backoff).await;
                        exponential_backoff = exponential_backoff.min(Duration::from_secs(30)) * 2;
                    }
                    Err((err, None)) => return Err((err, None)),
                }
            } else {
                debug!(client = ?self.name, "no connection available, waiting for new connections");
                if retry {
                    self.connections_change.notified().await;
                } else {
                    return Err((anyhow!("no connection available"), None));
                }
            }
        }
    }

    pub(crate) async fn send_json_request<B, U, R>(
        &self,
        method: Method,
        uri: U,
        body: &B,
        retry: bool,
    ) -> Result<R>
    where
        B: Serialize,
        U: TryInto<Uri>,
        <U as TryInto<Uri>>::Error: Into<anyhow::Error>,
        R: DeserializeOwned,
    {
        let uri: Uri = uri.try_into().map_err(|e| e.into())?;
        let json_body = serde_json::to_string(&body)?;
        let body = Body::from(json_body);
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(CONTENT_TYPE, "application/json")
            .body(body)?;
        let response = self
            .send_request(request, retry)
            .await
            .map_err(|(e, _)| e)?;
        if response.status().is_success() {
            let body = axum::body::to_bytes(response.into_body(), Self::MAX_JSON_BODY).await?;
            Ok(serde_json::from_slice(&body)?)
        } else {
            Err(anyhow!("request failed: {:?}", response))
        }
    }

    pub async fn request(&self, request: Request) -> Result<Response> {
        self.send_request(request, true)
            .await
            .map_err(|(e, _)| e.into())
    }

    pub async fn json_request<B, U, R>(&self, method: Method, uri: U, body: &B) -> Result<R>
    where
        B: Serialize,
        U: TryInto<Uri>,
        <U as TryInto<Uri>>::Error: Into<anyhow::Error>,
        R: DeserializeOwned,
    {
        self.send_json_request(method, uri, body, true).await
    }

    fn get_connection(&self) -> Option<MappedRwLockReadGuard<'_, RawRwLock, Http2Connection>> {
        let connections = self.connections.read();
        let round_robin = self
            .round_robin_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        RwLockReadGuard::try_map(connections, |connections| {
            if connections.is_empty() {
                None
            } else {
                connections
                    .get(round_robin % connections.len())
                    .filter(|connection| !connection.is_closed())
            }
        })
        .ok()
    }

    #[cfg(test)]
    fn close_to(&self, node: &Node) {
        let connections = self.connections.read();
        for connection in connections.iter() {
            if connection.node == *node {
                connection.close();
            }
        }
    }
}

struct Http2Connection {
    node: Node,
    to: HeaderValue,
    sender: Mutex<SendRequest<Body>>,
    is_closed: CancellationToken,
    #[allow(unused)]
    close: DropGuard,
}

impl Debug for Http2Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http2Connection")
            .field("node", &self.node)
            .field("is_closed", &self.is_closed.is_cancelled())
            .finish()
    }
}

impl Http2Connection {
    const KEEP_ALIVE: Duration = Duration::from_secs(5);

    pub async fn open(client: String, node: Node) -> Result<Http2Connection> {
        let stream = TcpStream::connect(node.address).await?;

        // properly configure the TCP connection
        let stream = {
            let stream = stream.into_std()?;
            let socket = socket2::SockRef::from(&stream);
            socket.set_nodelay(true)?;
            socket.set_tcp_keepalive(&TcpKeepalive::new().with_time(Self::KEEP_ALIVE))?;
            socket.set_tcp_user_timeout(Some(Self::KEEP_ALIVE))?;
            tokio::net::TcpStream::from_std(stream)?
        };

        let io = TokioIo::new(stream);
        let (mut sender, conn) = http2::Builder::new(TokioExecutor::new())
            .keep_alive_interval(Self::KEEP_ALIVE)
            .keep_alive_timeout(Self::KEEP_ALIVE)
            .keep_alive_while_idle(true)
            .timer(TokioTimer::new())
            .handshake(io)
            .await?;
        let close = CancellationToken::new();

        {
            let close = close.clone();
            tokio::spawn(async move {
                select! {
                    _ = close.cancelled() => {
                      trace!(?client, "http2 connection closed by client");
                    }
                    conn = conn => {
                        if let Err(err) = conn {
                            error!(?client,?err, "http2 connection error");
                        } else {
                            debug!(?client, "http2 connection closed by server");
                        }
                        close.cancel();
                    }
                }
            });
        }

        sender.ready().await?;
        let is_closed = close.clone();
        let close = close.drop_guard();
        let sender = Mutex::new(sender);
        let to = HeaderValue::from_str(&node.uuid.to_string())?;

        Ok(Self {
            node,
            to,
            sender,
            is_closed,
            close,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.is_cancelled()
    }

    pub fn send(
        &self,
        req: Request<Body>,
    ) -> BoxFuture<'static, Result<Response, (anyhow::Error, Option<Request<Body>>)>> {
        if self.is_closed.is_cancelled() {
            future::ready(Err((anyhow!("http2 connection closed"), Some(req)))).boxed()
        } else {
            let res = {
                let mut sender = self.sender.lock();
                sender.try_send_request(req)
            };
            res.map(|res| match res {
                Ok(res) => Ok(res.into_response()),
                Err(mut err) => {
                    let request = err.take_message();
                    Err((err.into_error().into(), request))
                }
            })
            .boxed()
        }
    }

    #[allow(unused)]
    pub fn close(&self) {
        self.is_closed.cancel();
    }
}

#[cfg(test)]
mod tests {
    use axum::{debug_handler, extract::State, routing::get, Json};
    use hyper::HeaderMap;
    use test_log::test;
    use tracing::info;

    use super::*;

    struct Server {
        node: Node,
        requests_count: Arc<AtomicUsize>,
        #[allow(unused)]
        shutdown: DropGuard,
    }

    impl Server {
        fn count_requests(&self) -> usize {
            self.requests_count
                .load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    async fn start_server(name: &str) -> Result<Server> {
        let server = name.to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let node = Node::new("az".to_owned(), listener.local_addr()?);
        info!(?server, ?node, "server started");
        let cancel = CancellationToken::new();
        let shutdown = cancel.clone().drop_guard();
        let requests_count = Arc::new(AtomicUsize::new(0));
        let state = (requests_count.clone(), server.clone(), node.clone());
        tokio::spawn(async move {
            #[debug_handler]
            async fn yo(
                State((requests_count, server, node)): State<(Arc<AtomicUsize>, String, Node)>,
                headers: HeaderMap,
                Json(payload): Json<String>,
            ) -> impl IntoResponse {
                requests_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                debug!(?server, ?headers, payload, "yo");
                assert_eq!(
                    headers
                        .get(Http2Client::X_LBC_TO)
                        .unwrap_or(&HeaderValue::from_static("?"))
                        .to_str()
                        .unwrap_or_default(),
                    node.uuid.to_string()
                );
                assert!(headers.get(Http2Client::X_LBC_FROM).is_some(),);
                Json("yo")
            }

            let router = axum::Router::new().route("/yo", get(yo)).with_state(state);

            select! {
                _ = cancel.cancelled() => {}
                _ = axum::serve(listener, router) => {}
            }

            // take care: at this point the server won't accept new connections but won't close the existing ones
            info!(?server, "server stopped");
        });
        Ok(Server {
            node,
            requests_count,
            shutdown,
        })
    }

    #[test(tokio::test)]
    async fn connect_client() -> Result<()> {
        // we have a server somewhere
        let server = start_server("server").await?;

        // we keep track of the nodes
        let (_, nodes) = watch::channel(vec![server.node.clone()]);

        // let's connect to the server
        let client = Http2Client::open(
            "test",
            Node::new("X", "127.0.0.1:8000".parse()?),
            nodes,
            None,
        )
        .await?;

        debug!("client connected");

        // we can send a request
        let res: String = client.json_request(Method::GET, "/yo", &"1").await?;
        assert_eq!(res, "yo");

        Ok(())
    }

    #[test(tokio::test)]
    async fn no_server() -> Result<()> {
        // we keep track of the nodes
        let (_, nodes) = watch::channel(vec![]);

        // let's connect to the server
        let client = Http2Client::open(
            "test",
            Node::new("X", "127.0.0.1:8000".parse()?),
            nodes,
            None,
        )
        .await?;

        // we can send a request
        let res: Result<String> = client
            .send_json_request(Method::GET, "/yo", &"1", false)
            .await;

        // because we specified a retry timeout of 0, we should get an error
        assert!(res.is_err());

        Ok(())
    }

    #[test(tokio::test)]
    async fn server_joining_late() -> Result<()> {
        // we keep track of the nodes
        let (update_nodes, nodes) = watch::channel(vec![]);

        // let's connect to the server
        let client = Http2Client::open(
            "test",
            Node::new("X", "127.0.0.1:8000".parse()?),
            nodes,
            None,
        )
        .await?;

        // we can send a request
        let mut res = client
            .json_request::<_, _, String>(Method::GET, "/yo", &"1")
            .boxed();

        // for now the client is still waiting for a connection
        assert!((&mut res).now_or_never().is_none());

        // but let's add a server
        let server = start_server("server").await?;

        // and update the nodes
        update_nodes.send(vec![server.node.clone()])?;

        // so the request will eventually succeed
        assert_eq!(res.await?, "yo");

        Ok(())
    }

    #[test(tokio::test)]
    async fn replace_server() -> Result<()> {
        // let's have 2 servers
        let server1 = start_server("server1").await?;
        let server2 = start_server("server2").await?;

        let (update_nodes, nodes) = watch::channel(vec![]);

        // the client only knows about the first server
        update_nodes.send(vec![server1.node.clone()])?;
        let client = Http2Client::open(
            "test",
            Node::new("X", "127.0.0.1:8000".parse()?),
            nodes,
            None,
        )
        .await?;

        // we can send a request
        let res: String = client.json_request(Method::GET, "/yo", &"1").await?;
        assert_eq!(res, "yo");
        assert_eq!(1, server1.count_requests());

        // now we replace the server
        client.close_to(&server1.node);
        drop(server1);
        update_nodes.send(vec![server2.node.clone()])?;

        // we can send a request
        let res: String = client.json_request(Method::GET, "/yo", &"2").await?;
        assert_eq!(res, "yo");
        assert_eq!(1, server2.count_requests());

        Ok(())
    }

    #[test(tokio::test)]
    async fn round_robin() -> Result<()> {
        // let's have 2 servers
        let server1 = start_server("server1").await?;
        let server2 = start_server("server2").await?;

        let (update_nodes, nodes) = watch::channel(vec![]);

        // the client only knows about both servers
        update_nodes.send(vec![server1.node.clone(), server2.node.clone()])?;
        let client = Http2Client::open(
            "test",
            Node::new("X", "127.0.0.1:8000".parse()?),
            nodes,
            None,
        )
        .await?;

        // let's make 10 requests
        for i in 0..10 {
            let res: String = client
                .json_request(Method::GET, "/yo", &i.to_string())
                .await?;
            assert_eq!(res, "yo");
        }

        // the requests should be distributed evenly
        assert_eq!(5, server1.count_requests());
        assert_eq!(5, server2.count_requests());

        // remove server1
        update_nodes.send(vec![server2.node.clone()])?;
        client.close_to(&server1.node);

        // let's make 10 more requests
        for i in 0..10 {
            let res: String = client
                .json_request(Method::GET, "/yo", &i.to_string())
                .await?;
            assert_eq!(res, "yo");
        }

        assert_eq!(5, server1.count_requests());
        assert_eq!(15, server2.count_requests());

        Ok(())
    }
}
