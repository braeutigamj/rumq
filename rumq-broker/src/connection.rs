use derive_more::From;
use rumq_core::{connack, Packet, Connect, ConnectReturnCode};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tokio::stream::iter;
use tokio::stream::StreamExt;
use tokio::time;
use tokio::time::Elapsed;
use tokio::time::Instant;
use tokio::select;
use futures_util::sink::SinkExt;
use futures_util::stream::Stream;

use crate::router::{self, Router, RouterMessage};
use crate::ServerSettings;
use crate::Network;

use std::sync::Arc;
use std::time::Duration;
use std::rc::Rc;
use std::cell::RefCell;
use std::io;

#[derive(Debug, From)]
pub enum Error {
    Io(io::Error),
    Core(rumq_core::Error),
    Router(router::Error),
    Timeout(Elapsed),
    KeepAlive,
    Send(SendError<(String, RouterMessage)>),
    /// Received a wrong packet while waiting for another packet
    WrongPacket,
    /// Invalid client ID
    InvalidClientId,
    NotConnack,
    StreamDone
}

pub async fn eventloop<S: Network>(config: Arc<ServerSettings>, router: Rc<RefCell<Router>>, stream: S, mut router_tx: Sender<(String, RouterMessage)>) -> Result<String, Error> {
    let mut connection = Connection::new(config, router, stream, router_tx.clone()).await?;
    let id = connection.id.clone();

    if let Err(err) = connection.run().await {
        error!("Connection error = {:?}. Id = {}", err, id);
        router_tx.send((id.clone(), RouterMessage::Death(id.clone()))).await?;
    }

    Ok(id)
}

pub struct Connection<S> {
    id:         String,
    keep_alive: Duration,
    stream:     S,
    this_rx:    Receiver<RouterMessage>,
    router: Rc<RefCell<Router>>,
    router_tx:  Sender<(String, RouterMessage)>,
}

impl<S: Network> Connection<S> {
    async fn new(config: Arc<ServerSettings>, router: Rc<RefCell<Router>>, mut stream: S, router_tx: Sender<(String, RouterMessage)>) -> Result<Connection<S>, Error> {
        let (this_tx, this_rx) = channel(100);
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let connect = time::timeout(timeout, async {
            let packet = stream.next().await.ok_or(Error::StreamDone)??;
            let o = handle_incoming_connect(packet)?;
            Ok::<_, Error>(o)
        })
        .await??;


        let id = connect.client_id.clone();
        let keep_alive = Duration::from_secs(connect.keep_alive as u64);

        let message = router.borrow_mut().handle_connect(connect, this_tx)?;
        let mut pending = match message {
            Some(RouterMessage::Pending(packets)) => packets,
            _ => unimplemented!()
        };

        // eventloop which pending packets from the last session 
        if pending.len() > 0 {
            let connack = connack(ConnectReturnCode::Accepted, true);
            let packet = Packet::Connack(connack);
            let keep_alive = keep_alive + keep_alive.mul_f32(0.5);

            stream.send(packet).await?;

            let mut pending = iter(pending.drain(..)).map(|publish| RouterMessage::Packet(Packet::Publish(publish)));
            let mut incoming = time::throttle(Duration::from_millis(100), &mut stream);
            let mut timeout = time::delay_for(keep_alive);

            loop {
                let (done, routermessage) = select(&mut incoming, &mut pending, keep_alive, &mut timeout).await?;
                if let Some(mut message) = routermessage {
                    match router.borrow_mut().handle_incoming_router_message(id.clone(), &mut message) {
                        Ok(Some(RouterMessage::Packet(packet))) => incoming.get_mut().send(packet).await?,
                        Ok(_) => (),
                        Err(e) => {
                            error!("Incoming handle error = {:?}", e);
                            continue;
                        }
                    }
                }

                if done {
                    break
                }
            }
        } else {
            let connack = connack(ConnectReturnCode::Accepted, false);
            let packet = Packet::Connack(connack);
            stream.send(packet).await?;
        }

        let connection = Connection { id, keep_alive, stream, this_rx, router, router_tx };
        Ok(connection)
    }


    async fn run(&mut self) -> Result<(), Error> {
        let keep_alive = self.keep_alive + self.keep_alive.mul_f32(0.5);
        let id = self.id.clone();

        // eventloop which processes packets and router messages
        let mut incoming = &mut self.stream;
        let mut incoming = time::throttle(Duration::from_millis(0), &mut incoming);
        loop {
            let mut timeout = time::delay_for(keep_alive);
            let (done, routermessage) = select(&mut incoming, &mut self.this_rx, keep_alive, &mut timeout).await?; 
            if let Some(mut message) = routermessage {
                match self.router.borrow_mut().handle_incoming_router_message(id.clone(), &mut message) {
                    Ok(Some(RouterMessage::Packet(packet))) => incoming.get_mut().send(packet).await?,
                    Ok(_) => (),
                    Err(e) => {
                        error!("Incoming handle error = {:?}", e);
                        continue;
                    }
                }
            
                let _ = self.router.borrow_mut().route(id.clone(), message);
            }

            if done {
                break
            }
        }

        Ok(())
    }
}

use tokio::time::Throttle;
use tokio::time::Delay;

/// selects incoming packets from the network stream and router message stream
/// Forwards router messages to network
/// bool field can be used to instruct outer loop to stop processing messages
async fn select<S: Network>(
    stream: &mut Throttle<S>, 
    mut outgoing: impl Stream<Item = RouterMessage> + Unpin,
    keep_alive: Duration,
    mut timeout: &mut Delay) -> Result<(bool, Option<RouterMessage>), Error> {

    let keepalive = &mut timeout;
    select! {
        _ = keepalive => return Err(Error::KeepAlive),
        o = stream.next() => {
            timeout.reset(Instant::now() + keep_alive);
            let o = match o {
                Some(o) => o,
                None => {
                    let done = true;
                    let packet = None;
                    return Ok((done, packet))
                }
            };

            match o? {
                Packet::Pingreq => stream.get_mut().send(Packet::Pingresp).await?,
                packet => {
                    let message = Some(RouterMessage::Packet(packet));
                    let done = false;
                    return Ok((done, message))
                }
            }
        }
        o = outgoing.next() => match o {
            Some(RouterMessage::Packet(packet)) => stream.get_mut().send(packet).await?,
            Some(message) => {
                warn!("Invalid router message = {:?}", message);
                return Ok((false, None))
            }
            None => {
                let message = None;
                let done = true;
                return Ok((done, message))
            }
        }
    }

    Ok((false, None))
}

pub fn handle_incoming_connect(packet: Packet) -> Result<Connect, Error> {
    let mut connect = match packet {
        Packet::Connect(connect) => connect,
        packet => {
            error!("Invalid packet. Expecting connect. Received = {:?}", packet);
            return Err(Error::WrongPacket);
        }
    };

    // this broker expects a keepalive. 0 keepalives are promoted to 10 minutes
    if connect.keep_alive == 0 {
        warn!("0 keepalive. Promoting it to 10 minutes");
        connect.keep_alive = 10 * 60;
    }

    if connect.client_id.starts_with(' ') || connect.client_id.is_empty() {
        error!("Client id shouldn't start with space (or) shouldn't be emptys");
        return Err(Error::InvalidClientId);
    }

    Ok(connect)
}
