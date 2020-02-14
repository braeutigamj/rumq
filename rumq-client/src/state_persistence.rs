use crate::MqttState;

use std::iter::FromIterator;
use rumq_core::*;
use serde::{Serialize, Deserialize};
use std::collections::VecDeque;
use tokio::fs;
use async_trait::async_trait;


// State persistence interface
#[async_trait]
pub trait StatePersistence: Send + Sync {
    async fn save(&mut self, state: &MqttState) -> Result<(), tokio::io::Error>;
    async fn load(&mut self) -> MqttState;
}


// Dummy state persistence. Will not save state.
pub struct NoStatePersistence;

#[async_trait]
impl StatePersistence for NoStatePersistence {
    async fn save(&mut self, _: &MqttState) -> Result<(), tokio::io::Error> {
        Ok(())
    }

    async fn load(&mut self) -> MqttState {
        MqttState::new()
    }
}


// Filesystem Persistence. Will serialize State and save it to a file on disk.
pub struct FileSystemStatePersistence {
    pub filename: String
}

#[async_trait]
impl StatePersistence for FileSystemStatePersistence {
    async fn save(&mut self, state: &MqttState) -> Result<(), tokio::io::Error>
    {
        let data: StateData = state.into();
        let content = bincode
            ::serialize(&data)
            .expect("could not encode state");

        fs::write(&self.filename, content).await
    }

    async fn load(&mut self) -> MqttState {
        let content = fs::read(&self.filename).await;

        let data: StateData = match content {
            Ok(bytes) => (bincode
                          ::deserialize(&bytes)
                          .expect("could not parse saved state")),
            _ => StateData {
                last_pkid: PacketIdentifierData(0),
                outgoing_pub: vec![],
                outgoing_rel: vec![],
                incoming_pub: vec![],
            }
        };

        data.into()
    }
}


// Implementation details.
#[derive(Serialize, Deserialize)]
struct PacketIdentifierData(u16);

#[derive(Serialize, Deserialize)]
struct QoSData(u8);

#[derive(Serialize, Deserialize)]
struct PublishData {
    dup: bool,
    qos: QoSData,
    retain: bool,
    topic_name: String,
    pkid: Option<PacketIdentifierData>,
    payload: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct StateData {
    last_pkid: PacketIdentifierData,
    outgoing_pub: Vec<PublishData>,
    outgoing_rel: Vec<PacketIdentifierData>,
    incoming_pub: Vec<PublishData>,
}

macro_rules! container_into {
    ($Container: ty, $src: expr) => {
        <$Container>::from_iter($src.into_iter().map(|el| el.into()))
    }
}


impl From<&MqttState> for StateData {
    fn from(state: &MqttState) -> Self {
        StateData {
            last_pkid: (&state.last_pkid).into(),
            outgoing_pub: container_into!(Vec<PublishData>,
                                          &state.outgoing_pub),
            outgoing_rel: container_into!(Vec<PacketIdentifierData>,
                                          &state.outgoing_rel),
            incoming_pub: container_into!(Vec<PublishData>,
                                          &state.incoming_pub),
        }
    }
}

impl From<StateData> for MqttState {
    fn from(data: StateData) -> Self {
        let mut s = MqttState::new();
        s.last_pkid = data.last_pkid.into();
        s.outgoing_pub = container_into!(VecDeque<Publish>, data.outgoing_pub);
        s.outgoing_rel = container_into!(VecDeque<PacketIdentifier>,
                                         data.outgoing_rel);
        s.incoming_pub = container_into!(VecDeque<Publish>, data.incoming_pub);

        s
    }
}

impl From<&PacketIdentifier> for PacketIdentifierData {
    fn from(pi: &PacketIdentifier) -> Self {
        PacketIdentifierData(pi.0)
    }
}

impl From<PacketIdentifierData> for PacketIdentifier {
    fn from(pi: PacketIdentifierData) -> Self {
        PacketIdentifier(pi.0)
    }
}

impl From<&Publish> for PublishData {
    fn from(p: &Publish) -> Self {
        PublishData {
            dup: p.dup,
            qos: p.qos.into(),
            retain: p.retain,
            topic_name: p.topic_name.clone(),
            pkid: if let Some(pkid) = p.pkid
                    { Some((&pkid).into()) }
                    else { None },
            payload: p.payload.clone(),
        }
    }
}

impl From<PublishData> for Publish {
    fn from(p: PublishData) -> Self {
        Publish {
            dup: p.dup,
            qos: Option::<QoS>::from(p.qos).unwrap(),
            retain: p.retain,
            topic_name: p.topic_name,
            pkid: if let Some(pkid) = p.pkid
                    { Some(pkid.into()) }
                    else { None },
            payload: p.payload,
        }
    }
}

impl From<QoS> for QoSData {
    fn from(qos: QoS) -> Self {
        QoSData(match qos {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        })
    }
}

impl From<QoSData> for Option<QoS> {
    fn from(qos: QoSData) -> Self {
        match qos.0 {
            0 => Some(QoS::AtMostOnce),
            1 => Some(QoS::AtLeastOnce),
            2 => Some(QoS::ExactlyOnce),
            _ => None
        }
    }
}
