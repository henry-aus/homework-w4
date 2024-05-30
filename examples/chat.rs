use anyhow::{Ok, Result};

use dashmap::DashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use futures::SinkExt;
use std::fmt;
use tracing::error;
//use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{fmt::Layer, layer::SubscriberExt, util::SubscriberInitExt, Layer as _};

const CHANNEL_BUFFER_SIZE: usize = 1024;

struct Peer {
    lines: Framed<TcpStream, LinesCodec>,
    rx: mpsc::Receiver<Arc<Message>>,
}

enum Message {
    UserJoined {
        client_name: String,
    },
    UserLeft {
        client_name: String,
    },
    Chat {
        client_name: String,
        content: String,
    },
}

impl Message {
    async fn client_name(&self) -> &str {
        match self {
            Message::UserJoined { client_name } => &client_name,
            Message::UserLeft { client_name } => &client_name,
            Message::Chat {
                client_name,
                #[allow(unused_variables)]
                content,
            } => &client_name,
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::UserJoined { client_name } => write!(f, "[{}] joined", client_name),
            Message::UserLeft { client_name } => write!(f, "[{}] left", client_name),
            Message::Chat {
                client_name,
                content,
            } => write!(f, "[{}]: {}", client_name, content),
        }
    }
}

#[derive(Debug, Default)]
struct State {
    peers: DashMap<String, mpsc::Sender<Arc<Message>>>,
}

impl State {
    async fn add(&self, client_name: &str, lines: Framed<TcpStream, LinesCodec>) -> Result<Peer> {
        let (tx, rx) = mpsc::channel::<Arc<Message>>(CHANNEL_BUFFER_SIZE);

        self.peers.insert(client_name.to_string(), tx);

        Ok(Peer { lines, rx })
    }

    async fn remove_client(&self, client_name: &str) -> Result<()> {
        self.peers.remove(client_name);
        Box::pin(self.broadcast(Message::UserLeft {
            client_name: client_name.to_string(),
        }))
        .await
    }

    async fn broadcast(&self, message: Message) -> Result<()> {
        let message = Arc::new(message);
        let client_name = message.client_name().await;
        for peer in self.peers.iter() {
            if peer.key() == &client_name.to_string() {
                continue;
            }
            if let Err(e) = peer.value().send(message.clone()).await {
                println!("error {} sending message", e);
                self.remove_client(client_name).await?;
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let layer = Layer::new().with_filter(LevelFilter::INFO);
    tracing_subscriber::registry().with(layer).init();

    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await?;

    info!("Server started on {}", addr);

    let state = Arc::new(State::default());
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            // a function to handle the incoming connection
            if let Err(e) = process_connection(stream, state).await {
                error!("Process client connection error. {}", e);
            }
        });
    }

    #[allow(unreachable_code)]
    Ok(())
}

async fn process_connection(stream: TcpStream, state: Arc<State>) -> Result<()> {
    let mut lines = Framed::new(stream, LinesCodec::new());

    lines.send("Please enter your name:").await?;

    let client_name = match lines.next().await {
        Some(result) if result.is_ok() => result.unwrap(),
        // We didn't get a line so we return early here.
        _ => {
            error!("Failed to get username. Client disconnected.");
            return Ok(());
        }
    };

    let mut peer = state.add(&client_name, lines).await?;

    state
        .broadcast(Message::UserJoined {
            client_name: client_name.clone(),
        })
        .await?;

    loop {
        tokio::select! {
            Some(msg) = peer.rx.recv() => {
                peer.lines.send(format!("{}", msg)).await?
            }
            input_chat = peer.lines.next() =>  { match input_chat {
                Some(result) if result.is_ok() => {
                    state.broadcast(Message::Chat{client_name: client_name.clone(), content: result.unwrap()}).await?;
                },
                Some(_) => (),
                None => break
            }}

        }
    }

    state.remove_client(&client_name[..]).await?;

    Ok(())
}
