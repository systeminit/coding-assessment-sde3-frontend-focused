use axum::{
    extract::{
        ws::{Message, WebSocket},
        Extension, WebSocketUpgrade,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    AddExtensionLayer, Json, Router,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{
    broadcast::{self, Receiver, Sender},
    RwLock,
};
use tower_http::cors::CorsLayer;

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Debug, Error)]
pub enum ChatError {
    #[error("not found")]
    NotFound,
    #[error("broadcast send error: {0}")]
    SendError(#[from] broadcast::error::SendError<BroadcastPayload>),
    #[error("broadcast recv error: {0}")]
    RecvError(#[from] broadcast::error::RecvError),
}

pub type ChatResult<T> = std::result::Result<T, ChatError>;

impl IntoResponse for ChatError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            ChatError::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        let body = Json(
            serde_json::json!({ "error": { "message": error_message, "code": 42, "statusCode": status.as_u16() } }),
        );

        (status, body).into_response()
    }
}

#[derive(Debug, Clone)]
pub struct Users {
    data: Arc<RwLock<HashSet<String>>>,
}

impl Users {
    pub fn new() -> Self {
        Users {
            data: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn add_user(&mut self, user: impl Into<String>) {
        let user = user.into();
        let mut data = self.data.write().await;
        data.insert(user);
    }

    pub async fn list(&self) -> Vec<String> {
        let data = self.data.read().await;
        let mut results: Vec<String> = data.iter().map(|s| s.into()).collect();
        results.sort();
        results
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessagePayload {
    pub index: usize,
    pub user: Arc<String>,
    pub message: Arc<String>,
}

impl MessagePayload {
    pub fn new(user: String, message: String, index: usize) -> Self {
        MessagePayload {
            index,
            user: Arc::new(user),
            message: Arc::new(message),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Messages {
    messages: Arc<RwLock<Vec<MessagePayload>>>,
    counter: Arc<AtomicUsize>,
}

impl Messages {
    pub fn new() -> Self {
        Messages {
            messages: Arc::new(RwLock::new(Vec::new())),
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn send(
        &mut self,
        user: impl Into<String>,
        message: impl Into<String>,
    ) -> MessagePayload {
        let user = user.into();
        let message = message.into();
        let index = self.counter.fetch_add(1, Ordering::SeqCst);
        let payload = MessagePayload::new(user, message, index);
        let mut messages = self.messages.write().await;
        messages.push(payload.clone());
        payload
    }

    pub async fn list(&self) -> Vec<MessagePayload> {
        let messages = self.messages.read().await;
        let mut results: Vec<MessagePayload> = messages.iter().map(|m| m.clone()).collect();
        results.sort_by_key(|m| m.index);
        results
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BroadcastPayload {
    Message(MessagePayload),
    SignIn(SignInResponse),
}

#[derive(Debug, Clone)]
pub struct Broadcast {
    tx: Sender<BroadcastPayload>,
}

impl Broadcast {
    pub fn new() -> (Self, Receiver<BroadcastPayload>) {
        let (tx, rx) = broadcast::channel(1000);
        (Broadcast { tx }, rx)
    }

    pub fn sign_in(&self, response: SignInResponse) -> ChatResult<()> {
        self.tx.send(BroadcastPayload::SignIn(response))?;
        Ok(())
    }

    pub fn send_message(&self, response: MessagePayload) -> ChatResult<()> {
        self.tx.send(BroadcastPayload::Message(response))?;
        Ok(())
    }

    pub fn subscribe(&self) -> Receiver<BroadcastPayload> {
        self.tx.subscribe()
    }
}

pub async fn log_broadcast(mut rx: Receiver<BroadcastPayload>) -> ChatResult<()> {
    loop {
        let payload = rx.recv().await?;
        dbg!(payload);
    }
}

pub fn app(users: Users, messages: Messages, broadcast: Broadcast) -> Router {
    Router::new()
        .route("/signin", post(signin))
        .route("/users", get(users_list))
        .route("/messages", get(messages_list))
        .route("/messages", post(message_send))
        .route("/ws", get(websocket_endpoint))
        .layer(AddExtensionLayer::new(users))
        .layer(AddExtensionLayer::new(messages))
        .layer(AddExtensionLayer::new(broadcast))
        .layer(CorsLayer::permissive())
}

pub fn state() -> (Users, Messages, Broadcast, Receiver<BroadcastPayload>) {
    let users = Users::new();
    let messages = Messages::new();
    let (broadcast, rx) = Broadcast::new();
    (users, messages, broadcast, rx)
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct SignInRequest {
    user: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct SignInResponse {
    user: String,
}

async fn signin(
    Json(request): Json<SignInRequest>,
    Extension(mut users): Extension<Users>,
    Extension(broadcast): Extension<Broadcast>,
) -> ChatResult<Json<SignInResponse>> {
    users.add_user(&request.user).await;
    let response = SignInResponse { user: request.user };
    broadcast.sign_in(response.clone())?;
    Ok(Json(response))
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct UsersListResponse {
    users: Vec<String>,
}

async fn users_list(Extension(users): Extension<Users>) -> ChatResult<Json<UsersListResponse>> {
    let users = users.list().await;
    let response = UsersListResponse { users };
    Ok(Json(response))
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct MessagesListResponse {
    messages: Vec<MessagePayload>,
}

async fn messages_list(
    Extension(messages): Extension<Messages>,
) -> ChatResult<Json<MessagesListResponse>> {
    let messages = messages.list().await;
    let response = MessagesListResponse { messages };
    Ok(Json(response))
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct MessageSendRequest {
    user: String,
    message: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct MessageSendResponse {
    index: usize,
}

async fn message_send(
    Json(request): Json<MessageSendRequest>,
    Extension(mut messages): Extension<Messages>,
    Extension(broadcast): Extension<Broadcast>,
) -> ChatResult<Json<MessageSendResponse>> {
    let message = messages.send(request.user, request.message).await;
    broadcast.send_message(message.clone())?;
    let response = MessageSendResponse {
        index: message.index,
    };
    Ok(Json(response))
}

async fn websocket_endpoint(
    ws: WebSocketUpgrade,
    Extension(broadcast): Extension<Broadcast>,
) -> impl IntoResponse {
    let broadcast = broadcast.clone();
    ws.on_upgrade(move |socket| handle_socket(socket, broadcast))
}

async fn handle_socket(mut socket: WebSocket, broadcast: Broadcast) {
    let mut rx = broadcast.subscribe();
    loop {
        tokio::select! {
            payload = rx.recv() => {
                match payload {
                    Ok(payload) => {
                        socket.send(Message::Text(serde_json::to_string(&payload).expect("cannot serialize broadcast payload; bug!"))).await.expect("cannot send on the outbound socket!");
                    },
                    Err(e) => {dbg!("broadcast receive error: {:0?}", e);},
                }
            },
            inbound = socket.recv() => {
                match inbound {
                    Some(msg) => {
                        match msg {
                            Ok(Message::Close(_)) => {
                                dbg!("client disconnected");
                                return;
                            },
                            Ok(Message::Text(_)) => {
                                dbg!("text messages are not used; you have a bug!");
                            },
                            Ok(Message::Binary(_)) => {
                                dbg!("binary messages are not used; you have a bug!");
                            },
                            Ok(Message::Ping(_)) => {
                                dbg!("socket ping");
                            },
                            Ok(Message::Pong(_)) => {
                                dbg!("socket pong");
                            },
                            Err(e) => {
                                dbg!("error in message receipt from websocket; bug!");
                                dbg!(&e);
                                return;
                            }
                        }
                    },
                    None => {
                        dbg!("client disconnected");
                        return;
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let (users, messages, broadcast, rx) = state();
    let _log_handle = tokio::spawn(log_broadcast(rx));
    let app = app(users, messages, broadcast);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .expect("cannot start service");
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, TcpListener};

    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
    };
    use futures_util::StreamExt;
    use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn signin() {
        let (users, messages, broadcast, mut rx) = state();
        let app = app(users.clone(), messages, broadcast);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/signin")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SignInRequest {
                            user: "adam".to_string(),
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let response: SignInResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            response,
            SignInResponse {
                user: "adam".to_string()
            }
        );
        let payload = rx.try_recv().expect("should have a payload");
        match payload {
            BroadcastPayload::SignIn(s) => {
                assert_eq!(response, s);
            }
            _ => panic!("expected a signin payload, but got something else"),
        }
        let users = users.list().await;
        assert_eq!(users[0], "adam");
    }

    #[tokio::test]
    async fn users_list() {
        let (mut users, messages, broadcast, _rx) = state();
        users.add_user("adam").await;
        users.add_user("frank").await;
        let app = app(users.clone(), messages, broadcast);
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/users")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let response: UsersListResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            response,
            UsersListResponse {
                users: vec!["adam".to_string(), "frank".to_string()]
            }
        );
    }

    #[tokio::test]
    async fn messages_list() {
        let (mut users, mut messages, broadcast, _rx) = state();
        users.add_user("adam").await;
        messages.send("adam", "municipal waste").await;
        users.add_user("frank").await;
        messages.send("frank", "black sabbath").await;
        let app = app(users.clone(), messages, broadcast);
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/messages")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let response: MessagesListResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            response,
            MessagesListResponse {
                messages: vec![
                    MessagePayload {
                        index: 0,
                        user: Arc::new("adam".to_string()),
                        message: Arc::new("municipal waste".to_string()),
                    },
                    MessagePayload {
                        index: 1,
                        user: Arc::new("frank".to_string()),
                        message: Arc::new("black sabbath".to_string()),
                    },
                ]
            }
        );
    }

    #[tokio::test]
    async fn message_send() {
        let (mut users, messages, broadcast, mut rx) = state();
        users.add_user("adam").await;
        let app = app(users.clone(), messages, broadcast);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/messages")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&MessageSendRequest {
                            user: "adam".to_string(),
                            message: "wewt".to_string(),
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let response: MessageSendResponse = serde_json::from_slice(&body).unwrap();
        let payload = rx.recv().await.expect("cannot get payload");
        match payload {
            BroadcastPayload::Message(msg) => {
                assert_eq!(response.index, msg.index);
                assert_eq!(msg.user.as_ref(), "adam");
                assert_eq!(msg.message.as_ref(), "wewt");
            }
            _ => panic!("wrong message type returned"),
        }
    }

    #[tokio::test]
    async fn websocket() {
        let (mut users, mut messages, broadcast, _rx) = state();
        let app = app(users.clone(), messages.clone(), broadcast.clone());

        let listener = TcpListener::bind("0.0.0.0:0".parse::<SocketAddr>().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::Server::from_tcp(listener)
                .unwrap()
                .serve(app.into_make_service())
                .await
                .unwrap();
        });

        let url = url::Url::parse(&format!("ws://{addr}/ws")).expect("cannot parse url");

        let (mut ws_stream, _) = connect_async(url).await.expect("failed to connect");
        users.add_user("adam").await;
        broadcast.sign_in(SignInResponse { user: "adam".to_string() }).expect("cannot brodcast signin");
        let message_payload = messages.send("adam", "woohoo").await;
        broadcast
            .send_message(message_payload)
            .expect("cannot send message");

        // First payload is the sign in
        {
            let item = ws_stream.next().await.expect("cannot get next message");
            match item {
                Ok(message) => match message {
                    Message::Text(payload) => {
                        let broadcast_payload: BroadcastPayload =
                        serde_json::from_str(&payload).expect("cannot deserialize payload");
                        match broadcast_payload {
                            BroadcastPayload::Message(_) => panic!("got a broadcast message payload out of order"),
                            BroadcastPayload::SignIn(sign_in) => {
                                assert_eq!(sign_in, SignInResponse { user: "adam".to_string() });
                            }
                        }
                    }
                    p => {
                        panic!("websocket received invalid payload: {}", p);
                    }
                },
                Err(e) => {
                    panic!("websocket stream error: {}", e.to_string());
                }
            }
        }

        // Second payload is the message 
        {
            let item = ws_stream.next().await.expect("cannot get next message");
            match item {
                Ok(message) => match message {
                    Message::Text(payload) => {
                        let broadcast_payload: BroadcastPayload =
                        serde_json::from_str(&payload).expect("cannot deserialize payload");
                        match broadcast_payload {
                            BroadcastPayload::Message(m) => {
                                assert_eq!(m.user.as_ref(), "adam");
                                assert_eq!(m.message.as_ref(), "woohoo");
                            } 
                            BroadcastPayload::SignIn(_) => {
                                panic!("got a broadcast message payload out of order");
                            }
                        }
                    }
                    p => {
                        panic!("websocket received invalid payload: {}", p);
                    }
                },
                Err(e) => {
                    panic!("websocket stream error: {}", e.to_string());
                }
            }
        }

    }
}
