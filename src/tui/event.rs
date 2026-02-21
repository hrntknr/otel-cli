use crossterm::event::{self, Event, KeyEvent};
use std::time::Duration;
use tokio::sync::broadcast;

use crate::store::StoreEvent;

pub enum AppEvent {
    Key(KeyEvent),
    StoreUpdate,
    Tick,
}

pub struct EventHandler {
    store_rx: broadcast::Receiver<StoreEvent>,
}

impl EventHandler {
    pub fn new(store_rx: broadcast::Receiver<StoreEvent>) -> Self {
        Self { store_rx }
    }

    pub async fn next(&mut self) -> AppEvent {
        loop {
            tokio::select! {
                result = self.store_rx.recv() => {
                    match result {
                        Ok(_) => return AppEvent::StoreUpdate,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => return AppEvent::Tick,
                    }
                }
                result = tokio::task::spawn_blocking(|| {
                    if event::poll(Duration::from_millis(250)).unwrap_or(false) {
                        if let Ok(Event::Key(key)) = event::read() {
                            return Some(key);
                        }
                    }
                    None
                }) => {
                    match result {
                        Ok(Some(key)) => return AppEvent::Key(key),
                        _ => return AppEvent::Tick,
                    }
                }
            }
        }
    }
}
