use crossterm::event::{self, Event, KeyEvent, MouseEvent};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

use crate::store::StoreEvent;

pub enum AppEvent {
    Key(KeyEvent),
    Mouse(MouseEvent),
    StoreUpdate,
    Tick,
}

enum TermEvent {
    Key(KeyEvent),
    Mouse(MouseEvent),
}

pub struct EventHandler {
    store_rx: broadcast::Receiver<StoreEvent>,
    term_rx: mpsc::UnboundedReceiver<TermEvent>,
}

impl EventHandler {
    pub fn new(store_rx: broadcast::Receiver<StoreEvent>) -> Self {
        let (term_tx, term_rx) = mpsc::unbounded_channel();
        std::thread::spawn(move || loop {
            if event::poll(Duration::from_millis(250)).unwrap_or(false) {
                match event::read() {
                    Ok(Event::Key(key)) => {
                        if term_tx.send(TermEvent::Key(key)).is_err() {
                            break;
                        }
                    }
                    Ok(Event::Mouse(mouse)) => {
                        if term_tx.send(TermEvent::Mouse(mouse)).is_err() {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        });
        Self { store_rx, term_rx }
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
                result = self.term_rx.recv() => {
                    match result {
                        Some(TermEvent::Key(key)) => return AppEvent::Key(key),
                        Some(TermEvent::Mouse(mouse)) => return AppEvent::Mouse(mouse),
                        None => return AppEvent::Tick,
                    }
                }
            }
        }
    }
}
