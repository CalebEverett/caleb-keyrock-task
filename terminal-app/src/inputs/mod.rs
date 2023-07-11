use key::Key;
use orderbook_agg::book_summary::Summary;

pub mod events;
pub mod key;

pub enum InputEvent {
    /// An input event occurred.
    Input(Key),
    /// An tick event occurred.
    Tick,
    Update(Summary),
}
