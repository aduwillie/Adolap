pub enum InterruptAction {
    Continue,
    Exit,
}

pub fn handle_interrupt(buffer: &mut String) -> InterruptAction {
    if buffer.is_empty() {
        println!("Exiting.");
        InterruptAction::Exit
    } else {
        buffer.clear();
        println!("Query buffer cleared.");
        InterruptAction::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::{handle_interrupt, InterruptAction};

    #[test]
    fn clears_non_empty_buffer() {
        let mut buffer = String::from("SELECT 1");
        let action = handle_interrupt(&mut buffer);
        assert!(matches!(action, InterruptAction::Continue));
        assert!(buffer.is_empty());
    }

    #[test]
    fn exits_on_empty_buffer() {
        let mut buffer = String::new();
        let action = handle_interrupt(&mut buffer);
        assert!(matches!(action, InterruptAction::Exit));
    }
}