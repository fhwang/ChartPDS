//! Maps Oura's single-char sleep-stage codes to AASM stages.
//!
//! Oura's `sleep_phase_5_min` string uses one character per 5-minute
//! epoch: `'1'` = deep, `'2'` = light, `'3'` = REM, `'4'` = awake.
//! This module converts each character to the canonical
//! [`AasmSleepStage`] enum.

use crate::clinical::AasmSleepStage;

/// Duration of one Oura sleep epoch in seconds (5 minutes).
pub const EPOCH_SECONDS: i64 = 300;

/// Convert an Oura single-char stage code to an AASM sleep stage.
///
/// Returns `None` for unknown characters.
#[must_use]
pub fn oura_char_to_aasm(c: char) -> Option<AasmSleepStage> {
    match c {
        '1' => Some(AasmSleepStage::N3),   // deep
        '2' => Some(AasmSleepStage::N2),   // light
        '3' => Some(AasmSleepStage::Rem),  // REM
        '4' => Some(AasmSleepStage::Wake), // awake
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_maps_to_n3() {
        assert_eq!(oura_char_to_aasm('1'), Some(AasmSleepStage::N3));
    }

    #[test]
    fn light_maps_to_n2() {
        assert_eq!(oura_char_to_aasm('2'), Some(AasmSleepStage::N2));
    }

    #[test]
    fn rem_maps_to_rem() {
        assert_eq!(oura_char_to_aasm('3'), Some(AasmSleepStage::Rem));
    }

    #[test]
    fn awake_maps_to_wake() {
        assert_eq!(oura_char_to_aasm('4'), Some(AasmSleepStage::Wake));
    }

    #[test]
    fn unknown_char_returns_none() {
        assert_eq!(oura_char_to_aasm('0'), None);
        assert_eq!(oura_char_to_aasm('5'), None);
        assert_eq!(oura_char_to_aasm('x'), None);
    }

    #[test]
    fn epoch_seconds_is_five_minutes() {
        assert_eq!(EPOCH_SECONDS, 300);
    }
}
