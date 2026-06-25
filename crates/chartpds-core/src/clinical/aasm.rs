//! AASM sleep-stage taxonomy.
//!
//! Canonical sleep-stage vocabulary. The numeric discriminant carries
//! a stable contract for queries: `0 == Wake`; any positive value is
//! some flavor of asleep. New stages added later are positive integers
//! and never break existing "any asleep" range queries.

/// Canonical AASM sleep stage.
///
/// Discriminants are stable: `Wake == 0`, all non-wake stages are positive.
/// `PartialOrd`/`Ord` follow the discriminant order, so callers can write
/// `stage > AasmSleepStage::Wake` to mean "any flavor of asleep."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
#[non_exhaustive]
pub enum AasmSleepStage {
    /// Awake.
    Wake = 0,
    /// Stage N1 (light sleep, transition from wake).
    N1 = 1,
    /// Stage N2 (light sleep, predominant in early-night cycles).
    N2 = 2,
    /// Stage N3 (slow-wave / deep sleep).
    N3 = 3,
    /// REM (rapid eye movement) sleep.
    Rem = 4,
}

impl AasmSleepStage {
    /// All stages, ascending by discriminant. Drives the minted-coding
    /// catalog so its value list cannot drift from the encoder.
    pub const ALL: [AasmSleepStage; 5] = [Self::Wake, Self::N1, Self::N2, Self::N3, Self::Rem];

    /// Stable lowercase token, identical to the stored `value_string`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Wake => "wake",
            Self::N1 => "n1",
            Self::N2 => "n2",
            Self::N3 => "n3",
            Self::Rem => "rem",
        }
    }

    /// Human-facing label for catalog/display use.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Wake => "awake",
            Self::N1 => "light sleep (N1, transition)",
            Self::N2 => "light sleep (N2)",
            Self::N3 => "deep / slow-wave sleep (N3)",
            Self::Rem => "REM",
        }
    }

    /// Numeric discriminant as stored in `value_quantity`.
    #[must_use]
    pub fn discriminant(&self) -> u8 {
        *self as u8
    }
}

impl std::fmt::Display for AasmSleepStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// FHIR `system` URI for the canonical AASM sleep-stage coding.
///
/// Query callers ask for AASM stages via this system; the query layer
/// translates to per-source native queries using each source's registered
/// AASM mapping.
///
/// Renamed from `https://vitals.fhwang.net/coding/aasm/sleep-stage` in the
/// `ChartPDS` rewrite; legacy data is re-tagged on re-ingestion (no
/// compatibility shim).
pub const AASM_SLEEP_STAGE_SYSTEM: &str = "https://chartpds.fhwang.net/coding/aasm/sleep-stage";

/// Short code for the canonical AASM sleep-stage coding.
pub const AASM_SLEEP_STAGE_CODE: &str = "aasm-sleep-stage";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wake_is_zero_other_stages_are_positive() {
        assert_eq!(AasmSleepStage::Wake as u8, 0);
        assert!(AasmSleepStage::N1 as u8 > 0);
        assert!(AasmSleepStage::N2 as u8 > 0);
        assert!(AasmSleepStage::N3 as u8 > 0);
        assert!(AasmSleepStage::Rem as u8 > 0);
    }

    #[test]
    fn stage_discriminants_match_vitals_taxonomy() {
        // Stable contract: existing query SQL depends on these values.
        assert_eq!(AasmSleepStage::Wake as u8, 0);
        assert_eq!(AasmSleepStage::N1 as u8, 1);
        assert_eq!(AasmSleepStage::N2 as u8, 2);
        assert_eq!(AasmSleepStage::N3 as u8, 3);
        assert_eq!(AasmSleepStage::Rem as u8, 4);
    }

    #[test]
    fn stages_are_copy_and_equal() {
        let a = AasmSleepStage::N2;
        let b = a; // Copy
        assert_eq!(a, b);
    }

    #[test]
    fn stages_are_strictly_increasing_from_wake() {
        // If a future variant gets added out of order, this catches it.
        assert!(AasmSleepStage::Wake < AasmSleepStage::N1);
        assert!(AasmSleepStage::N1 < AasmSleepStage::N2);
        assert!(AasmSleepStage::N2 < AasmSleepStage::N3);
        assert!(AasmSleepStage::N3 < AasmSleepStage::Rem);
    }

    #[test]
    fn display_emits_lowercase_short_form() {
        assert_eq!(format!("{}", AasmSleepStage::Wake), "wake");
        assert_eq!(format!("{}", AasmSleepStage::N1), "n1");
        assert_eq!(format!("{}", AasmSleepStage::Rem), "rem");
    }

    #[test]
    fn system_and_code_constants_are_chartpds_branded() {
        assert_eq!(
            AASM_SLEEP_STAGE_SYSTEM,
            "https://chartpds.fhwang.net/coding/aasm/sleep-stage"
        );
        assert_eq!(AASM_SLEEP_STAGE_CODE, "aasm-sleep-stage");
    }
}
