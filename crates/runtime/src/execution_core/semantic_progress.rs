//! Bounded, content-free semantic progress from the execution-owned stream.
/// Accept strictly increasing events from the current nonzero bus generation.
/// The caller separately rejects terminal executions and stale generations.
pub(super) fn admit(
    last_generation: &mut u64,
    last_sequence: &mut u64,
    generation: u64,
    sequence: u64,
) -> bool {
    if generation == 0 || sequence == 0 || generation < *last_generation {
        return false;
    }
    if generation > *last_generation {
        *last_generation = generation;
        *last_sequence = 0;
    }
    if sequence <= *last_sequence {
        return false;
    }
    *last_sequence = sequence;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn progressing_long_stream_is_constant_space_and_replay_is_rejected() {
        let (mut generation, mut sequence) = (0, 0);
        for n in 1..=10_000 {
            assert!(admit(&mut generation, &mut sequence, 1, n));
        }
        assert!(!admit(&mut generation, &mut sequence, 1, 9_999));
        assert!(!admit(&mut generation, &mut sequence, 1, 10_000));
        assert!(!admit(&mut generation, &mut sequence, 0, 10_001));
        assert!(admit(&mut generation, &mut sequence, 2, 1));
        assert!(!admit(&mut generation, &mut sequence, 1, 10_001));
        assert!(!admit(&mut generation, &mut sequence, 2, 0));
    }
}
