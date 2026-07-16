// ── Animation Engine — Frame-based transitions & effects ─────────
// Provides frame-counted animation state for:
//   - Sidebar slide (width transition over 8 frames)
//   - Search highlight pulse (bright→dim over 4 frames)
//   - Dialog fade-in (opacity 0→1 over 4 frames)
//   - Spinner smooth rotation (unicode braille spinner)
//
// All animations are frame-counter based — no wall-clock timing needed
// because the render loop runs at a consistent tick rate.
// -------------------------------------------------------------------

#![allow(dead_code)]

/// Types of animations the engine can drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnimationKind {
    /// Sidebar width transition (in-progress).
    SidebarSlide,
    /// Search highlight pulse effect.
    SearchPulse,
    /// Dialog fade-in transition.
    DialogFade,
    /// Spinner rotation (always active when turn is).
    Spinner,
}

/// Current state of a single animation.
#[derive(Debug, Clone, Copy)]
pub struct AnimationState {
    /// What kind of animation this is.
    pub kind: AnimationKind,
    /// Current frame number (0-based, increments each tick).
    pub frame: u32,
    /// Total frames for this animation (completion at frame >= total_frames).
    pub total_frames: u32,
    /// Whether the animation repeats (loops) after completion.
    pub repeating: bool,
    /// Current progress [0.0, 1.0].
    pub progress: f32,
}

impl AnimationState {
    /// Create a new one-shot animation with the given total frames.
    pub fn one_shot(kind: AnimationKind, total_frames: u32) -> Self {
        Self {
            kind,
            frame: 0,
            total_frames,
            repeating: false,
            progress: 0.0,
        }
    }

    /// Create a new repeating animation.
    pub fn repeating(kind: AnimationKind, total_frames: u32) -> Self {
        Self {
            kind,
            frame: 0,
            total_frames,
            repeating: true,
            progress: 0.0,
        }
    }

    /// Advance one frame. Returns true if the animation is still active.
    pub fn tick(&mut self) -> bool {
        self.frame += 1;
        if self.repeating {
            self.frame %= self.total_frames.max(1);
            self.progress = self.frame as f32 / self.total_frames.max(1) as f32;
            true
        } else if self.frame >= self.total_frames {
            self.progress = 1.0;
            false // animation complete
        } else {
            self.progress = self.frame as f32 / self.total_frames.max(1) as f32;
            true
        }
    }

    /// Whether this animation is still in-progress.
    pub fn is_active(&self) -> bool {
        if self.repeating {
            true
        } else {
            self.frame < self.total_frames
        }
    }

    /// Reset the animation to frame 0.
    pub fn reset(&mut self) {
        self.frame = 0;
        self.progress = 0.0;
    }
}

// ── AnimationEngine ──────────────────────────────────────────────

/// Manages multiple concurrent animations with frame-based progress.
///
/// The engine is ticked once per render loop iteration. Animations
/// that complete (one-shot) are automatically removed on the next tick.
pub struct AnimationEngine {
    /// Active animations indexed by kind.
    states: Vec<AnimationState>,
}

impl AnimationEngine {
    /// Create an empty animation engine.
    pub fn new() -> Self {
        Self { states: Vec::new() }
    }

    /// Start (or restart) an animation of the given kind.
    ///
    /// If an animation of the same kind is already active, it is replaced
    /// (useful for re-triggering search pulse on each n/N press).
    pub fn start(&mut self, state: AnimationState) {
        // Replace existing animation of same kind
        self.states.retain(|s| s.kind != state.kind);
        self.states.push(state);
    }

    /// Start a one-shot animation. Convenience wrapper.
    pub fn start_one_shot(&mut self, kind: AnimationKind, total_frames: u32) {
        self.start(AnimationState::one_shot(kind, total_frames));
    }

    /// Start a repeating animation. Convenience wrapper.
    pub fn start_repeating(&mut self, kind: AnimationKind, total_frames: u32) {
        self.start(AnimationState::repeating(kind, total_frames));
    }

    /// Advance all active animations by one frame.
    /// Completed one-shot animations are removed.
    pub fn tick(&mut self) {
        for state in &mut self.states {
            state.tick();
        }
        // Remove completed one-shot animations
        self.states.retain(|s| s.is_active());
    }

    /// Get the current state for an animation kind, if active.
    pub fn get(&self, kind: AnimationKind) -> Option<&AnimationState> {
        self.states.iter().find(|s| s.kind == kind)
    }

    /// Ease-out cubic: slows down near the end. Useful for sidebar slide.
    /// Maps progress [0..1] → eased [0..1].
    pub fn ease_out_cubic(t: f32) -> f32 {
        let t = t.clamp(0.0, 1.0);
        1.0 - (1.0 - t).powi(3)
    }

    /// Ease-in-out quad: smooth at both ends. Useful for fade effects.
    pub fn ease_in_out_quad(t: f32) -> f32 {
        let t = t.clamp(0.0, 1.0);
        if t < 0.5 {
            2.0 * t * t
        } else {
            1.0 - (-2.0 * t + 2.0).powi(2) / 2.0
        }
    }

    /// Pulse brightness: bright→dim over N frames for search highlight.
    /// Returns a multiplier [0.3, 1.0] where 1.0 = brightest.
    pub fn pulse_brightness(progress: f32) -> f32 {
        // Oscillate: bright at start, dim at middle, bright at end
        // This creates a "pulse" effect
        let t = progress.clamp(0.0, 1.0);
        let wave = (t * std::f32::consts::PI * 2.0).sin().abs();
        0.3 + wave * 0.7
    }

    /// Fade opacity: 0→1 over N frames for dialog entrance.
    /// Returns a multiplier [0.0, 1.0].
    pub fn fade_opacity(progress: f32) -> f32 {
        Self::ease_in_out_quad(progress.clamp(0.0, 1.0))
    }

    /// Get the unicode braille spinner character at the given frame index.
    pub fn spinner_char(frame: u32) -> &'static str {
        const SPINNER: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        SPINNER[(frame as usize) % SPINNER.len()]
    }

    /// Whether any animation is currently active.
    pub fn any_active(&self) -> bool {
        self.states.iter().any(|s| s.is_active())
    }
}

impl Default for AnimationEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_shot_completes() {
        let mut state = AnimationState::one_shot(AnimationKind::DialogFade, 4);
        assert!(state.is_active());
        assert!((state.progress - 0.0).abs() < 0.001);

        state.tick(); // frame 1
        assert!(state.is_active());
        assert!((state.progress - 0.25).abs() < 0.001);

        state.tick(); // frame 2
        state.tick(); // frame 3
        let active = state.tick(); // frame 4 → complete
        assert!(!active);
        assert!(!state.is_active());
        assert!((state.progress - 1.0).abs() < 0.001);
    }

    #[test]
    fn repeating_loops() {
        let mut state = AnimationState::repeating(AnimationKind::Spinner, 4);
        for _ in 0..10 {
            assert!(state.tick());
        }
        assert!(state.is_active());
        // After 4 ticks, frame wraps to 0
        state.reset();
        assert_eq!(state.frame, 0);
        assert!(state.is_active());
    }

    #[test]
    fn engine_replaces_same_kind() {
        let mut engine = AnimationEngine::new();
        engine.start_one_shot(AnimationKind::SearchPulse, 4);
        assert!(engine.get(AnimationKind::SearchPulse).is_some());

        // Start a new one with different frame count
        engine.start_one_shot(AnimationKind::SearchPulse, 8);
        let state = engine.get(AnimationKind::SearchPulse).unwrap();
        assert_eq!(state.total_frames, 8);
        assert_eq!(state.frame, 0);
    }

    #[test]
    fn engine_tick_removes_completed() {
        let mut engine = AnimationEngine::new();
        engine.start_one_shot(AnimationKind::DialogFade, 2);
        engine.tick(); // frame 1
        engine.tick(); // frame 2 → complete
        engine.tick(); // should remove
        assert!(engine.get(AnimationKind::DialogFade).is_none());
    }

    #[test]
    fn ease_out_cubic_endpoints() {
        assert!((AnimationEngine::ease_out_cubic(0.0) - 0.0).abs() < 0.001);
        assert!((AnimationEngine::ease_out_cubic(1.0) - 1.0).abs() < 0.001);
    }

    #[test]
    fn ease_in_out_quad_endpoints() {
        assert!((AnimationEngine::ease_in_out_quad(0.0) - 0.0).abs() < 0.001);
        assert!((AnimationEngine::ease_in_out_quad(1.0) - 1.0).abs() < 0.001);
    }

    #[test]
    fn pulse_brightness_range() {
        for i in 0..=100 {
            let t = i as f32 / 100.0;
            let b = AnimationEngine::pulse_brightness(t);
            assert!((0.3..=1.0).contains(&b), "brightness {b} out of range at t={t}");
        }
    }

    #[test]
    fn fade_opacity_monotonic() {
        let mut prev = 0.0;
        for i in 0..=100 {
            let t = i as f32 / 100.0;
            let o = AnimationEngine::fade_opacity(t);
            assert!(o >= prev, "fade should be monotonic: {o} < {prev} at t={t}");
            prev = o;
        }
    }

    #[test]
    fn spinner_char_rotates() {
        let c0 = AnimationEngine::spinner_char(0);
        let c1 = AnimationEngine::spinner_char(1);
        assert_ne!(c0, c1, "spinner should change each frame");
        // Full rotation returns to same character
        assert_eq!(
            AnimationEngine::spinner_char(0),
            AnimationEngine::spinner_char(10)
        );
    }

    #[test]
    fn any_active_empty() {
        let engine = AnimationEngine::new();
        assert!(!engine.any_active());
    }

    #[test]
    fn any_active_with_animation() {
        let mut engine = AnimationEngine::new();
        engine.start_one_shot(AnimationKind::SidebarSlide, 8);
        assert!(engine.any_active());
    }
}
