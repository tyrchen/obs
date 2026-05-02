//! `obs::SpanTrace` — capture the active scope/span ancestry for
//! attaching to error types.
//!
//! Spec 13 § 9.

use std::fmt;

use crate::scope::{ScopeField, ScopeFrame, with_frames_innermost_first};

/// One captured frame's name + fields, decoupled from the live
/// `ScopeFrame` so the `SpanTrace` can outlive the scope guard.
#[derive(Debug, Clone)]
struct CapturedFrame {
    name: Option<String>,
    target: Option<String>,
    fields: Vec<(String, String)>,
}

/// Captured `obs::scope!` ancestry. Like `tracing-error::SpanTrace`.
#[derive(Debug, Clone, Default)]
pub struct SpanTrace {
    frames: Vec<CapturedFrame>,
}

impl SpanTrace {
    /// Walk the active task's `obs::scope!` stack and capture frames
    /// outermost-first. Cheap: zero allocation when the stack is
    /// empty, linear in stack depth otherwise.
    #[must_use]
    pub fn capture() -> Self {
        let mut frames: Vec<CapturedFrame> =
            with_frames_innermost_first(|stack| stack.iter().map(capture_frame).collect());
        // `with_frames_innermost_first` returns the stack as-is
        // (outermost-first by index because Vec::push appends), so
        // emit Display in that natural order.
        frames.shrink_to_fit();
        Self { frames }
    }

    /// `true` when no scope was active at capture time.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Number of captured frames.
    #[must_use]
    pub fn len(&self) -> usize {
        self.frames.len()
    }
}

fn capture_frame(frame: &ScopeFrame) -> CapturedFrame {
    let span = frame.as_span_frame();
    CapturedFrame {
        name: span.map(|s| s.name.to_string()),
        target: span.map(|s| s.target.to_string()),
        fields: frame
            .fields()
            .iter()
            .map(|f| match f {
                ScopeField::TraceId(v) => ("trace_id".to_string(), v.clone()),
                ScopeField::SpanId(v) => ("span_id".to_string(), v.clone()),
                ScopeField::ParentSpanId(v) => ("parent_span_id".to_string(), v.clone()),
                ScopeField::Label(k, v) => ((*k).to_string(), v.clone()),
            })
            .collect(),
    }
}

impl fmt::Display for SpanTrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.frames.is_empty() {
            return f.write_str("(no obs scope active)");
        }
        // Print innermost first so error chain reads "the immediate
        // context, then its parent, …".
        for (i, frame) in self.frames.iter().enumerate().rev() {
            let depth = self.frames.len() - 1 - i;
            write!(
                f,
                "  {depth}: {}",
                frame.name.as_deref().unwrap_or("<scope>")
            )?;
            if let Some(t) = frame.target.as_deref() {
                write!(f, " @ {t}")?;
            }
            if !frame.fields.is_empty() {
                write!(f, " [")?;
                for (j, (k, v)) in frame.fields.iter().enumerate() {
                    if j > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}={v}")?;
                }
                write!(f, "]")?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}
