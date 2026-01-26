//! Type-safe pipeline for async stage composition.
//!
//! Uses the typestate pattern to enforce valid pipeline construction at compile time:
//! - Pipelines must start with a `source`
//! - Stages can only be added after a source
//! - The output type of each stage must match the input type of the next
//! - Pipelines must end with a `sink` to be runnable
//!
//! # Example
//!
//! ```ignore
//! let result = Pipeline::new(ctx)
//!     .source(|ctx, tx| async move {
//!         tx.send("hello").ok();
//!         tx.send("world").ok();
//!     })
//!     .stage(|ctx, rx, tx| async move {
//!         while let Some(s) = rx.recv().await {
//!             tx.send(s.len()).ok();
//!         }
//!     })
//!     .sink(|ctx, rx| async move {
//!         let mut results = vec![];
//!         while let Some(n) = rx.recv().await {
//!             results.push(n);
//!         }
//!         results
//!     })
//!     .run()
//!     .await;
//! ```

use std::{collections::HashMap, sync::Arc};

use error_stack::{FutureExt, Report, ResultExt};
use futures::future::join_all;
use tokio::{
    spawn,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

use crate::{
    prometheus::FileSystemMetrics,
    timings::{Event, Stage},
    v2::writer::WriterOptions,
};

/// Error type for task failures within a [`Pipeline`].
///
/// Captures which pipeline stage failed for better error context.
#[derive(Debug)]
pub struct PipelineError {
    /// The pipeline stage that failed.
    pub stage: Stage,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task '{}' failed", self.stage)
    }
}

impl core::error::Error for PipelineError {}

pub trait Timings {
    fn timings(&self) -> &UnboundedSender<Event>;
}

/// Shared context available to all pipeline stages.
///
/// Provides access to shared resources (like timing event channels) that
/// pipeline stages may need during execution.
pub struct PipelineContext {
    /// Channel sender for emitting timing events.
    pub timings_tx: UnboundedSender<Event>,
    /// Pre-loaded filesystem metrics keyed by cluster name.
    pub batch_data: HashMap<String, FileSystemMetrics>,
    /// Configuration options for the output writer.
    pub writer_options: WriterOptions,
}

impl PipelineContext {
    /// Creates a new pipeline context with the given timing event sender.
    ///
    /// # Arguments
    ///
    /// * `timings_tx` - Channel sender for emitting [`Event`]s to track stage timings.
    pub fn new(
        timings_tx: UnboundedSender<Event>,
        batch_data: HashMap<String, FileSystemMetrics>,
        writer_options: WriterOptions,
    ) -> Self {
        Self {
            timings_tx,
            batch_data,
            writer_options,
        }
    }
}

impl Timings for PipelineContext {
    fn timings(&self) -> &UnboundedSender<Event> {
        &self.timings_tx
    }
}

/// Typestate marker: pipeline has no source yet.
pub struct Empty;

/// Typestate marker: pipeline has a source producing items of type `T`.
pub struct HasSource<T> {
    receiver: UnboundedReceiver<T>,
}

/// A pipeline under construction.
///
/// The `State` type parameter tracks what stage the pipeline is in:
/// - `Empty`: no source added yet
/// - `HasSource<T>`: has a source producing `T`, ready for stages or sink
pub struct Pipeline<Ctx, State> {
    context: Arc<Ctx>,
    handles: Vec<(Stage, JoinHandle<()>)>,
    state: State,
}

impl<Ctx: Timings> Pipeline<Ctx, Empty> {
    /// Create a new pipeline with the given context.
    pub fn new(context: Ctx) -> Self {
        Pipeline {
            context: context.into(),
            handles: Vec::new(),
            state: Empty,
        }
    }

    /// Add a source stage that produces items of type `Out`.
    ///
    /// The source receives a sender and should send items into the pipeline.
    /// This transitions the pipeline from `Empty` to `HasSource<Out>`.
    pub fn source<Out, F, Fut>(self, stage: Stage, f: F) -> Pipeline<Ctx, HasSource<Out>>
    where
        Ctx: Send + Sync + 'static,
        Out: Send + 'static,
        F: FnOnce(Arc<Ctx>, UnboundedSender<Out>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let ctx = self.context.clone();
        let fut = async move {
            ctx.timings().send(Event::Start(stage)).ok();
            let ctx_for_f = ctx.clone();
            f(ctx_for_f, tx).await;
            ctx.timings().send(Event::End(stage)).ok();
        };

        let mut handles = self.handles;
        handles.push((stage, spawn(fut)));
        Pipeline {
            context: self.context,
            handles,
            state: HasSource { receiver: rx },
        }
    }
}

impl<Ctx: Timings, In> Pipeline<Ctx, HasSource<In>> {
    /// Add a processing stage that transforms `In` items to `Out` items.
    ///
    /// The stage receives items from the previous stage and sends transformed
    /// items to the next stage. This transitions from `HasSource<In>` to `HasSource<Out>`.
    pub fn stage<Out, F, Fut>(self, stage: Stage, f: F) -> Pipeline<Ctx, HasSource<Out>>
    where
        Ctx: Send + Sync + 'static,
        Out: Send + 'static,
        In: Send + 'static,
        F: FnOnce(Arc<Ctx>, UnboundedReceiver<In>, UnboundedSender<Out>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let ctx = self.context.clone();
        let fut = async move {
            ctx.timings().send(Event::Start(stage)).ok();
            let ctx_for_f = ctx.clone();
            f(ctx_for_f, self.state.receiver, tx).await;
            ctx.timings().send(Event::End(stage)).ok();
        };

        let mut handles = self.handles;
        handles.push((stage, spawn(fut)));
        Pipeline {
            context: self.context,
            handles,
            state: HasSource { receiver: rx },
        }
    }

    /// Add a terminal sink that consumes items and produces a final result.
    ///
    /// The sink receives items from the previous stage and returns a value
    /// when the pipeline completes. Returns a `RunnablePipeline` that can be executed.
    pub fn sink<R, F, Fut>(self, stage: Stage, f: F) -> RunnablePipeline<R>
    where
        Ctx: Send + Sync + 'static,
        In: Send + 'static,
        R: Send + 'static,
        F: FnOnce(Arc<Ctx>, UnboundedReceiver<In>) -> Fut + Send + 'static,
        Fut: Future<Output = R> + Send + 'static,
    {
        let ctx = self.context.clone();
        let fut = async move {
            ctx.timings().send(Event::Start(stage)).ok();
            let ctx_for_f = ctx.clone();
            let result = f(ctx_for_f, self.state.receiver).await;
            ctx.timings().send(Event::End(stage)).ok();
            result
        };

        let handle = spawn(fut);
        RunnablePipeline {
            handles: self.handles,
            sink_handle: (stage, handle),
        }
    }
}

/// A fully constructed pipeline ready to execute.
///
/// Created by calling `sink()` on a pipeline. Call `run()` to execute
/// all stages concurrently and get the sink's result.
pub struct RunnablePipeline<R> {
    handles: Vec<(Stage, JoinHandle<()>)>,
    sink_handle: (Stage, JoinHandle<R>),
}

impl<R> RunnablePipeline<R> {
    /// Execute the pipeline, waiting for all stages to complete.
    ///
    /// Returns the result produced by the sink.
    pub async fn run(self) -> Result<R, Report<PipelineError>> {
        let void_futures = self.handles.into_iter().map(|(stage, handle)| async move {
            handle.await.change_context(PipelineError { stage })?;
            Ok::<_, Report<PipelineError>>(())
        });

        let (void_results, result) = futures::future::join(
            join_all(void_futures),
            self.sink_handle.1.change_context(PipelineError {
                stage: self.sink_handle.0,
            }),
        )
        .await;
        for result in void_results {
            result?;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::unbounded_channel;

    use crate::timings::Event;

    use super::*;

    #[tokio::test]
    async fn pipeline_flows_data() {
        let (timings_tx, _) = unbounded_channel::<Event>();
        let ctx = PipelineContext {
            timings_tx,
            batch_data: HashMap::new(),
            writer_options: WriterOptions::default(),
        };

        let result = Pipeline::new(ctx)
            .source(Stage::DatabasePortal, |_ctx, tx| async move {
                tx.send("hello").ok();
                tx.send("world").ok();
            })
            .stage(Stage::Scan, |_ctx, mut rx, tx| async move {
                while let Some(s) = rx.recv().await {
                    tx.send(s.len()).ok();
                }
            })
            .sink(Stage::Analyze, |_ctx, mut rx| async move {
                let mut results = vec![];
                while let Some(n) = rx.recv().await {
                    results.push(n);
                }
                results
            })
            .run()
            .await
            .expect("pipeline should complete successfully");

        assert_eq!(result, vec![5, 5]);
    }
}
