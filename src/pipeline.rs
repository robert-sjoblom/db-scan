use std::marker::PhantomData;

use tokio::{
    sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

use crate::timings::Event;

/// Shared context available to all pipeline stages.
struct PipelineContext {
    timings_tx: UnboundedSender<Event>,
}

/// Typestate marker: pipeline has no source yet.
struct Empty;

/// Typestate marker: pipeline has a source producing items of type `T`.
struct HasSource<T>(PhantomData<T>);

/// A pipeline under construction.
///
/// The `State` type parameter tracks what stage the pipeline is in:
/// - `Empty`: no source added yet
/// - `HasSource<T>`: has a source producing `T`, ready for stages or sink
struct Pipeline<Ctx, State> {
    context: Ctx,
    handles: Vec<JoinHandle<()>>,
    _state: PhantomData<State>,
}

impl<Ctx> Pipeline<Ctx, Empty> {
    /// Create a new pipeline with the given context.
    pub fn new(context: Ctx) -> Self {
        Pipeline {
            context,
            handles: Vec::new(),
            _state: PhantomData,
        }
    }

    /// Add a source stage that produces items of type `Out`.
    ///
    /// The source receives a sender and should send items into the pipeline.
    /// This transitions the pipeline from `Empty` to `HasSource<Out>`.
    pub fn source<Out, F, Fut>(self, f: F) -> Pipeline<Ctx, HasSource<Out>>
    where
        F: FnOnce(&Ctx, UnboundedSender<Out>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
    }
}

impl<Ctx, In> Pipeline<Ctx, HasSource<In>> {
    /// Add a processing stage that transforms `In` items to `Out` items.
    ///
    /// The stage receives items from the previous stage and sends transformed
    /// items to the next stage. This transitions from `HasSource<In>` to `HasSource<Out>`.
    pub fn stage<Out, F, Fut>(self, f: F) -> Pipeline<Ctx, HasSource<Out>>
    where
        F: FnOnce(&Ctx, UnboundedReceiver<In>, UnboundedSender<Out>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        todo!()
    }

    /// Add a terminal sink that consumes items and produces a final result.
    ///
    /// The sink receives items from the previous stage and returns a value
    /// when the pipeline completes. Returns a `RunnablePipeline` that can be executed.
    pub fn sink<R, F, Fut>(self, f: F) -> RunnablePipeline<R>
    where
        F: FnOnce(&Ctx, UnboundedReceiver<In>) -> Fut,
        Fut: Future<Output = R> + Send + 'static,
    {
        todo!()
    }
}

/// A fully constructed pipeline ready to execute.
///
/// Created by calling `sink()` on a pipeline. Call `run()` to execute
/// all stages concurrently and get the sink's result.
struct RunnablePipeline<R> {
    handles: Vec<JoinHandle<()>>,
    sink_handle: JoinHandle<R>,
}

impl<R> RunnablePipeline<R> {
    /// Execute the pipeline, waiting for all stages to complete.
    ///
    /// Returns the result produced by the sink.
    pub async fn run(self) -> R {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::unbounded_channel;

    use crate::{
        timings::Event,
        v2::{
            analyze::AnalyzedCluster,
            cluster::{Cluster, cluster_builder},
            scan::{AnalyzedNode, scan_nodes},
        },
    };

    use super::*;

    #[tokio::test]
    async fn pipeline_flows_data() {
        let (timings_tx, _) = unbounded_channel::<Event>();
        let ctx = PipelineContext { timings_tx };

        let result = Pipeline::new(ctx)
            .source(|_ctx, tx| async move {
                tx.send("hello").ok();
                tx.send("world").ok();
            })
            .stage(|_ctx, mut rx, tx| async move {
                while let Some(s) = rx.recv().await {
                    tx.send(s.len()).ok();
                }
            })
            .sink(|_ctx, mut rx| async move {
                let mut results = vec![];
                while let Some(n) = rx.recv().await {
                    results.push(n);
                }
                results
            })
            .run()
            .await;

        assert_eq!(result, vec![5, 5]);
    }
}
