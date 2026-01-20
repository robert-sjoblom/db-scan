use std::marker::PhantomData;

use tokio::{
    sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

use crate::timings::Event;

struct PipelineContext {
    timings_tx: UnboundedSender<Event>,
}

struct Empty;
struct HasSource<T>(PhantomData<T>);

struct Pipeline<Ctx, State> {
    context: Ctx,
    handles: Vec<JoinHandle<()>>,
    _state: PhantomData<State>,
}

impl<Ctx> Pipeline<Ctx, Empty> {
    pub fn new(context: Ctx) -> Self {
        Pipeline {
            context,
            handles: Vec::new(),
            _state: PhantomData,
        }
    }

    pub fn source<Out, F, Fut>(self, f: F) -> Pipeline<Ctx, HasSource<Out>>
    where
        F: FnOnce(&Ctx, UnboundedSender<Out>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        todo!()
    }
}

impl<Ctx, In> Pipeline<Ctx, HasSource<In>> {
    pub fn stage<Out, F, Fut>(self, f: F) -> Pipeline<Ctx, HasSource<Out>>
    where
        F: FnOnce(&Ctx, UnboundedReceiver<In>, UnboundedSender<Out>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        todo!()
    }

    pub fn sink<R, F, Fut>(self, f: F) -> RunnablePipeline<R>
    where
        F: FnOnce(&Ctx, UnboundedReceiver<In>) -> Fut,
        Fut: Future<Output = R> + Send + 'static,
    {
        todo!()
    }
}

struct RunnablePipeline<R> {
    handles: Vec<JoinHandle<()>>,
    sink_handle: JoinHandle<R>,
}

impl<R> RunnablePipeline<R> {
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
    }
}
