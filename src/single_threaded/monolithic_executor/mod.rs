use crate::metric::{EXECUTION_LATENCY_TIME_ID, EXECUTION_TIME_TAKEN_ID, OPERATIONS_EXECUTED_PER_SECOND_ID, UNORDERED_OPS_PER_SECOND_ID};
use crate::ExecutorReplier;
use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::error::*;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_metrics::metrics::{metric_duration, metric_increment};
use atlas_smr_application::app::{Application, BatchReplies, Reply, Request, UnorderedBatch, UpdateBatch};
use atlas_smr_application::state::monolithic_state::{
    AppStateMessage, InstallStateMessage, MonolithicState,
};
use atlas_smr_application::{ExecutionRequest, ExecutorHandle};
use atlas_smr_core::exec::ReplyNode;
use atlas_smr_core::SMRReply;
use log::info;
use std::sync::Arc;
use std::time::Instant;
use rayon::{ThreadPool, ThreadPoolBuilder};
use crate::scalable::scalable_unordered_execution;
use crate::single_threaded::UnorderedExecutor;

const EXECUTING_BUFFER: usize = 16384;
const STATE_BUFFER: usize = 128;

pub struct MonolithicExecutor<S, A, NT>
    where
        S: MonolithicState + 'static,
        A: Application<S> + 'static,
{
    application: A,
    state: S,
    t_pool: ThreadPool,

    work_rx: ChannelSyncRx<ExecutionRequest<Request<A, S>>>,
    state_rx: ChannelSyncRx<InstallStateMessage<S>>,
    checkpoint_tx: ChannelSyncTx<AppStateMessage<S>>,

    send_node: Arc<NT>,
}


impl<S, A, NT> MonolithicExecutor<S, A, NT>
    where
        S: MonolithicState + 'static,
        A: Application<S> + 'static + Send,
        NT: 'static,
{
    pub fn init_handle() -> (
        ExecutorHandle<Request<A, S>>,
        ChannelSyncRx<ExecutionRequest<Request<A, S>>>,
    ) {
        let (tx, rx) = channel::new_bounded_sync(
            EXECUTING_BUFFER,
            Some("ST Monolithic Executor Work Channel"),
        );

        (ExecutorHandle::new(tx), rx)
    }

    pub fn init<T>(
        handle: ChannelSyncRx<ExecutionRequest<Request<A, S>>>,
        initial_state: Option<(S, Vec<Request<A, S>>)>,
        service: A,
        send_node: Arc<NT>,
    ) -> Result<(
        ChannelSyncTx<InstallStateMessage<S>>,
        ChannelSyncRx<AppStateMessage<S>>,
    )>
        where
            T: ExecutorReplier + 'static,
            NT: ReplyNode<SMRReply<A::AppData>>,
    {
        let (state, requests) = if let Some(state) = initial_state {
            state
        } else {
            (A::initial_state()?, vec![])
        };

        let (state_tx, state_rx) =
            channel::new_bounded_sync(STATE_BUFFER, Some("ST Monolithic Executor Work InstState"));

        let (checkpoint_tx, checkpoint_rx) =
            channel::new_bounded_sync(STATE_BUFFER, Some("ST Monolithic Executor AppState"));

        let mut executor = MonolithicExecutor {
            application: service,
            state,
            t_pool: ThreadPoolBuilder::new().num_threads(4).build()?,
            work_rx: handle,
            state_rx,
            checkpoint_tx,
            send_node,
        };

        for request in requests {
            executor.application.update(&mut executor.state, request);
        }

        std::thread::Builder::new()
            .name("Executor thread".to_string())
            .spawn(move || executor.worker::<T>())
            .expect("Failed to start executor thread");

        Ok((state_tx, checkpoint_rx))
    }

    fn worker<T>(&mut self)
        where
            T: ExecutorReplier + 'static,
            NT: ReplyNode<SMRReply<A::AppData>>,
    {
        while let Ok(exec_req) = self.work_rx.recv() {
            match exec_req {
                ExecutionRequest::PollStateChannel => {
                    if let Ok(state_recvd) = self.state_rx.recv() {
                        self.state = state_recvd.into_state();
                    }
                }
                ExecutionRequest::CatchUp(requests) => {
                    info!("Catching up with {} batches of requests", requests.len());

                    for batch in requests {
                        let (seq_no, reply_batch) = self.execute_op_batch(batch);

                        self.execution_finished::<T>(Some(seq_no), reply_batch);
                    }
                }
                ExecutionRequest::Update((batch, instant)) => {
                    metric_duration(EXECUTION_LATENCY_TIME_ID, instant.elapsed());

                    let (seq_no, reply_batch) = self.execute_op_batch(batch);

                    // deliver replies
                    self.execution_finished::<T>(Some(seq_no), reply_batch);
                }
                ExecutionRequest::UpdateAndGetAppstate((batch, instant)) => {
                    metric_duration(EXECUTION_LATENCY_TIME_ID, instant.elapsed());

                    let (seq_no, reply_batch) = self.execute_op_batch(batch);

                    // deliver checkpoint state to the replica
                    self.deliver_checkpoint_state(seq_no);

                    // deliver replies
                    self.execution_finished::<T>(Some(seq_no), reply_batch);
                }
                ExecutionRequest::Read(_peer_id) => {
                    todo!()
                }
                ExecutionRequest::ExecuteUnordered(batch) => {
                    let reply_batch = self.execute_unordered(batch);

                    self.execution_finished::<T>(None, reply_batch);
                }
            }
        }
    }

    fn execute_op_batch(&mut self, batch: UpdateBatch<Request<A, S>>) -> (SeqNo, BatchReplies<Reply<A, S>>) {
        let seq_no = batch.sequence_number();
        let operations = batch.len() as u64;


        let start = Instant::now();

        let reply_batch = self
            .application
            .update_batch(&mut self.state, batch);

        metric_duration(EXECUTION_TIME_TAKEN_ID, start.elapsed());
        metric_increment(OPERATIONS_EXECUTED_PER_SECOND_ID, Some(operations));

        (seq_no, reply_batch)
    }

    ///Clones the current state and delivers it to the application
    /// Takes a sequence number, which corresponds to the last executed consensus instance before we performed the checkpoint
    fn deliver_checkpoint_state(&self, seq: SeqNo) {
        let cloned_state = self.state.clone();

        self.checkpoint_tx
            .send_return(AppStateMessage::new(seq, cloned_state))
            .expect("Failed to send checkpoint");
    }

    fn execution_finished<T>(&self, seq: Option<SeqNo>, batch: BatchReplies<Reply<A, S>>)
        where
            NT: ReplyNode<SMRReply<A::AppData>> + 'static,
            T: ExecutorReplier + 'static,
    {
        let send_node = self.send_node.clone();

        T::execution_finished::<A::AppData, NT>(send_node, seq, batch);
    }
}

impl<S, A, NT> UnorderedExecutor<A, S> for MonolithicExecutor<S, A, NT>
    where
        S: MonolithicState + 'static,
        A: Application<S> + 'static + Send,
        NT: 'static, {
    default fn execute_unordered(&mut self, batch: UnorderedBatch<Request<A, S>>) -> BatchReplies<Reply<A, S>> where A: Application<S> {
        let operations = batch.len() as u64;

        let reply_batch = self
            .application
            .unordered_batched_execution(&self.state, batch);

        metric_increment(UNORDERED_OPS_PER_SECOND_ID, Some(operations));

        reply_batch
    }
}

impl<S, A, NT> UnorderedExecutor<A, S> for MonolithicExecutor<S, A, NT>
    where
        S: MonolithicState + Sync + 'static,
        A: Application<S> + 'static + Send,
        NT: 'static, {
    fn execute_unordered(&mut self, batch: UnorderedBatch<Request<A, S>>) -> BatchReplies<Reply<A, S>> where A: Application<S> {
        let operations = batch.len() as u64;

        let reply_batch = scalable_unordered_execution(
            &mut self.t_pool,
            &self.application,
            &self.state,
            batch,
        );

        metric_increment(UNORDERED_OPS_PER_SECOND_ID, Some(operations));

        reply_batch
    }
}