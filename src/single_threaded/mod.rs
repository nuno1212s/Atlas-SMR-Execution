use atlas_smr_application::app::{Application, BatchReplies, Reply, Request, UnorderedBatch};

pub mod divisible_state_exec;
pub mod monolithic_executor;

trait UnorderedExecutor<A, S> {
    fn execute_unordered(&mut self, batch: UnorderedBatch<Request<A, S>>) -> BatchReplies<Reply<A, S>> where A: Application<S>;
}