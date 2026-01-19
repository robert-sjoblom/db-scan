//! Task group orchestration for concurrent pipeline execution.
//!
//! This module provides [`TaskGroup`], a utility for spawning and managing multiple
//! concurrent async tasks with typed return values and unified error handling.
//!
//! # Overview
//!
//! The task group supports two types of tasks:
//! - **Void tasks**: Fire-and-forget tasks that complete without returning a value
//! - **Returning tasks**: Tasks that produce a result of type `T`
//!
//! All tasks are associated with a [`Stage`] identifier for error reporting and tracing.
//!
//! # Example
//!
//! ```no_run
//! use db_scan::task_group::TaskGroup;
//! use db_scan::timings::Stage;
//!
//! # async fn example() {
//! let mut group = TaskGroup::new();
//!
//! // Spawn a void task
//! group.spawn(Stage::Scan, async {
//!     // perform work...
//! });
//!
//! // Spawn a returning task
//! group.spawn_returning(Stage::Write, async {
//!     "output".to_string()
//! });
//!
//! // Run all tasks concurrently
//! let results = group.run().await.expect("tasks failed");
//!
//! for (stage, output) in results {
//!     println!("{}: {}", stage, output);
//! }
//! # }
//! ```

use std::future::Future;

use error_stack::{Report, ResultExt};
use futures::future::join_all;
use tokio::task::JoinHandle;

use crate::timings::Stage;

/// Error type for task failures within a [`TaskGroup`].
///
/// Captures which pipeline stage failed for better error context.
#[derive(Debug)]
pub struct TaskError {
    pub stage: Stage,
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task '{}' failed", self.stage)
    }
}

impl core::error::Error for TaskError {}

/// A collection of async tasks that run concurrently and can return typed results.
///
/// `TaskGroup<T>` manages multiple tokio tasks, tracking them by their pipeline stage.
/// Tasks can either complete without returning a value (void tasks) or produce a result
/// of type `T` (returning tasks). All tasks are executed concurrently, and errors from
/// any task will cause the entire group to fail.
///
/// # Type Parameter
///
/// - `T`: The return type for all returning tasks in this group.
///
/// # Error Handling
///
/// - Task panics are caught and converted to [`TaskError`]
/// - Any task failure stops the entire group
/// - Errors include the stage identifier for debugging
#[derive(Debug)]
pub struct TaskGroup<T> {
    handles: Vec<(Stage, JoinHandle<()>)>,
    returning_handles: Vec<(Stage, JoinHandle<T>)>,
}

impl<T> TaskGroup<T> {
    /// Creates a new empty task group.
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
            returning_handles: Vec::new(),
        }
    }

    /// Spawns a void task that completes without returning a value.
    ///
    /// The task is associated with a pipeline stage for error reporting.
    ///
    /// # Arguments
    ///
    /// - `stage`: The pipeline stage this task represents
    /// - `future`: The async task to execute
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use db_scan::task_group::TaskGroup;
    /// # use db_scan::timings::Stage;
    /// # async fn example() {
    /// let mut group = TaskGroup::<String>::new();
    /// group.spawn(Stage::Clustering, async {
    ///     // perform clustering work...
    /// });
    /// # }
    /// ```
    pub fn spawn<F>(&mut self, stage: Stage, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(future);
        self.handles.push((stage, handle))
    }

    /// Spawns a task that returns a value of type `T`.
    ///
    /// The returned value will be collected when [`run()`](Self::run) completes.
    ///
    /// # Arguments
    ///
    /// - `stage`: The pipeline stage this task represents
    /// - `future`: The async task to execute, must return type `T`
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use db_scan::task_group::TaskGroup;
    /// # use db_scan::timings::Stage;
    /// # async fn example() {
    /// let mut group = TaskGroup::new();
    /// group.spawn_returning(Stage::Write, async {
    ///     "output".to_string()
    /// });
    ///
    /// let results = group.run().await.unwrap();
    /// assert_eq!(results[0].1, "output");
    /// # }
    /// ```
    pub fn spawn_returning<F>(&mut self, stage: Stage, future: F)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::spawn(future);
        self.returning_handles.push((stage, handle))
    }

    /// Executes all spawned tasks concurrently and collects results.
    ///
    /// Waits for all tasks (both void and returning) to complete. Returns a vector
    /// of `(Stage, T)` tuples containing the stage identifier and result for each
    /// returning task.
    ///
    /// # Returns
    ///
    /// - `Ok(Vec<(Stage, T)>)`: Results from all returning tasks if all tasks succeed
    /// - `Err(Report<TaskError>)`: If any task panics or fails
    ///
    /// # Errors
    ///
    /// This method returns an error if:
    /// - Any task panics
    /// - A task's join handle fails
    ///
    /// The error will identify which stage failed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use db_scan::task_group::TaskGroup;
    /// # use db_scan::timings::Stage;
    /// # async fn example() {
    /// let mut group = TaskGroup::new();
    /// group.spawn(Stage::Scan, async { /* work */ });
    /// group.spawn_returning(Stage::Write, async { "done".to_string() });
    ///
    /// match group.run().await {
    ///     Ok(results) => {
    ///         for (stage, output) in results {
    ///             println!("{}: {}", stage, output);
    ///         }
    ///     }
    ///     Err(e) => eprintln!("Task failed: {}", e),
    /// }
    /// # }
    /// ```
    pub async fn run(self) -> Result<Vec<(Stage, T)>, Report<TaskError>> {
        // Await all handles concurrently
        let void_futures = self.handles.into_iter().map(|(stage, handle)| async move {
            handle.await.change_context(TaskError { stage })?;
            Ok::<_, Report<TaskError>>(())
        });

        let result_futures = self
            .returning_handles
            .into_iter()
            .map(|(stage, handle)| async move {
                let result = handle.await.change_context(TaskError { stage })?;
                Ok::<_, Report<TaskError>>((stage, result))
            });

        // Join both sets of futures together
        let (void_results, results): (Vec<_>, Vec<_>) =
            futures::future::join(join_all(void_futures), join_all(result_futures)).await;

        for result in void_results {
            result?;
        }

        results.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_happy_path() {
        let mut group = TaskGroup::new();

        group.spawn(Stage::Scan, async {
            tokio::time::sleep(Duration::from_millis(10)).await
        });

        group.spawn_returning(Stage::Write, async { "success".to_string() });

        let results = group.run().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, Stage::Write);
        assert_eq!(results[0].1, "success");
    }

    #[tokio::test]
    async fn test_task_panics() {
        // Suppress panic output
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        let mut group = TaskGroup::<()>::new();

        group.spawn(Stage::Scan, async {
            panic!("intentional panic");
        });

        let result = group.run().await;

        // Restore default panic hook
        std::panic::set_hook(default_hook);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.current_context().stage, Stage::Scan);
    }
}
