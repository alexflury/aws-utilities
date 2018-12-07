package com.alexflury.utils.executors;

import java.util.List;
import java.util.concurrent.*;

/**
 * This thread pool uses a fixed number of worker threads to execute tasks.  If a task is submitted for execution when
 * the pool is saturated, the calling thread becomes blocked until a worker thread becomes available.
 *
 * <p>Similar to {@link Executors#newFixedThreadPool}.  When using {@link Executors#newFixedThreadPool}, tasks submitted
 * for execution are stored in an unbounded queue if the pool is saturated.  If the tasks are processing large amounts
 * of data, the execution queue may occupy an undesirably large amount of memory.  For those cases, use this thread pool
 * instead.</p>
 *
 * <p>Copied from {@linktourl http://stackoverflow.com/questions/2001086/how-to-make-threadpoolexecutors-submit-method-block-if-it-is-saturated}</p>
 */
class BlockingFixedThreadPool extends AbstractExecutorService {

    private final ExecutorService executor;
    private final Semaphore blockExecution;

    BlockingFixedThreadPool(int nThreads) {
        this.executor = Executors.newFixedThreadPool(nThreads);
        blockExecution = new Semaphore(nThreads);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(final Runnable command) {
        blockExecution.acquireUninterruptibly();
        executor.execute(new Runnable() {
            public void run() {
                try {
                    command.run();
                } finally {
                    blockExecution.release();
                }
            }
        });
    }

}
