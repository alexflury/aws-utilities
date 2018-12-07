package com.alexflury.utils.executors;

import java.util.concurrent.ExecutorService;

/**
 * This class contains utility methods which operate on or return objects of type {@code Executor} or
 * {@link ExecutorService}.
 *
 * @author Alex Flury
 */
public class ExecutorUtils {

    /**
     * Prevents instantiation.
     */
    private ExecutorUtils() {

    }

    /**
     * Returns a thread pool which uses a fixed number of worker threads to execute tasks.  If a task is submitted when
     * the pool is saturated, the calling thread becomes blocked until a worker thread becomes available.
     *
     * @param nThreads the maximum number of worker threads
     * @return a thread pool which uses {@code nThreads} worker threads to execute tasks
     */
    public static ExecutorService blockingFixedThreadPool(final int nThreads) {
        return new BlockingFixedThreadPool(nThreads);
    }

}
