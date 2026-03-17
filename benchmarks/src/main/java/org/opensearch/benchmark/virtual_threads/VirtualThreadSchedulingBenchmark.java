/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.virtual_threads;

import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ResizableExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.VirtualThreadExecutorBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class VirtualThreadSchedulingBenchmark {

    private static final int QUEUE_SIZE = 10000;
    private static final int CPU_TOKENS = 5000;

    @Param({ "10", "100" })
    private int poolSize;

    @Param({ "100", "1000", "10000" })
    private int taskCount;

    private ExecutorService platformExecutor;
    private ExecutorService virtualExecutor;
    private ExecutorService virtualPerTaskExecutor;

    @Setup(Level.Trial)
    public void setUp() {
        LogConfigurator.setNodeName("benchmark");
        Settings settings = Settings.builder().put("node.name", "benchmark").build();
        ThreadContext threadContext = new ThreadContext(settings);

        ResizableExecutorBuilder platformBuilder = new ResizableExecutorBuilder(
            settings,
            "benchmark_platform",
            poolSize,
            QUEUE_SIZE,
            "thread_pool.benchmark_platform",
            null
        );
        platformExecutor = platformBuilder.build(platformBuilder.getSettings(settings), threadContext).executor();

        VirtualThreadExecutorBuilder virtualBuilder = new VirtualThreadExecutorBuilder(
            settings,
            "benchmark_virtual",
            poolSize,
            QUEUE_SIZE,
            "thread_pool.benchmark_virtual",
            null
        );
        virtualExecutor = virtualBuilder.build(virtualBuilder.getSettings(settings), threadContext).executor();

        // Thread-per-task executor: creates a new virtual thread for each submitted task.
        // poolSize is irrelevant here; included as a baseline for idiomatic virtual thread usage.
        virtualPerTaskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        platformExecutor.shutdown();
        virtualExecutor.shutdown();
        virtualPerTaskExecutor.shutdown();
        try {
            platformExecutor.awaitTermination(10, TimeUnit.SECONDS);
            virtualExecutor.awaitTermination(10, TimeUnit.SECONDS);
            virtualPerTaskExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /*@Benchmark
    public void platformThreads() throws InterruptedException {
        submitAndAwait(platformExecutor);
    }

    @Benchmark
    public void virtualThreads() throws InterruptedException {
        submitAndAwait(virtualExecutor);
    }*/

    @Benchmark
    public void virtualThreadPerTask() throws InterruptedException {
        submitAndAwait(virtualPerTaskExecutor);
    }

    private void submitAndAwait(ExecutorService executor) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            executor.execute(() -> {
                Blackhole.consumeCPU(CPU_TOKENS);
                latch.countDown();
            });
        }
        latch.await();
    }
}
