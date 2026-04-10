/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.node.Node;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;

/**
 * Executor builder for an unbounded virtual-thread-per-task executor.
 * Each submitted task runs on a fresh virtual thread; there is no queue and no bound on concurrency.
 */
public class VirtualThreadPerTaskExecutorBuilder extends ExecutorBuilder<VirtualThreadPerTaskExecutorBuilder.VirtualThreadPerTaskExecutorSettings> {

    VirtualThreadPerTaskExecutorBuilder(final String name) {
        super(name);
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Collections.emptyList();
    }

    @Override
    VirtualThreadPerTaskExecutorSettings getSettings(Settings settings) {
        return new VirtualThreadPerTaskExecutorSettings(Node.NODE_NAME_SETTING.get(settings));
    }

    @Override
    ThreadPool.ExecutorHolder build(VirtualThreadPerTaskExecutorSettings settings, ThreadContext threadContext) {
        return new ThreadPool.ExecutorHolder(
            Executors.newVirtualThreadPerTaskExecutor(),
            new ThreadPool.Info(name(), ThreadPool.ThreadPoolType.VIRTUAL)
        );
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(Locale.ROOT, "name [%s], virtual thread per task", info.getName());
    }

    static class VirtualThreadPerTaskExecutorSettings extends ExecutorBuilder.ExecutorSettings {
        VirtualThreadPerTaskExecutorSettings(String nodeName) {
            super(nodeName);
        }
    }
}
