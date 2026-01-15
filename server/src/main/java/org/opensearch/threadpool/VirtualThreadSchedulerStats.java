/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Statistics for the virtual thread scheduler.
 */
public class VirtualThreadSchedulerStats implements Writeable, ToXContentFragment {

    private final long queuedVirtualThreadCount;
    private final long mountedVirtualThreadCount;
    private final int parallelism;
    private final int poolSize;

    public VirtualThreadSchedulerStats(long queuedVirtualThreadCount, long mountedVirtualThreadCount, int parallelism, int poolSize) {
        this.queuedVirtualThreadCount = queuedVirtualThreadCount;
        this.mountedVirtualThreadCount = mountedVirtualThreadCount;
        this.parallelism = parallelism;
        this.poolSize = poolSize;
    }

    public VirtualThreadSchedulerStats(StreamInput in) throws IOException {
        this.queuedVirtualThreadCount = in.readLong();
        this.mountedVirtualThreadCount = in.readLong();
        this.parallelism = in.readInt();
        this.poolSize = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(queuedVirtualThreadCount);
        out.writeLong(mountedVirtualThreadCount);
        out.writeInt(parallelism);
        out.writeInt(poolSize);
    }

    public long getQueuedVirtualThreadCount() {
        return queuedVirtualThreadCount;
    }

    public long getMountedVirtualThreadCount() {
        return mountedVirtualThreadCount;
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getPoolSize() {
        return poolSize;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("virtual_thread_scheduler");
        builder.field("queued_virtual_thread_count", queuedVirtualThreadCount);
        builder.field("mounted_virtual_thread_count", mountedVirtualThreadCount);
        builder.field("parallelism", parallelism);
        builder.field("pool_size", poolSize);
        builder.endObject();
        return builder;
    }

    public static VirtualThreadSchedulerStats readStats() {
        return new VirtualThreadSchedulerStats(
            VirtualThreadBeanHelper.getQueuedVirtualThreadCount(),
            VirtualThreadBeanHelper.getMountedVirtualThreadCount(),
            VirtualThreadBeanHelper.getParallelism(),
            VirtualThreadBeanHelper.getPoolSize()
        );
    }
}
