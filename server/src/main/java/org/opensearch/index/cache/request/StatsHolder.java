/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.request;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;

/**
 * The basic StatsHolder class, which accumulates shard-level stats that are common across all tiers.
 * Used in ShardRequestCache. Extending classes also handle tier-specific stats for each tier.
 */
public class StatsHolder implements Serializable, Writeable, ToXContentFragment {
    final CounterMetric totalMetric;
    final CounterMetric evictionsMetric;
    final CounterMetric hitCount;
    final CounterMetric missCount;
    final CounterMetric entries;

    public StatsHolder() {
        this.totalMetric = new CounterMetric();
        this.evictionsMetric = new CounterMetric();
        this.hitCount = new CounterMetric();
        this.missCount = new CounterMetric();
        this.entries = new CounterMetric();
    }

    public StatsHolder(long memorySize, long evictions, long hitCount, long missCount, long entries) {
        // Switched argument order to match RequestCacheStats
        this.totalMetric = new CounterMetric();
        this.totalMetric.inc(memorySize);
        this.evictionsMetric = new CounterMetric();
        this.evictionsMetric.inc(evictions);
        this.hitCount = new CounterMetric();
        this.hitCount.inc(hitCount);
        this.missCount = new CounterMetric();
        this.missCount.inc(missCount);
        this.entries = new CounterMetric();
        this.entries.inc(entries);
    }

    public StatsHolder(StreamInput in) throws IOException {
        // Read and write the values of the counter metrics. They should always be positive
        // This object is new, so we shouldn't need version checks for different behavior
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        // java forces us to do this in one line
        // guaranteed to be evaluated in correct order (https://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.7.4)
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalMetric.count());
        out.writeVLong(evictionsMetric.count());
        out.writeVLong(hitCount.count());
        out.writeVLong(missCount.count());
        out.writeVLong(entries.count());
    }

    public void add(StatsHolder otherStats) {
        // Add the argument's metrics to this object's metrics.
        totalMetric.inc(otherStats.totalMetric.count());
        evictionsMetric.inc(otherStats.evictionsMetric.count());
        hitCount.inc(otherStats.hitCount.count());
        missCount.inc(otherStats.missCount.count());
        entries.inc(otherStats.entries.count());
    }

    public long getEvictions() {
        return evictionsMetric.count();
    }

    public long getMemorySize() {
        return totalMetric.count();
    }

    public long getHitCount() {
        return hitCount.count();
    }

    public long getMissCount() {
        return missCount.count();
    }

    public long getEntries() {
        return entries.count();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.humanReadableField(
            RequestCacheStats.Fields.MEMORY_SIZE_IN_BYTES,
            RequestCacheStats.Fields.MEMORY_SIZE,
            new ByteSizeValue(getMemorySize())
        );
        builder.field(RequestCacheStats.Fields.EVICTIONS, getEvictions());
        builder.field(RequestCacheStats.Fields.HIT_COUNT, getHitCount());
        builder.field(RequestCacheStats.Fields.MISS_COUNT, getMissCount());
        builder.field(RequestCacheStats.Fields.ENTRIES, getEntries());
        return builder;
    }
}
