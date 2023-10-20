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

public class StatsHolder implements Serializable, Writeable, ToXContentFragment {
    final CounterMetric evictionsMetric;
    final CounterMetric totalMetric;
    final CounterMetric hitCount;
    final CounterMetric missCount;
    final CounterMetric entries;


    public StatsHolder() {
        this.evictionsMetric = new CounterMetric();
        this.totalMetric = new CounterMetric();
        this.hitCount = new CounterMetric();
        this.missCount = new CounterMetric();
        this.entries = new CounterMetric();
    }

    public StatsHolder(StreamInput in) throws IOException  {
        // Read and write the values of the counter metrics. They should always be positive
        this.evictionsMetric = new CounterMetric();
        this.evictionsMetric.inc(in.readVLong());
        this.totalMetric = new CounterMetric();
        this.totalMetric.inc(in.readVLong());
        this.hitCount = new CounterMetric();
        this.hitCount.inc(in.readVLong());
        this.missCount = new CounterMetric();
        this.missCount.inc(in.readVLong());
        this.entries = new CounterMetric();
        this.entries.inc(in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(evictionsMetric.count());
        out.writeVLong(totalMetric.count());
        out.writeVLong(hitCount.count());
        out.writeVLong(missCount.count());
        out.writeVLong(entries.count());
    }

    public void add(StatsHolder otherStats) {
        // Add the argument's metrics to this object's metrics.
        evictionsMetric.inc(otherStats.evictionsMetric.count());
        totalMetric.inc(otherStats.totalMetric.count());
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
        builder.humanReadableField(RequestCacheStats.Fields.MEMORY_SIZE_IN_BYTES, RequestCacheStats.Fields.MEMORY_SIZE, new ByteSizeValue(getMemorySize()));
        builder.field(RequestCacheStats.Fields.EVICTIONS, getEvictions());
        builder.field(RequestCacheStats.Fields.HIT_COUNT, getHitCount());
        builder.field(RequestCacheStats.Fields.MISS_COUNT, getMissCount());
        builder.field(RequestCacheStats.Fields.ENTRIES, getEntries());
        return builder;
    }
}
