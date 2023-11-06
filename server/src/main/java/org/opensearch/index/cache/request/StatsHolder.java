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
    final CounterMetric totalMetric;
    final CounterMetric evictionsMetric;
    final CounterMetric hitCount;
    final CounterMetric missCount;
    final CounterMetric entries;
    double getTimeEWMA; // CounterMetric is long, we need a double

    public StatsHolder() {
        this.totalMetric = new CounterMetric();
        this.evictionsMetric = new CounterMetric();
        this.hitCount = new CounterMetric();
        this.missCount = new CounterMetric();
        this.entries = new CounterMetric();
        this.getTimeEWMA = 0.0;
    }

    public StatsHolder(long memorySize, long evictions, long hitCount, long missCount, long entries, double getTimeEWMA) {
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
        this.getTimeEWMA = getTimeEWMA;
    }

    public StatsHolder(StreamInput in) throws IOException {
        // Read and write the values of the counter metrics. They should always be positive
        // This object is new, so we shouldn't need version checks for different behavior
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readDouble());
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
        out.writeDouble(getTimeEWMA);
    }

    public void add(StatsHolder otherStats) {
        // Add the argument's metrics to this object's metrics.
        totalMetric.inc(otherStats.totalMetric.count());
        evictionsMetric.inc(otherStats.evictionsMetric.count());
        hitCount.inc(otherStats.hitCount.count());
        missCount.inc(otherStats.missCount.count());
        entries.inc(otherStats.entries.count());
        if (!otherStats.isEmpty()) {
            getTimeEWMA = otherStats.getTimeEWMA;
        }

        /* Adding two EWMAs is a bit tricky. If both stats are non-empty we can assume the newer one dominates.
        add() is only called in CommonStats.java in two places:
        1) it's used to either add otherStats to a new (empty) RequestCacheStats
        2) it's used to add new stats to an existing RequestCacheStats
        In both cases, the existing object is older, so we can assume otherStats's EWMA dominates.
        It doesn't make sense to use the existing EWMA in case 1, and in case 2 the actual value
        will be updated from the disk tier on the next hit/miss, so it's probably ok to use otherStats.getTimeEWMA.
        */
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

    public double getTimeEWMA() {
        return getTimeEWMA;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, false); // By default do not write the getTime field
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params, boolean includeGetTime) throws IOException {
        builder.humanReadableField(
            RequestCacheStats.Fields.MEMORY_SIZE_IN_BYTES,
            RequestCacheStats.Fields.MEMORY_SIZE,
            new ByteSizeValue(getMemorySize())
        );
        builder.field(RequestCacheStats.Fields.EVICTIONS, getEvictions());
        builder.field(RequestCacheStats.Fields.HIT_COUNT, getHitCount());
        builder.field(RequestCacheStats.Fields.MISS_COUNT, getMissCount());
        builder.field(RequestCacheStats.Fields.ENTRIES, getEntries());
        if (includeGetTime) {
            builder.field(RequestCacheStats.Fields.GET_TIME_EWMA, getTimeEWMA());
        }
        return builder;
    }

    private boolean isEmpty() {
        return (getEvictions() == 0)
            && (getMemorySize() == 0)
            && (getHitCount() == 0)
            && (getMissCount() == 0)
            && (getEntries() == 0)
            && (getTimeEWMA() == 0.0);
    }
}
