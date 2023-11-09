/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.request;

import org.opensearch.common.cache.tier.DiskTierRequestStats;
import org.opensearch.common.cache.tier.TierRequestStats;
import org.opensearch.common.cache.tier.TierType;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

public class DiskStatsHolder extends StatsHolder {
    final CounterMetric totalGetTime;
    final CounterMetric totalDiskReaches; // Number of times a get() has actually reached the disk
    public DiskStatsHolder() {
        super();
        this.totalGetTime = new CounterMetric();
        this.totalDiskReaches = new CounterMetric();
    }

    public DiskStatsHolder(long memorySize, long evictions, long hitCount, long missCount, long entries, long totalGetTime, long totalDiskReaches) {
        super(memorySize, evictions, hitCount, missCount, entries);
        this.totalGetTime = new CounterMetric();
        this.totalGetTime.inc(totalGetTime);
        this.totalDiskReaches = new CounterMetric();
        this.totalDiskReaches.inc(totalDiskReaches);
    }

    public DiskStatsHolder(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong()
        );
    }

    public long getTotalGetTime() {
        return totalGetTime.count();
    }

    public long getTotalDiskReaches() {
        return totalDiskReaches.count();
    }

    @Override
    public void addRequestStats(TierRequestStats stats) {
        assert stats.getTierType() == TierType.DISK;
        DiskTierRequestStats diskStats = (DiskTierRequestStats) stats;
        // TODO: Should this instead be accomplished with generics somehow
        if (diskStats.getRequestReachedDisk()) {
            this.totalDiskReaches.inc();
            this.totalGetTime.inc(diskStats.getRequestGetTimeNanos());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(totalGetTime.count());
        out.writeVLong(totalDiskReaches.count());
    }

    @Override
    public void add(StatsHolder stats) {
        assert stats.getClass() == DiskStatsHolder.class;
        super.add(stats);
        DiskStatsHolder diskStats = (DiskStatsHolder) stats;
        totalDiskReaches.inc(diskStats.totalDiskReaches.count());
        totalGetTime.inc(diskStats.totalGetTime.count());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        builder.field(Fields.TOTAL_GET_TIME, getTotalGetTime());
        builder.field(Fields.TOTAL_DISK_REACHES, getTotalDiskReaches());
        return builder;
    }

    static final class Fields {
        static final String TOTAL_GET_TIME = "total_get_time_nanos";
        static final String TOTAL_DISK_REACHES = "total_disk_reaches";
    }


}
