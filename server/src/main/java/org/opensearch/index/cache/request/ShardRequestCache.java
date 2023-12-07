/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.cache.request;

import org.apache.lucene.util.Accountable;
import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.tier.CacheValue;
import org.opensearch.common.cache.tier.DiskTierRequestStats;
import org.opensearch.common.cache.tier.OnHeapTierRequestStats;
import org.opensearch.common.cache.tier.TierRequestStats;
import org.opensearch.common.cache.tier.TierType;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the portion of the request cache in use for a particular shard.
 *
 * @opensearch.internal
 */
public final class ShardRequestCache {

    // Holds stats common to all tiers
    private final EnumMap<TierType, StatsHolder> defaultStatsHolder = new EnumMap<>(TierType.class);

    // Holds tier-specific stats
    private final EnumMap<TierType, TierStatsAccumulator> tierSpecificStatsHolder = new EnumMap<>(TierType.class);

    public ShardRequestCache() {
        tierSpecificStatsHolder.put(TierType.ON_HEAP, new OnHeapStatsAccumulator());
        tierSpecificStatsHolder.put(TierType.DISK, new DiskStatsAccumulator());
        for (TierType tierType : TierType.values()) {
            defaultStatsHolder.put(tierType, new StatsHolder());
            if (tierSpecificStatsHolder.get(tierType) == null) {
                throw new OpenSearchException("Missing TierStatsAccumulator for TierType " + tierType.getStringValue());
            }
        }
    }

    public RequestCacheStats stats() {
        return new RequestCacheStats(defaultStatsHolder, tierSpecificStatsHolder);
    }

    public void onHit(CacheValue<BytesReference> cacheValue) {
        TierType source = cacheValue.getSource();
        defaultStatsHolder.get(source).hitCount.inc();
        tierSpecificStatsHolder.get(source).addRequestStats(cacheValue.getStats());
    }

    public void onMiss(CacheValue<BytesReference> cacheValue) {
        TierType source = cacheValue.getSource();
        defaultStatsHolder.get(source).missCount.inc();
        tierSpecificStatsHolder.get(source).addRequestStats(cacheValue.getStats());
    }

    public void onCached(Accountable key, BytesReference value, TierType tierType) {
        defaultStatsHolder.get(tierType).totalMetric.inc(key.ramBytesUsed() + value.ramBytesUsed());
        defaultStatsHolder.get(tierType).entries.inc();
    }

    public void onRemoval(Accountable key, BytesReference value, boolean evicted, TierType tierType) {
        if (evicted) {
            defaultStatsHolder.get(tierType).evictionsMetric.inc();
        }
        long dec = 0;
        if (key != null) {
            dec += key.ramBytesUsed();
        }
        if (value != null) {
            dec += value.ramBytesUsed();
        }
        defaultStatsHolder.get(tierType).totalMetric.dec(dec);
        defaultStatsHolder.get(tierType).entries.dec();
    }

    /**
     * An abstract whose extending classes accumulate tier-specific stats.
     * All extending classes should provide a constructor like TierStatsAccumulator(StreamInput in)
     * as well as a no-argument constructor
     * @param <S> The tier-specific implementation of TierRequestStats to use
     */
    static abstract class TierStatsAccumulator<S extends TierRequestStats> implements Writeable, ToXContentFragment {
        /**
         * Add new stats from a single request to this holder.
         * @param stats The stats from a single request to add
         */
        public abstract void addRequestStats(S stats);

        /**
         * Add the stats from another TierStatsHolder to this TierStatsHolder.
         * Used when combining stats across multiple shards.
         * @param other The other TierStatsHolder.
         */
        public abstract void add(TierStatsAccumulator<S> other);
    }

    /**
     * This class accumulates on-heap-tier-specific stats for a single shard.
     * For now, on-heap tier has no unique stats, but future stats would be added here.
     */
    public static class OnHeapStatsAccumulator extends TierStatsAccumulator<OnHeapTierRequestStats> {
        OnHeapStatsAccumulator() {}

        OnHeapStatsAccumulator(StreamInput in) {}

        @Override
        public void addRequestStats(OnHeapTierRequestStats stats) {}

        @Override
        public void add(TierStatsAccumulator<OnHeapTierRequestStats> other) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    /**
     * This class accumulates disk-tier-specific stats for a single shard.
     */
    public static class DiskStatsAccumulator extends TierStatsAccumulator<DiskTierRequestStats> {
        final CounterMetric totalGetTime;
        final CounterMetric totalDiskReaches; // Number of times a get() has actually reached the disk
        boolean keystoreIsFull;
        long keystoreWeight;
        double staleKeyThreshold;

        public DiskStatsAccumulator() {
            // values accumulated with each new request
            this.totalGetTime = new CounterMetric();
            this.totalDiskReaches = new CounterMetric();

            // values updated with each new request
            this.keystoreIsFull = false;
            this.keystoreWeight = 0;
            this.staleKeyThreshold = 0.0;
        }

        public DiskStatsAccumulator(long totalGetTime, long totalDiskReaches, boolean keystoreIsFull, long keystoreWeight, double staleKeyThreshold) {
            this.totalGetTime = new CounterMetric();
            this.totalGetTime.inc(totalGetTime);
            this.totalDiskReaches = new CounterMetric();
            this.totalDiskReaches.inc(totalDiskReaches);
            this.keystoreIsFull = keystoreIsFull;
            this.keystoreWeight = keystoreWeight;
            this.staleKeyThreshold = staleKeyThreshold;
        }

        public DiskStatsAccumulator(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readBoolean(), in.readVLong(), in.readDouble());
        }

        public long getTotalGetTime() {
            return totalGetTime.count();
        }

        public long getTotalDiskReaches() {
            return totalDiskReaches.count();
        }

        public long getKeystoreWeight() {
            return keystoreWeight;
        }

        public boolean getKeystoreIsFull() {
            return keystoreIsFull;
        }

        public double getStaleKeyThreshold() {
            return staleKeyThreshold;
        }

        @Override
        public void addRequestStats(DiskTierRequestStats stats) {
            if (stats.getRequestReachedDisk()) {
                this.totalDiskReaches.inc();
                this.totalGetTime.inc(stats.getRequestGetTimeNanos());
            }
            // below values only update based on new info from the disk tier, they don't add
            this.keystoreIsFull = stats.getKeystoreIsFull();
            this.keystoreWeight = stats.getKeystoreWeight();
            this.staleKeyThreshold = stats.getStaleKeyThreshold();
        }

        @Override
        public void add(TierStatsAccumulator<DiskTierRequestStats> other) {
            assert other.getClass() == DiskStatsAccumulator.class;
            DiskStatsAccumulator castOther = (DiskStatsAccumulator) other;
            this.totalDiskReaches.inc(castOther.totalDiskReaches.count());
            this.totalGetTime.inc(castOther.totalGetTime.count());
            this.keystoreIsFull = castOther.keystoreIsFull;
            this.keystoreWeight = castOther.keystoreWeight;
            this.staleKeyThreshold = castOther.staleKeyThreshold;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalGetTime.count());
            out.writeVLong(totalDiskReaches.count());
            out.writeBoolean(keystoreIsFull);
            out.writeVLong(keystoreWeight);
            out.writeDouble(staleKeyThreshold);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.TOTAL_GET_TIME, getTotalGetTime());
            builder.field(Fields.TOTAL_DISK_REACHES, getTotalDiskReaches());
            builder.field(Fields.KEYSTORE_IS_FULL, keystoreIsFull);
            builder.field(Fields.KEYSTORE_WEIGHT, keystoreWeight);
            builder.field(Fields.STALE_KEYS_THRESHOLD, staleKeyThreshold);
            return builder;
        }

        static final class Fields { // Used for field names in API response
            static final String TOTAL_GET_TIME = "total_get_time_nanos";
            static final String TOTAL_DISK_REACHES = "total_disk_reaches";
            static final String KEYSTORE_IS_FULL = "keystore_full";
            static final String KEYSTORE_WEIGHT = "keystore_weight";
            static final String STALE_KEYS_THRESHOLD = "stale_keys_threshold";
        }
    }
}
