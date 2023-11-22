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

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.cache.tier.enums.CacheStoreType;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Request for the query cache statistics
 *
 * @opensearch.internal
 */
public class RequestCacheStats implements Writeable, ToXContentFragment {

    private Map<String, StatsHolder> defaultStatsMap = new HashMap<>() {
        {
            for (CacheStoreType cacheStoreType : CacheStoreType.values()) {
                put(cacheStoreType.getStringValue(), new StatsHolder());
                // Every possible tier type must have counters, even if they are disabled. Then the counters report 0
            }
        }
    };

    private Map<String, ShardRequestCache.TierStatsAccumulator> tierSpecificStatsMap = new HashMap<>() {
        {
            put(CacheStoreType.ON_HEAP.getStringValue(), new ShardRequestCache.OnHeapStatsAccumulator());
            put(CacheStoreType.DISK.getStringValue(), new ShardRequestCache.DiskStatsAccumulator());
        }
    };

    public RequestCacheStats() {}

    public RequestCacheStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.defaultStatsMap = in.readMap(StreamInput::readString, StatsHolder::new);
            // Manually fill the tier-specific stats map
            this.tierSpecificStatsMap = new HashMap<>();
            tierSpecificStatsMap.put(CacheStoreType.ON_HEAP.getStringValue(), new ShardRequestCache.OnHeapStatsAccumulator(in));
            tierSpecificStatsMap.put(CacheStoreType.DISK.getStringValue(), new ShardRequestCache.DiskStatsAccumulator(in));
        } else {
            // objects from earlier versions only contain on-heap info, and do not have entries info
            long memorySize = in.readVLong();
            long evictions = in.readVLong();
            long hitCount = in.readVLong();
            long missCount = in.readVLong();
            this.defaultStatsMap.put(CacheStoreType.ON_HEAP.getStringValue(), new StatsHolder(memorySize, evictions, hitCount, missCount, 0));
        }
        checkTierSpecificMap();
    }

    public RequestCacheStats(Map<CacheStoreType, StatsHolder> defaultMap, Map<CacheStoreType, ShardRequestCache.TierStatsAccumulator> tierSpecificMap) {
        // Create a RequestCacheStats with multiple tiers' statistics
        // The input maps don't have to have all tiers statistics available
        for (CacheStoreType cacheStoreType : defaultMap.keySet()) {
            defaultStatsMap.put(cacheStoreType.getStringValue(), defaultMap.get(cacheStoreType));
        }
        for (CacheStoreType cacheStoreType : tierSpecificMap.keySet()) {
            tierSpecificStatsMap.put(cacheStoreType.getStringValue(), tierSpecificMap.get(cacheStoreType));
        }
        checkTierSpecificMap();
    }

    public void add(RequestCacheStats stats) {
        for (CacheStoreType cacheStoreType : CacheStoreType.values()) {
            defaultStatsMap.get(cacheStoreType.getStringValue()).add(stats.defaultStatsMap.get(cacheStoreType.getStringValue()));
            tierSpecificStatsMap.get(cacheStoreType.getStringValue()).add(stats.tierSpecificStatsMap.get(cacheStoreType.getStringValue()));
        }
    }

    private StatsHolder getTierStats(CacheStoreType cacheStoreType) {
        return defaultStatsMap.get(cacheStoreType.getStringValue());
    }

    ShardRequestCache.TierStatsAccumulator getTierSpecificStats(CacheStoreType cacheStoreType) { // pkg-private for testing
        return tierSpecificStatsMap.get(cacheStoreType.getStringValue());
    }

    public ShardRequestCache.DiskStatsAccumulator getDiskSpecificStats() {
        return (ShardRequestCache.DiskStatsAccumulator) tierSpecificStatsMap.get(CacheStoreType.DISK.getStringValue());
    }

    public long getMemorySizeInBytes(CacheStoreType cacheStoreType) {
        return getTierStats(cacheStoreType).totalMetric.count();
    }

    public ByteSizeValue getMemorySize(CacheStoreType cacheStoreType) {
        return new ByteSizeValue(getMemorySizeInBytes(cacheStoreType));
    }

    public long getEvictions(CacheStoreType cacheStoreType) {
        return getTierStats(cacheStoreType).evictionsMetric.count();
    }

    public long getHitCount(CacheStoreType cacheStoreType) {
        return getTierStats(cacheStoreType).hitCount.count();
    }

    public long getMissCount(CacheStoreType cacheStoreType) {
        return getTierStats(cacheStoreType).missCount.count();
    }

    public long getEntries(CacheStoreType cacheStoreType) {
        return getTierStats(cacheStoreType).entries.count();
    }

    // By default, return on-heap stats if no tier is specified

    public long getMemorySizeInBytes() {
        return getMemorySizeInBytes(CacheStoreType.ON_HEAP);
    }

    public ByteSizeValue getMemorySize() {
        return getMemorySize(CacheStoreType.ON_HEAP);
    }

    public long getEvictions() {
        return getEvictions(CacheStoreType.ON_HEAP);
    }

    public long getHitCount() {
        return getHitCount(CacheStoreType.ON_HEAP);
    }

    public long getMissCount() {
        return getMissCount(CacheStoreType.ON_HEAP);
    }

    public long getEntries() {
        return getEntries(CacheStoreType.ON_HEAP);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeMap(this.defaultStatsMap, StreamOutput::writeString, (o, v) -> v.writeTo(o)); // ?
            // Manually write tier-specific stats map
            tierSpecificStatsMap.get(CacheStoreType.ON_HEAP.getStringValue()).writeTo(out);
            tierSpecificStatsMap.get(CacheStoreType.DISK.getStringValue()).writeTo(out);
        } else {
            // Write only on-heap values, and don't write entries metric
            StatsHolder heapStats = defaultStatsMap.get(CacheStoreType.ON_HEAP.getStringValue());
            out.writeVLong(heapStats.getMemorySize());
            out.writeVLong(heapStats.getEvictions());
            out.writeVLong(heapStats.getHitCount());
            out.writeVLong(heapStats.getMissCount());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.REQUEST_CACHE_STATS);
        // write on-heap stats outside of tiers object
        getTierStats(CacheStoreType.ON_HEAP).toXContent(builder, params);
        getTierSpecificStats(CacheStoreType.ON_HEAP).toXContent(builder, params);
        if (FeatureFlags.isEnabled(FeatureFlags.TIERED_CACHING)) {
            builder.startObject(Fields.TIERS);
            for (CacheStoreType cacheStoreType : CacheStoreType.values()) { // fixed order
                if (cacheStoreType != CacheStoreType.ON_HEAP) {
                    String tier = cacheStoreType.getStringValue();
                    builder.startObject(tier);
                    defaultStatsMap.get(tier).toXContent(builder, params);
                    tierSpecificStatsMap.get(tier).toXContent(builder, params);
                    builder.endObject();
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private void checkTierSpecificMap() {
        for (CacheStoreType cacheStoreType : CacheStoreType.values()) {
            if (tierSpecificStatsMap.get(cacheStoreType.getStringValue()) == null) {
                throw new OpenSearchException("Missing TierStatsAccumulator for TierType " + cacheStoreType.getStringValue());
            }
        }
    }

    /**
     * Fields used for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String REQUEST_CACHE_STATS = "request_cache";
        static final String MEMORY_SIZE = "memory_size";
        static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
    }
}
