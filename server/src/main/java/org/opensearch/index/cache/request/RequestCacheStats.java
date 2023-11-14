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
import org.opensearch.common.cache.tier.TierRequestStats;
import org.opensearch.common.cache.tier.TierType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Request for the query cache statistics
 *
 * @opensearch.internal
 */
public class RequestCacheStats implements Writeable, ToXContentFragment {

    private Map<String, StatsHolder> defaultStatsMap = new HashMap<>(){{
        for (TierType tierType : TierType.values()) {
            put(tierType.getStringValue(), new StatsHolder());
            // Every possible tier type must have counters, even if they are disabled. Then the counters report 0
        }}
    };

     private Map<String, ShardRequestCache.TierStatsAccumulator> tierSpecificStatsMap = new HashMap<>(){{
        put(TierType.ON_HEAP.getStringValue(), new ShardRequestCache.OnHeapStatsAccumulator());
        put(TierType.DISK.getStringValue(), new ShardRequestCache.DiskStatsAccumulator());
     }};

    public RequestCacheStats() {}

    public RequestCacheStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.defaultStatsMap = in.readMap(StreamInput::readString, StatsHolder::new);
            // Manually fill the tier-specific stats map
            this.tierSpecificStatsMap = new HashMap<>();
            tierSpecificStatsMap.put(TierType.ON_HEAP.getStringValue(), new ShardRequestCache.OnHeapStatsAccumulator(in));
            tierSpecificStatsMap.put(TierType.DISK.getStringValue(), new ShardRequestCache.DiskStatsAccumulator(in));
        } else {
            // objects from earlier versions only contain on-heap info, and do not have entries info
            long memorySize = in.readVLong();
            long evictions = in.readVLong();
            long hitCount = in.readVLong();
            long missCount = in.readVLong();
            this.defaultStatsMap.put(TierType.ON_HEAP.getStringValue(), new StatsHolder(memorySize, evictions, hitCount, missCount, 0));
        }
        checkTierSpecificMap();
    }

    public RequestCacheStats(Map<TierType, StatsHolder> defaultMap, Map<TierType, ShardRequestCache.TierStatsAccumulator> tierSpecificMap) {
        // Create a RequestCacheStats with multiple tiers' statistics
        // The input maps don't have to have all tiers statistics available
        for (TierType tierType : defaultMap.keySet()) {
            defaultStatsMap.put(tierType.getStringValue(), defaultMap.get(tierType));
        }
        for (TierType tierType : tierSpecificMap.keySet()) {
            tierSpecificStatsMap.put(tierType.getStringValue(), tierSpecificMap.get(tierType));
        }
        checkTierSpecificMap();
    }

    public void add(RequestCacheStats stats) {
        for (TierType tierType : TierType.values()) {
            defaultStatsMap.get(tierType.getStringValue()).add(stats.defaultStatsMap.get(tierType.getStringValue()));
            tierSpecificStatsMap.get(tierType.getStringValue()).add(stats.tierSpecificStatsMap.get(tierType.getStringValue()));
        }
    }

    private StatsHolder getTierStats(TierType tierType) {
        return defaultStatsMap.get(tierType.getStringValue());
    }

    ShardRequestCache.TierStatsAccumulator getTierSpecificStats(TierType tierType) { // pkg-private for testing
        return tierSpecificStatsMap.get(tierType.getStringValue());
    }

    public ShardRequestCache.DiskStatsAccumulator getDiskSpecificStats() {
        return (ShardRequestCache.DiskStatsAccumulator) tierSpecificStatsMap.get(TierType.DISK.getStringValue());
    }

    public long getMemorySizeInBytes(TierType tierType) {
        return getTierStats(tierType).totalMetric.count();
    }

    public ByteSizeValue getMemorySize(TierType tierType) {
        return new ByteSizeValue(getMemorySizeInBytes(tierType));
    }

    public long getEvictions(TierType tierType) {
        return getTierStats(tierType).evictionsMetric.count();
    }

    public long getHitCount(TierType tierType) {
        return getTierStats(tierType).hitCount.count();
    }

    public long getMissCount(TierType tierType) {
        return getTierStats(tierType).missCount.count();
    }

    public long getEntries(TierType tierType) {
        return getTierStats(tierType).entries.count();
    }

    // By default, return on-heap stats if no tier is specified

    public long getMemorySizeInBytes() {
        return getMemorySizeInBytes(TierType.ON_HEAP);
    }

    public ByteSizeValue getMemorySize() {
        return getMemorySize(TierType.ON_HEAP);
    }

    public long getEvictions() {
        return getEvictions(TierType.ON_HEAP);
    }

    public long getHitCount() {
        return getHitCount(TierType.ON_HEAP);
    }

    public long getMissCount() {
        return getMissCount(TierType.ON_HEAP);
    }

    public long getEntries() {
        return getEntries(TierType.ON_HEAP);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeMap(this.defaultStatsMap, StreamOutput::writeString, (o, v) -> v.writeTo(o)); // ?
            // Manually write tier-specific stats map
            tierSpecificStatsMap.get(TierType.ON_HEAP.getStringValue()).writeTo(out);
            tierSpecificStatsMap.get(TierType.DISK.getStringValue()).writeTo(out);
        } else {
            // Write only on-heap values, and don't write entries metric
            StatsHolder heapStats = defaultStatsMap.get(TierType.ON_HEAP.getStringValue());
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
        getTierStats(TierType.ON_HEAP).toXContent(builder, params);
        getTierSpecificStats(TierType.ON_HEAP).toXContent(builder, params);
        builder.startObject(Fields.TIERS);
        for (TierType tierType : TierType.values()) { // fixed order
            if (tierType != TierType.ON_HEAP) {
                String tier = tierType.getStringValue();
                builder.startObject(tier);
                defaultStatsMap.get(tier).toXContent(builder, params);
                tierSpecificStatsMap.get(tier).toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    private void checkTierSpecificMap() {
        for (TierType tierType : TierType.values()) {
            if (tierSpecificStatsMap.get(tierType.getStringValue()) == null) {
                throw new OpenSearchException("Missing TierStatsAccumulator for TierType " + tierType.getStringValue());
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
        static final String TIERS = "tiers";
        static final String MEMORY_SIZE = "memory_size";
        static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
        static final String ENTRIES = "entries";
    }
}
