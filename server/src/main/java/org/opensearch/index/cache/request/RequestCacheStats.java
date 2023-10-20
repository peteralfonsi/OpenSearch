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

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.cache.tier.TierType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Request for the query cache statistics
 *
 * @opensearch.internal
 */
public class RequestCacheStats implements Writeable, ToXContentFragment {

    private Map<String, StatsHolder> map;
    public RequestCacheStats() {
        this.map = new HashMap<>();
        for (TierType tierType : TierType.values()) {
            map.put(tierType.getStringValue(), new StatsHolder());
            // Every possible tier type must have counters, even if they are disabled. Then the counters report 0
        }
    }

    public RequestCacheStats(StreamInput in) throws IOException {
        this();
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.map = in.readMap(StreamInput::readString, StatsHolder::new); // does it know to use the right constructor? does it rly need to be registered?
        } else {
            // objects from earlier versions only contain on-heap info, and do not have entries info
            long memorySize = in.readVLong();
            long evictions = in.readVLong();
            long hitCount = in.readVLong();
            long missCount = in.readVLong();
            this.map.put(
                TierType.ON_HEAP.getStringValue(),
                new StatsHolder(
                    memorySize,
                    evictions,
                    hitCount,
                    missCount,
                    0
                ));
        }
    }

    public RequestCacheStats(Map<TierType, StatsHolder> inputMap) {
        // Create a RequestCacheStats with multiple tiers' statistics
        this();
        for (TierType tierType : inputMap.keySet()) {
            map.put(tierType.getStringValue(), inputMap.get(tierType));
        }
    }

    // can prob eliminate some of these constructors

    public void add(RequestCacheStats stats) {
        for (String tier : stats.map.keySet()) {
            map.get(tier).add(stats.map.get(tier));
        }
    }

    private StatsHolder getTierStats(TierType tierType) {
        return map.get(tierType.getStringValue());
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
            out.writeMap(this.map, StreamOutput::writeString, (o, v) -> v.writeTo(o)); // ?
        } else {
            // Write only on-heap values, and don't write entries metric
            StatsHolder heapStats = map.get(TierType.ON_HEAP.getStringValue());
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
        builder.startObject(Fields.TIERS);
        for (TierType tierType : TierType.values()) { // fixed order
            if (tierType != TierType.ON_HEAP) {
                String tier = tierType.getStringValue();
                builder.startObject(tier);
                map.get(tier).toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endObject();
        builder.endObject();
        return builder;
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
