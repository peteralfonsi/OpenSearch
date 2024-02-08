/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A CacheStats implementation for caches that aggregate over a single dimension.
 * For example, caches in the IndicesRequestCache only aggregate over ShardId value.
 */
public class SingleDimensionCacheStats implements CacheStats {
    // Maintain a counter metric for each shard id (dimension values)
    private final Map<String, CounterMetric> hitsMap;
    private final Map<String, CounterMetric> missesMap;
    private final Map<String, CounterMetric> evictionsMap;
    private final Map<String, CounterMetric> memorySizeMap;
    private final Map<String, CounterMetric> entriesMap;

    // Also maintain a single total counter metric, to avoid having to sum over many values for shards
    private final CounterMetric totalHits;
    private final CounterMetric totalMisses;
    private final CounterMetric totalEvictions;
    private final CounterMetric totalMemorySize;
    private final CounterMetric totalEntries;

    // The allowed dimension name. This stats only allows a single dimension name
    private final String allowedDimensionName;

    public SingleDimensionCacheStats(String allowedDimensionName) {
        this.hitsMap = new HashMap<>();
        this.missesMap = new HashMap<>();
        this.evictionsMap = new HashMap<>();
        this.memorySizeMap = new HashMap<>();
        this.entriesMap = new HashMap<>();

        this.totalHits = new CounterMetric();
        this.totalMisses = new CounterMetric();
        this.totalEvictions = new CounterMetric();
        this.totalMemorySize = new CounterMetric();
        this.totalEntries = new CounterMetric();

        this.allowedDimensionName = allowedDimensionName;
    }

    public SingleDimensionCacheStats(StreamInput in) throws IOException {
        this.hitsMap = convertLongMapToCounterMetric(in.readMap(StreamInput::readString, StreamInput::readVLong));
        this.missesMap = convertLongMapToCounterMetric(in.readMap(StreamInput::readString, StreamInput::readVLong));
        this.evictionsMap = convertLongMapToCounterMetric(in.readMap(StreamInput::readString, StreamInput::readVLong));
        this.memorySizeMap = convertLongMapToCounterMetric(in.readMap(StreamInput::readString, StreamInput::readVLong));
        this.entriesMap = convertLongMapToCounterMetric(in.readMap(StreamInput::readString, StreamInput::readVLong));

        this.totalHits = new CounterMetric();
        totalHits.inc(in.readVLong());
        this.totalMisses = new CounterMetric();
        totalMisses.inc(in.readVLong());
        this.totalEvictions = new CounterMetric();
        totalEvictions.inc(in.readVLong());
        this.totalMemorySize = new CounterMetric();
        totalMemorySize.inc(in.readVLong());
        this.totalEntries = new CounterMetric();
        totalEntries.inc(in.readVLong());

        this.allowedDimensionName = in.readString();
    }

    @Override
    public long getTotalHits() {
        return this.totalHits.count();
    }

    @Override
    public long getTotalMisses() {
        return this.totalMisses.count();
    }

    @Override
    public long getTotalEvictions() {
        return this.totalEvictions.count();
    }

    @Override
    public long getTotalMemorySize() {
        return this.totalMemorySize.count();
    }

    @Override
    public long getTotalEntries() {
        return this.totalEntries.count();
    }

    private long internalGetByDimension(CacheStatsDimension dimension, Map<String, CounterMetric> metricsMap) {
        CounterMetric counter = metricsMap.get(dimension.dimensionValue);
        if (counter == null) {
            return 0;
        }
        return counter.count();
    }

    @Override
    public long getHitsByDimension(CacheStatsDimension dimension) {
        return internalGetByDimension(dimension, hitsMap);
    }

    @Override
    public long getMissesByDimension(CacheStatsDimension dimension) {
        return internalGetByDimension(dimension, missesMap);
    }

    @Override
    public long getEvictionsByDimension(CacheStatsDimension dimension) {
        return internalGetByDimension(dimension, evictionsMap);
    }

    @Override
    public long getMemorySizeByDimension(CacheStatsDimension dimension) {
        return internalGetByDimension(dimension, memorySizeMap);
    }

    @Override
    public long getEntriesByDimension(CacheStatsDimension dimension) {
        return internalGetByDimension(dimension, entriesMap);
    }

    private boolean checkDimensionList(List<CacheStatsDimension> dimensions) {
        return dimensions.size() == 1 && allowedDimensionName.equals(dimensions.get(0).dimensionName);
    }
    private void internalIncrement(List<CacheStatsDimension> dimensions, Map<String, CounterMetric> metricMap, CounterMetric totalMetric, long incrementAmount) {
        if (checkDimensionList(dimensions)) {
            String dimensionValue = dimensions.get(0).dimensionValue;
            totalMetric.inc(incrementAmount);
            CounterMetric counter = metricMap.get(dimensionValue);
            if (counter == null) {
                counter = new CounterMetric();
                metricMap.put(dimensionValue, counter);
            }
            counter.inc(incrementAmount);
        }
    }

    @Override
    public void incrementHitsByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, hitsMap, totalHits, 1);
    }

    @Override
    public void incrementMissesByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, missesMap, totalMisses, 1);
    }

    @Override
    public void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, evictionsMap, totalEvictions, 1);
    }

    @Override
    public void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes) {
        internalIncrement(dimensions, memorySizeMap, totalMemorySize, amountBytes);
    }

    @Override
    public void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, entriesMap, totalEntries, 1);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(convertCounterMapToLong(hitsMap), StreamOutput::writeString, StreamOutput::writeVLong);
        out.writeMap(convertCounterMapToLong(missesMap), StreamOutput::writeString, StreamOutput::writeVLong);
        out.writeMap(convertCounterMapToLong(evictionsMap), StreamOutput::writeString, StreamOutput::writeVLong);
        out.writeMap(convertCounterMapToLong(memorySizeMap), StreamOutput::writeString, StreamOutput::writeVLong);
        out.writeMap(convertCounterMapToLong(entriesMap), StreamOutput::writeString, StreamOutput::writeVLong);

        out.writeVLong(totalHits.count());
        out.writeVLong(totalMisses.count());
        out.writeVLong(totalEvictions.count());
        out.writeVLong(totalMemorySize.count());
        out.writeVLong(totalEntries.count());

        out.writeString(allowedDimensionName);
    }

    public String getAllowedDimensionName() {
        return allowedDimensionName;
    }

    // For converting to StreamOutput/StreamInput, write maps of longs rather than CounterMetrics which don't support writing
    private Map<String, Long> convertCounterMapToLong(Map<String, CounterMetric> inputMap) {
        Map<String, Long> result = new HashMap<>();
        for (String key : inputMap.keySet()) {
            result.put(key, inputMap.get(key).count());
        }
        return result;
    }

    private Map<String, CounterMetric> convertLongMapToCounterMetric(Map<String, Long> inputMap) {
        Map<String, CounterMetric> result = new HashMap<>();
        for (String key: inputMap.keySet()) {
            CounterMetric counter = new CounterMetric();
            counter.inc(inputMap.get(key));
            result.put(key, counter);
        }
        return result;
    }

    /*@Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }*/

    /*static final class Fields {
        static final String MEMORY_SIZE = "memory_size";
        static final String EVICTIONS = "evictions";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
        static final String ENTRIES = "entries";
    }*/
}
