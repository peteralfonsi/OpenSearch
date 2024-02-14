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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A CacheStats implementation for caches that aggregate over a single dimension, as well as holding a tier dimension.
 * For example, caches in the IndicesRequestCache only aggregate over ShardId value.
 */
public class SingleDimensionCacheStats implements CacheStats {
    // Maintain a counter metric for each shard id (dimension values)
    private final ConcurrentMap<String, CounterMetric> hitsMap;
    private final ConcurrentMap<String, CounterMetric> missesMap;
    private final ConcurrentMap<String, CounterMetric> evictionsMap;
    private final ConcurrentMap<String, CounterMetric> memorySizeMap;
    private final ConcurrentMap<String, CounterMetric> entriesMap;

    // Also maintain a single total counter metric, to avoid having to sum over many values for shards
    private final CounterMetric totalHits;
    private final CounterMetric totalMisses;
    private final CounterMetric totalEvictions;
    private final CounterMetric totalMemorySize;
    private final CounterMetric totalEntries;

    // The allowed dimension name. This stats only allows a single dimension name. Package-private for testing.
    final String allowedDimensionName;
    // The value of the tier dimension for entries in this Stats object. Package-private for testing.
    final String tierDimensionValue;

    public SingleDimensionCacheStats(String allowedDimensionName, String tierDimensionValue) {
        this.hitsMap = new ConcurrentHashMap<>();
        this.missesMap = new ConcurrentHashMap<>();
        this.evictionsMap = new ConcurrentHashMap<>();
        this.memorySizeMap = new ConcurrentHashMap<>();
        this.entriesMap = new ConcurrentHashMap<>();

        this.totalHits = new CounterMetric();
        this.totalMisses = new CounterMetric();
        this.totalEvictions = new CounterMetric();
        this.totalMemorySize = new CounterMetric();
        this.totalEntries = new CounterMetric();

        this.allowedDimensionName = allowedDimensionName;
        this.tierDimensionValue = tierDimensionValue;
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
        this.tierDimensionValue = in.readString();
    }

    @Override
    public CacheStatsResponse getTotalStats() {
        return new CacheStatsResponse(getTotalHits(), getTotalMisses(), getTotalEvictions(), getTotalMemorySize(), getTotalEntries());
    }

    @Override
    public CacheStatsResponse getStatsByDimensions(List<CacheStatsDimension> dimensions) {
        return new CacheStatsResponse(
            getHitsByDimensions(dimensions),
            getMissesByDimensions(dimensions),
            getEvictionsByDimensions(dimensions),
            getMemorySizeByDimensions(dimensions),
            getEntriesByDimensions(dimensions)
        );
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

    private long internalGetByDimension(List<CacheStatsDimension> dimensions, Map<String, CounterMetric> metricsMap, CounterMetric totalMetric) {
        CacheStatsDimension tierDimension = getTierDimensionIfPresent(dimensions);
        if (tierDimension != null) {
            // This get request includes a tier dimension. Return values only if the tier dimension value
            // matches the one for this stats object, otherwise return 0
            assert dimensions.size() == 1 || dimensions.size() == 2; // There can be at most one non-tier dimension value
            if (tierDimension.dimensionValue.equals(tierDimensionValue)) {
                // The list passed in may not be mutable; create a mutable copy to remove the tier dimension
                ArrayList<CacheStatsDimension> modifiedDimensions = new ArrayList<>(dimensions);
                modifiedDimensions.remove(tierDimension);

                if (modifiedDimensions.size() == 1){
                    return internalGetHelper(modifiedDimensions, metricsMap);
                } else {
                    return totalMetric.count();
                }

            } else {
                // Return 0 for incorrect tier value
                return 0;
            }
        } else {
            // This get request doesn't include a tier dimension. Return the appropriate values.
            return internalGetHelper(dimensions, metricsMap);
        }
    }

    private long internalGetHelper(List<CacheStatsDimension> dimensions, Map<String, CounterMetric> metricsMap) {
        assert dimensions.size() == 1;
        CounterMetric counter = metricsMap.get(dimensions.get(0).dimensionValue);
        if (counter == null) {
            return 0;
        }
        return counter.count();
    }

    /**
     * Returns the dimension that represents a tier value, if one is present. Otherwise return null.
     */
    private CacheStatsDimension getTierDimensionIfPresent(List<CacheStatsDimension> dimensions) {
        for (CacheStatsDimension dim : dimensions) {
            if (dim.dimensionName.equals(CacheStatsDimension.TIER_DIMENSION_NAME)) {
                return dim;
            }
        }
        return null;
    }

    @Override
    public long getHitsByDimensions(List<CacheStatsDimension> dimensions) {
        return internalGetByDimension(dimensions, hitsMap, totalHits);
    }

    @Override
    public long getMissesByDimensions(List<CacheStatsDimension> dimensions) {
        return internalGetByDimension(dimensions, missesMap, totalMisses);
    }

    @Override
    public long getEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        return internalGetByDimension(dimensions, evictionsMap, totalEvictions);
    }

    @Override
    public long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions) {
        return internalGetByDimension(dimensions, memorySizeMap, totalMemorySize);
    }

    @Override
    public long getEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        return internalGetByDimension(dimensions, entriesMap, totalEntries);
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
    public void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, entriesMap, totalEntries, -1);
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
        out.writeString(tierDimensionValue);
    }

    // For converting to StreamOutput/StreamInput, write maps of longs rather than CounterMetrics which don't support writing
    private Map<String, Long> convertCounterMapToLong(Map<String, CounterMetric> inputMap) {
        Map<String, Long> result = new HashMap<>();
        for (String key : inputMap.keySet()) {
            result.put(key, inputMap.get(key).count());
        }
        return result;
    }

    private ConcurrentMap<String, CounterMetric> convertLongMapToCounterMetric(Map<String, Long> inputMap) {
        ConcurrentMap<String, CounterMetric> result = new ConcurrentHashMap<>();
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
