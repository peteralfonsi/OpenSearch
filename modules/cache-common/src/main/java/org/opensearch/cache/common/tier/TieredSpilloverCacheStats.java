/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.MultiDimensionCacheStats;
import org.opensearch.common.cache.stats.StatsHolder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * A CacheStats implementation for TieredSpilloverCache, which keeps track of the stats for its tiers.
 */
public class TieredSpilloverCacheStats  { // implements CacheStats
    // Pkg-private for testing
    /*final MultiDimensionCacheStats heapStats;
    final MultiDimensionCacheStats diskStats;

    public static final String TIER_DIMENSION_NAME = "tier";
    public static final String TIER_DIMENSION_VALUE_ON_HEAP = "on_heap";
    public static final String TIER_DIMENSION_VALUE_DISK = "disk";
    public static final List<CacheStatsDimension> HEAP_DIMS = List.of(new CacheStatsDimension(TIER_DIMENSION_NAME, TIER_DIMENSION_VALUE_ON_HEAP));
    public static final List<CacheStatsDimension> DISK_DIMS = List.of(new CacheStatsDimension(TIER_DIMENSION_NAME, TIER_DIMENSION_VALUE_DISK));

    public TieredSpilloverCacheStats(
        Map<StatsHolder.Key, CacheStatsResponse.Snapshot> heapSnapshot,
        Map<StatsHolder.Key, CacheStatsResponse.Snapshot> diskSnapshot,
        List<String> dimensionNames) {
        this.heapStats = new MultiDimensionCacheStats(heapSnapshot, dimensionNames);
        this.diskStats = new MultiDimensionCacheStats(diskSnapshot, dimensionNames);
    }

    public TieredSpilloverCacheStats(StreamInput in) throws IOException {
        this.heapStats = new MultiDimensionCacheStats(in);
        this.diskStats = new MultiDimensionCacheStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        heapStats.writeTo(out);
        diskStats.writeTo(out);
    }

    @Override
    public CacheStatsResponse.Snapshot getTotalStats() {
        return combineTierResponses(heapStats.getTotalStats(), diskStats.getTotalStats());
    }

    public TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> aggregateByLevels(List<String> levels) {
        TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> result = new TreeMap<>(new MultiDimensionCacheStats.KeyComparator());
        if (levels.contains(TIER_DIMENSION_NAME)) {
            // Aggregate by tier. Get the aggregations from each MultiDimensionCacheStats, and combine them into a single
            // TreeMap, adding the tier dimension to each TreeMap key.
            List<String> noTierLevels = new ArrayList<>(levels); // levels might be immutable
            noTierLevels.remove(TIER_DIMENSION_NAME);
            TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> heapAgg = heapStats.aggregateByLevels(noTierLevels);
            TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> diskAgg = diskStats.aggregateByLevels(noTierLevels);

            addKeysWithTierDimension(result, heapAgg, TIER_DIMENSION_VALUE_ON_HEAP);
            addKeysWithTierDimension(result, diskAgg, TIER_DIMENSION_VALUE_DISK);
        }
        else {
            // Don't aggregate by tier. Get aggregations from each MultiDimensionCacheStats. Combine them using combineTierResponses
            // if both aggregations share a key. Otherwise, add directly from the only one which has the key.
            TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> heapAgg = heapStats.aggregateByLevels(levels);
            TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> diskAgg = diskStats.aggregateByLevels(levels);

            for (Map.Entry<StatsHolder.Key, CacheStatsResponse.Snapshot> entry : heapAgg.entrySet()) {
                CacheStatsResponse.Snapshot heapValue = entry.getValue();
                CacheStatsResponse.Snapshot diskValue = diskAgg.get(entry.getKey());
                StatsHolder.Key key = entry.getKey();
                if (diskValue == null) {
                    // Only the heap agg has this particular combination of values, add directly to result
                    result.put(key, heapValue);
                } else {
                    // Both aggregations have this combination, combine them before adding and then remove from diskAgg to avoid double-counting
                    CacheStatsResponse.Snapshot combined = combineTierResponses(heapValue, diskValue);
                    result.put(key, combined);
                    diskAgg.remove(key);
                }
            }
            // The remaining keys are only present in diskAgg
            result.putAll(diskAgg);
        }
        return result;
    }

    // Add all keys in originalAggregation to result, but first add tierDimName to the end of the key.
    private void addKeysWithTierDimension(TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> result,
                                          TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> originalAggregation,
                                          String tierDimName) {
        for (Map.Entry<StatsHolder.Key, CacheStatsResponse.Snapshot> entry : originalAggregation.entrySet()) {
            List<String> newDimensions = new ArrayList<>(entry.getKey().getDimensionValues());
            newDimensions.add(tierDimName); // Tier dimension is at the end as it's the innermost dimension in API responses
            StatsHolder.Key newKey = new StatsHolder.Key(newDimensions);
            result.put(newKey, entry.getValue());
        }
    }

    // pkg-private for testing
    static CacheStatsResponse.Snapshot combineTierResponses(CacheStatsResponse.Snapshot heap, CacheStatsResponse.Snapshot disk) {
        return new CacheStatsResponse.Snapshot(
            heap.getHits() + disk.getHits(),
            disk.getMisses(),
            disk.getEvictions(),
            heap.getSizeInBytes() + disk.getSizeInBytes(),
            heap.getEntries() + disk.getEntries()
        );
    }

    @Override
    public long getTotalHits() {
        return getTotalStats().getHits();
    }

    @Override
    public long getTotalMisses() {
        return getTotalStats().getMisses();
    }

    @Override
    public long getTotalEvictions() {
        return getTotalStats().getEvictions();
    }

    @Override
    public long getTotalSizeInBytes() {
        return getTotalStats().getSizeInBytes();
    }

    @Override
    public long getTotalEntries() {
        return getTotalStats().getEntries();
    }

    CacheStatsResponse.Snapshot getTotalHeapStats() {
        return heapStats.getTotalStats();
    }

    CacheStatsResponse.Snapshot getTotalDiskStats() {
        return diskStats.getTotalStats();
    }*/
}
