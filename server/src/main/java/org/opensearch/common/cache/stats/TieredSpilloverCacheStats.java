/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class TieredSpilloverCacheStats implements CacheStats {
    final SingleDimensionCacheStats heapStats;
    final SingleDimensionCacheStats diskStats;
    public static final String TIER_DIMENSION_NAME = "tier";
    public static final String TIER_DIMENSION_VALUE_ON_HEAP = "on_heap";
    public static final String TIER_DIMENSION_VALUE_DISK = "disk";

    public TieredSpilloverCacheStats(SingleDimensionCacheStats heapStats, SingleDimensionCacheStats diskStats) {
        this.heapStats = heapStats;
        this.diskStats = diskStats;
    }

    public TieredSpilloverCacheStats(StreamInput in) throws IOException {
        heapStats = new SingleDimensionCacheStats(in);
        diskStats = new SingleDimensionCacheStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        heapStats.writeTo(out);
        diskStats.writeTo(out);
    }

    @Override
    public long getTotalHits() {
        return heapStats.getTotalHits() + diskStats.getTotalHits();
    }

    @Override
    public long getTotalMisses() {
        return heapStats.getTotalMisses() + diskStats.getTotalMisses();
    }

    @Override
    public long getTotalEvictions() {
        return heapStats.getTotalEvictions() + diskStats.getTotalEvictions();
    }

    @Override
    public long getTotalMemorySize() {
        return heapStats.getTotalMemorySize() + diskStats.getTotalMemorySize();
    }

    @Override
    public long getTotalEntries() {
        return heapStats.getTotalEntries() + diskStats.getTotalEntries();
    }

    private CacheStatsDimension getTierDimension(List<CacheStatsDimension> dimensions) {
        for (CacheStatsDimension dim : dimensions) {
            if (dim.dimensionName.equals(TIER_DIMENSION_NAME)) {
                return dim;
            }
        }
        return null;
    }

    private long getValueByDimensions(
        List<CacheStatsDimension> dimensions,
        Function<List<CacheStatsDimension>, Long> heapStatsGetterByDimensions,
        Supplier<Long>  heapStatsGetterTotal,
        Function<List<CacheStatsDimension>, Long> diskStatsGetterByDimensions,
        Supplier<Long> diskStatsGetterTotal) {

        CacheStatsDimension tierDimension = getTierDimension(dimensions);
        if (tierDimension == null) {
            // We aren't slicing by tier; add results from both tiers
            return heapStatsGetterByDimensions.apply(dimensions) + diskStatsGetterByDimensions.apply(dimensions);
        } else {
            // We are slicing by tier. Pass the dimensions list (without the tier dimension) to the relevant tier's CacheStats.
            assert tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_ON_HEAP)
                || tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_DISK)
                : "Tier dimension had unrecognized value " + tierDimension.dimensionValue;
            List<CacheStatsDimension> tierDims = new ArrayList<>(dimensions); // The list passed in can be immutable; make a mutable copy
            tierDims.remove(tierDimension);
            if (tierDims.isEmpty()) {
                // If there are no other dimensions, use the Supplier (which gets the total value from the tier)
                if (tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_ON_HEAP)) {
                    return heapStatsGetterTotal.get();
                } else {
                    return diskStatsGetterTotal.get();
                }
            } else {
                // If there are other dimensions, use the Function (which gets the value by dimensions from the tier)
                if (tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_ON_HEAP)) {
                    return heapStatsGetterByDimensions.apply(tierDims);
                } else {
                    return diskStatsGetterByDimensions.apply(tierDims);
                }
            }
        }
    }

    @Override
    public long getHitsByDimensions(List<CacheStatsDimension> dimensions) {
        return getValueByDimensions(dimensions, heapStats::getHitsByDimensions, heapStats::getTotalHits, diskStats::getHitsByDimensions, diskStats::getTotalHits);
    }

    @Override
    public long getMissesByDimensions(List<CacheStatsDimension> dimensions) {
        return getValueByDimensions(dimensions, heapStats::getMissesByDimensions, heapStats::getTotalMisses, diskStats::getMissesByDimensions, diskStats::getTotalMisses);
    }

    @Override
    public long getEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        return getValueByDimensions(dimensions, heapStats::getEvictionsByDimensions, heapStats::getTotalEvictions, diskStats::getEvictionsByDimensions, diskStats::getTotalEvictions);
    }

    @Override
    public long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions) {
        return getValueByDimensions(dimensions, heapStats::getMemorySizeByDimensions, heapStats::getTotalMemorySize, diskStats::getMemorySizeByDimensions, diskStats::getTotalMemorySize);
    }

    @Override
    public long getEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        return getValueByDimensions(dimensions, heapStats::getEntriesByDimensions, heapStats::getTotalEntries, diskStats::getEntriesByDimensions, diskStats::getTotalEntries);
    }

    // The below functions shouldn't be used, so they do nothing. Values are incremented by the tiers themselves,
    // and they increment the heapStats or diskStats objects directly.

    @Override
    public void incrementHitsByDimensions(List<CacheStatsDimension> dimensions) {}
    @Override
    public void incrementMissesByDimensions(List<CacheStatsDimension> dimensions) {}
    @Override
    public void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions) {}
    @Override
    public void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes) {}
    @Override
    public void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {}
    @Override
    public void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {}
}
