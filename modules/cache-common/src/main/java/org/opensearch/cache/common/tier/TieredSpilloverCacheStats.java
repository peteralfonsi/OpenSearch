/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.CacheStatsResponse;
import org.opensearch.common.cache.stats.MultiDimensionCacheStats;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;


/**
 * A CacheStats implementation for TieredSpilloverCache, which keeps track of the stats for its tiers.
 * In future we want to just read the stats from the individual tiers' stats objects, but this isn't
 * possible right now because of the way computeIfAbsent is implemented.
 */
public class TieredSpilloverCacheStats implements CacheStats {
    final List<String> dimensionNames;
    final MultiDimensionCacheStats heapStats;
    final MultiDimensionCacheStats diskStats;

    // TODO: Pull these in from the implementing cache classes somehow?
    public static final String TIER_DIMENSION_VALUE_ON_HEAP = "on_heap";
    public static final String TIER_DIMENSION_VALUE_DISK = "disk";
    public static final List<CacheStatsDimension> HEAP_DIMS = List.of(new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, TIER_DIMENSION_VALUE_ON_HEAP));
    public static final List<CacheStatsDimension> DISK_DIMS = List.of(new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, TIER_DIMENSION_VALUE_DISK));

    public TieredSpilloverCacheStats(List<String> dimensionNames) {
        this.dimensionNames = dimensionNames;
        this.heapStats = new MultiDimensionCacheStats(dimensionNames, TIER_DIMENSION_VALUE_ON_HEAP);
        this.diskStats = new MultiDimensionCacheStats(dimensionNames, TIER_DIMENSION_VALUE_DISK);
    }

    public TieredSpilloverCacheStats(StreamInput in) throws IOException {
        this.dimensionNames = Arrays.asList(in.readStringArray());
        this.heapStats = new MultiDimensionCacheStats(in);
        this.diskStats = new MultiDimensionCacheStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        heapStats.writeTo(out);
        diskStats.writeTo(out);
    }

    @Override
    public CacheStatsResponse getTotalStats() {
        return combineTierResponses(heapStats.getTotalStats(), diskStats.getTotalStats());
    }

    @Override
    public CacheStatsResponse getStatsByDimensions(List<CacheStatsDimension> dimensions) {
        CacheStatsDimension tierDimension = getTierDimension(dimensions);
        if (tierDimension == null) {
            // We aren't slicing by tier; combine results from both tiers to get stats representative of the tiered cache as a whole
            return combineTierResponses(heapStats.getStatsByDimensions(dimensions), diskStats.getStatsByDimensions(dimensions));
        }
        else {
            // We are slicing by tier. Pass the dimensions list to the relevant tier's CacheStats.
            CacheStats tierStats = getTierStats(tierDimension);
            // If there's no non-tier dimension, it's more efficient to use totalStats
            List<CacheStatsDimension> tierDims = new ArrayList<>(dimensions); // The list passed in can be immutable; make a mutable copy
            tierDims.remove(tierDimension);
            if (tierDims.isEmpty()) {
                return tierStats.getTotalStats();
            } else {
                return tierStats.getStatsByDimensions(tierDims);
            }
        }
    }

    // pkg-private for testing
    static CacheStatsResponse combineTierResponses(CacheStatsResponse heap, CacheStatsResponse disk) {
        CacheStatsResponse result = new CacheStatsResponse();
        result.hits.inc(heap.getHits() + disk.getHits());
        result.misses.inc(disk.getMisses());
        result.evictions.inc(disk.getEvictions());
        result.memorySize.inc(heap.getMemorySize() + disk.getMemorySize());
        result.entries.inc(heap.getEntries() + disk.getEntries());
        return result;
    }

    private CacheStats getTierStats(CacheStatsDimension tierDimension) {
        if (tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_ON_HEAP)) {
            return heapStats;
        } else if (tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_DISK)) {
            return diskStats;
        } else {
            throw new IllegalArgumentException("Tier dimension had unrecognized value " + tierDimension.dimensionValue);
        }
    }

    private CacheStatsDimension getTierDimension(List<CacheStatsDimension> dimensions) {
        for (CacheStatsDimension dim : dimensions) {
            if (dim.dimensionName.equals(CacheStatsDimension.TIER_DIMENSION_NAME)) {
                return dim;
            }
        }
        return null;
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
    public long getTotalMemorySize() {
        return getTotalStats().getMemorySize();
    }

    @Override
    public long getTotalEntries() {
        return getTotalStats().getEntries();
    }

    @Override
    public long getHitsByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getHits();
    }

    @Override
    public long getMissesByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getMisses();
    }

    @Override
    public long getEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getEvictions();
    }

    @Override
    public long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getMemorySize();
    }

    @Override
    public long getEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getEntries();
    }

    private void incrementTierValue(List<CacheStatsDimension> dimensions, BiConsumer<CacheStats, List<CacheStatsDimension>> incrementer) {
        CacheStatsDimension tierDim = getTierDimension(dimensions);
        if (tierDim == null) {
            throw new IllegalArgumentException("Attempted to increment a stats value without specifying a tier dimension");
        }
        CacheStats tierStats = getTierStats(tierDim);
        List<CacheStatsDimension> dimsWithoutTier = new ArrayList<>(dimensions);
        dimsWithoutTier.remove(tierDim);
        incrementer.accept(tierStats, dimsWithoutTier);
    }

    @Override
    public void incrementHitsByDimensions(List<CacheStatsDimension> dimensions) {
        incrementTierValue(dimensions, CacheStats::incrementHitsByDimensions);
    }

    @Override
    public void incrementMissesByDimensions(List<CacheStatsDimension> dimensions) {
        incrementTierValue(dimensions, CacheStats::incrementMissesByDimensions);
    }

    @Override
    public void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        incrementTierValue(dimensions, CacheStats::incrementEvictionsByDimensions);
    }

    @Override
    public void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes) {
        CacheStatsDimension tierDim = getTierDimension(dimensions);
        if (tierDim == null) {
            throw new IllegalArgumentException("Attempted to increment a stats value without specifying a tier dimension");
        }
        CacheStats tierStats = getTierStats(tierDim);
        List<CacheStatsDimension> dimsWithoutTier = new ArrayList<>(dimensions);
        dimsWithoutTier.remove(tierDim);
        tierStats.incrementMemorySizeByDimensions(dimsWithoutTier, amountBytes);
    }

    @Override
    public void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        incrementTierValue(dimensions, CacheStats::incrementEntriesByDimensions);
    }

    @Override
    public void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        incrementTierValue(dimensions, CacheStats::decrementEntriesByDimensions);
    }
}
