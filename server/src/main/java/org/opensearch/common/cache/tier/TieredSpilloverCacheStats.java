/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.CacheStatsResponse;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class TieredSpilloverCacheStats implements CacheStats {
    private final CacheStats heapStats;
    private final CacheStats diskStats;

    public TieredSpilloverCacheStats(CacheStats heapStats, CacheStats diskStats) {
        this.heapStats = heapStats;
        this.diskStats = diskStats;
    }

    // TODO: This is a skeleton implementation, not yet functional!
    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public CacheStatsResponse getTotalStats() {
        return null;
    }

    @Override
    public CacheStatsResponse getStatsByDimensions(List<CacheStatsDimension> dimensions) {
        return null;
    }

    @Override
    public long getTotalHits() {
        return 0;
    }

    @Override
    public long getTotalMisses() {
        return 0;
    }

    @Override
    public long getTotalEvictions() {
        return 0;
    }

    @Override
    public long getTotalMemorySize() {
        return 0;
    }

    @Override
    public long getTotalEntries() {
        return 0;
    }

    @Override
    public long getHitsByDimensions(List<CacheStatsDimension> dimensions) {
        return 0;
    }

    @Override
    public long getMissesByDimensions(List<CacheStatsDimension> dimensions) {
        return 0;
    }

    @Override
    public long getEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        return 0;
    }

    @Override
    public long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions) {
        return 0;
    }

    @Override
    public long getEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        return 0;
    }

    @Override
    public void incrementHitsByDimensions(List<CacheStatsDimension> dimensions) {

    }

    @Override
    public void incrementMissesByDimensions(List<CacheStatsDimension> dimensions) {

    }

    @Override
    public void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions) {

    }

    @Override
    public void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes) {

    }

    @Override
    public void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {

    }

    @Override
    public void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {

    }
}
