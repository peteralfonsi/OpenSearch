/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;

import java.util.List;

/**
 * Interface for any cache specific stats.
 * TODO: Add rest of stats like hits/misses.
 */
public interface CacheStats extends Writeable { // TODO: Make this extend ToXContentFragment too
    long getTotalHits();
    long getTotalMisses();
    long getTotalEvictions();
    long getTotalMemorySize();
    long getTotalEntries();
    long getHitsByDimensions(List<CacheStatsDimension> dimensions);
    long getMissesByDimensions(List<CacheStatsDimension> dimensions);
    long getEvictionsByDimensions(List<CacheStatsDimension> dimensions);
    long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions);
    long getEntriesByDimensions(List<CacheStatsDimension> dimensions);

    void incrementHitsByDimensions(List<CacheStatsDimension> dimensions);
    void incrementMissesByDimensions(List<CacheStatsDimension> dimensions);
    void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions);
    // Can also use to decrement, with negative values
    void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes);
    void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions);

    void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions);

}
