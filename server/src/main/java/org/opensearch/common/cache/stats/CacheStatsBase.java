/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.Writeable;

import java.util.List;

/**
 * An interface for accessing stats for a specific cache. Stats can be gotten as totals, or only for specific
 * dimension values.
 */
public interface CacheStatsBase extends Writeable { // TODO: Also extend ToXContentFragment
    // Methods to get total values.
    long getTotalHits();
    long getTotalMisses();
    long getTotalEvictions();
    long getTotalMemorySize();
    long getTotalEntries();

    // Methods to get values for a specific set of dimensions.
    // Returns the sum of values for cache entries that match all dimensions in the list.
    long getHitsByDimensions(List<CacheStatsDimension> dimensions);
    long getMissesByDimensions(List<CacheStatsDimension> dimensions);
    long getEvictionsByDimensions(List<CacheStatsDimension> dimensions);
    long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions);
    long getEntriesByDimensions(List<CacheStatsDimension> dimensions);
}
