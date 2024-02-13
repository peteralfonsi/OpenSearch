/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.xcontent.ToXContentFragment;

import java.util.List;

/**
 * Interface for any cache specific stats. Allows accessing stats by total value or by dimension,
 * and also allows updating stats.
 * When updating stats, we take in the list of dimensions associated with the key/value pair that caused the update.
 * This allows us to aggregate stats by dimension when accessing them.
 */
public interface CacheStats extends CacheStatsBase { // TODO: Make this extend ToXContentFragment too
    void incrementHitsByDimensions(List<CacheStatsDimension> dimensions);
    void incrementMissesByDimensions(List<CacheStatsDimension> dimensions);
    void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions);
    // Can also use to decrement, with negative values
    void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes);
    void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions);
    void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions);

}
