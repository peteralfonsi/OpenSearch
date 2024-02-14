/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

/**
 * A class containing the 5 metrics tracked by a CacheStats object.
 */
public class CacheStatsResponse { // TODO: Make this extend ToXContent.
    public final long hits;
    public final long misses;
    public final long evictions;
    public final long memorySize;
    public final long entries;

    public CacheStatsResponse(long hits, long misses, long evictions, long memorySize, long entries) {
        this.hits = hits;
        this.misses = misses;
        this.evictions = evictions;
        this.memorySize = memorySize;
        this.entries = entries;
    }
}
