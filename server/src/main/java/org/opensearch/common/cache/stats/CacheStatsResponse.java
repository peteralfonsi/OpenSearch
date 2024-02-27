/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.Objects;

/**
 * A class containing the 5 metrics tracked by a CacheStats object.
 */
public class CacheStatsResponse { // TODO: Make this extend ToXContent.
    public long hits;
    public long misses;
    public long evictions;
    public long memorySize;
    public long entries;

    public CacheStatsResponse(long hits, long misses, long evictions, long memorySize, long entries) {
        this.hits = hits;
        this.misses = misses;
        this.evictions = evictions;
        this.memorySize = memorySize;
        this.entries = entries;
    }

    public CacheStatsResponse() {
        this.hits = 0;
        this.misses = 0;
        this.evictions = 0;
        this.memorySize = 0;
        this.entries = 0;
    }

    public void add(CacheStatsResponse other) {
        if (other == null) {
            return;
        }
        this.hits += other.hits;
        this.misses += other.misses;
        this.evictions += other.evictions;
        this.memorySize += other.memorySize;
        this.entries += other.entries;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != CacheStatsResponse.class) {
            return false;
        }
        CacheStatsResponse other = (CacheStatsResponse) o;
        return (hits == other.hits)
            && (misses == other.misses)
            && (evictions == other.evictions)
            && (memorySize == other.memorySize)
            && (entries == other.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits, misses, evictions, memorySize, entries);
    }
}
