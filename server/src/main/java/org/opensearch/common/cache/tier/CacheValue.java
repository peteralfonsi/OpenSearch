/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

/**
 * Represents a cache value, with its associated tier type where it is stored,
 * and tier-specific stats for an individual request stored in a map.
 * @param <V> Type of value.
 */
public class CacheValue<V> {
    V value;
    TierType source;
    TierRequestStats stats;

    CacheValue(V value, TierType source, TierRequestStats stats) {
        this.value = value;
        this.source = source;
        this.stats = stats;
    }

    public V getValue() {
        return value;
    }

    public TierType getSource() {
        return source;
    }

    public TierRequestStats getStats() {
        return stats;
    }
}
