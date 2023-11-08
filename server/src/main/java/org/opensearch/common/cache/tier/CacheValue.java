/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.tier.TierType;

import java.util.Map;

/**
 * Represents a cache value, with its associated tier type where it is stored,
 * and tier-specific stats for an individual request stored in a map.
 * @param <V> Type of value.
 */
public class CacheValue<V> {
    V value;
    TierType source;
    Map<String, Object> stats;

    CacheValue(V value, TierType source, Map<String, Object> stats) {
        this.value = value;
        this.source = source;
        this.stats = stats;
    }
}
