/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier.stats;

import org.opensearch.common.cache.store.enums.CacheStoreType;

/**
 * An interface for single-request tier-specific stats, which are produced on each request from a cache tier
 * and then added to the correct shard's overall StatsHolder for the tier.
 */
public interface TierRequestStats {
    CacheStoreType getCacheStoreType();
}
