/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.tier.enums.CacheStoreType;

/**
 * A class created by on-heap tier implementations containing on-heap-specific stats for a single request.
 */
public class OnHeapTierRequestStats implements TierRequestStats {
    @Override
    public CacheStoreType getCacheStoreType() {
        return CacheStoreType.ON_HEAP;
    }
}
