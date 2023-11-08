/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import java.util.Optional;

/**
 * This service encapsulates all logic to write/fetch to/from appropriate tiers. Can be implemented with different
 * flavors like spillover etc.
 * @param <K> Type of key
 * @param <V> Type of value
 * @param <W> Type that V can be transformed into to inspect its contents to apply policies.
 *           For example, if V is BytesReference, W might be QuerySearchResult.
 *           Can be the same as V if no transformation is necessary
 */
public interface TieredCacheService<K, V, W> {

    V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception;

    V get(K key);

    void invalidate(K key);

    void invalidateAll();

    long count();

    OnHeapCachingTier<K, V> getOnHeapCachingTier();

    Optional<DiskCachingTier<K, V>> getDiskCachingTier();

    /**
     * Function transforming values of type V to type W so they can be inspected for contents before applying disk-tier policies.
     * @param value
     * @return
     */
    W preDiskCachingPolicyFunction(V value);
}
