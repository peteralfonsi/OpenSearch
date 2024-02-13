/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.cache.stats.CacheStats;

import java.io.Closeable;

/**
 * Represents a cache interface.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.experimental
 */
public interface ICache<K, V> extends Closeable {
    V get(ICacheKey<K> key);

    void put(ICacheKey<K> key, V value);

    V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception;

    void invalidate(ICacheKey<K> key);

    void invalidateAll();

    Iterable<ICacheKey<K>> keys();

    long count();

    void refresh();

    CacheStats stats();
}
