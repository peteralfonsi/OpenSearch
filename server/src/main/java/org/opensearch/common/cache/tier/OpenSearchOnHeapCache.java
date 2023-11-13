/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.unit.TimeValue;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.ToLongBiFunction;

/**
 * This variant of on-heap cache uses OpenSearch custom cache implementation.
 * @param <K> Type of key
 * @param <V> Type of value
 */
public class OpenSearchOnHeapCache<K, V> implements OnHeapCachingTier<K, V>, RemovalListener<K, V> {

    private final Cache<K, V> cache;
    private RemovalListener<K, V> removalListener;

    private OpenSearchOnHeapCache(Builder<K, V> builder) {
        Objects.requireNonNull(builder.weigher);
        CacheBuilder<K, V> cacheBuilder = CacheBuilder.<K, V>builder()
            .setMaximumWeight(builder.maxWeightInBytes)
            .weigher(builder.weigher)
            .removalListener(this);
        if (builder.expireAfterAcess != null) {
            cacheBuilder.setExpireAfterAccess(builder.expireAfterAcess);
        }
        cache = cacheBuilder.build();
    }

    @Override
    public void setRemovalListener(RemovalListener<K, V> removalListener) {
        this.removalListener = removalListener;
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public Iterable<K> keys() {
        return this.cache.keys();
    }

    @Override
    public int count() {
        return cache.count();
    }

    @Override
    public TierType getTierType() {
        return TierType.ON_HEAP;
    }

    @Override
    public CacheValue<V> get(K key) {
        return new CacheValue<V>(cache.get(key), TierType.ON_HEAP, new OnHeapTierRequestStats());
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws ExecutionException {
        return cache.computeIfAbsent(key, key1 -> loader.load(key));
    }

    @Override
    public void invalidate(K key) {
        cache.invalidate(key);
    }

    @Override
    public V compute(K key, TieredCacheLoader<K, V> loader) throws Exception {
        return cache.compute(key, key1 -> loader.load(key));
    }

    @Override
    public void refresh() {
        cache.refresh();
    }

    @Override
    public void onRemoval(RemovalNotification<K, V> notification) {
        removalListener.onRemoval(notification);
    }

    /**
     * Builder object
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> {
        private long maxWeightInBytes;

        private ToLongBiFunction<K, V> weigher;

        private TimeValue expireAfterAcess;

        public Builder() {}

        public Builder<K, V> setMaximumWeight(long sizeInBytes) {
            this.maxWeightInBytes = sizeInBytes;
            return this;
        }

        public Builder<K, V> setWeigher(ToLongBiFunction<K, V> weigher) {
            this.weigher = weigher;
            return this;
        }

        public Builder<K, V> setExpireAfterAccess(TimeValue expireAfterAcess) {
            this.expireAfterAcess = expireAfterAcess;
            return this;
        }

        public OpenSearchOnHeapCache<K, V> build() {
            return new OpenSearchOnHeapCache<K, V>(this);
        }
    }
}