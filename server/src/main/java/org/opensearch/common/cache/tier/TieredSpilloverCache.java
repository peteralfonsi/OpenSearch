/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.ICacheKey;
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.StoreAwareCacheRemovalNotification;
import org.opensearch.common.cache.store.StoreAwareCacheValue;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.iterable.Iterables;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * This cache spillover the evicted items from heap tier to disk tier. All the new items are first cached on heap
 * and the items evicted from on heap cache are moved to disk based cache. If disk based cache also gets full,
 * then items are eventually evicted from it and removed which will result in cache miss.
 *
 * @param <K> Type of key
 * @param <V> Type of value
 *
 * @opensearch.experimental
 */
public class TieredSpilloverCache<K, V> implements ICache<K, V> {

    // TODO: Remove optional when diskCache implementation is integrated.
    private final Optional<ICache<K, V>> onDiskCache;
    private final ICache<K, V> onHeapCache;
    // Listeners for removals from the two tiers
    private final RemovalListener<ICacheKey<K>, V> onDiskRemovalListener;
    private final RemovalListener<ICacheKey<K>, V> onHeapRemovalListener;

    // The listener for removals from the spillover cache as a whole
    private final RemovalListener<ICacheKey<K>, V> removalListener;
    private final CacheStats stats;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    ReleasableLock readLock = new ReleasableLock(readWriteLock.readLock());
    ReleasableLock writeLock = new ReleasableLock(readWriteLock.writeLock());

    /**
     * Maintains caching tiers in ascending order of cache latency.
     */
    private final List<ICache<K, V>> cacheList;

    TieredSpilloverCache(Builder<K, V> builder) {
        Objects.requireNonNull(builder.onHeapCacheBuilder, "onHeap cache builder can't be null");
        this.onHeapRemovalListener = new TierRemovalListener<>();
        this.onDiskRemovalListener = new TierRemovalListener<>();
        this.onHeapCache = builder.onHeapCacheBuilder.setRemovalListener(onHeapRemovalListener).build();
        if (builder.onDiskCacheBuilder != null) {
            this.onDiskCache = Optional.of(builder.onDiskCacheBuilder.setRemovalListener(onDiskRemovalListener).build());
        } else {
            this.onDiskCache = Optional.empty();
        }
        this.removalListener = builder.removalListener;
        this.cacheList = this.onDiskCache.map(diskTier -> Arrays.asList(this.onHeapCache, diskTier)).orElse(List.of(this.onHeapCache));
        this.stats = new TieredSpilloverCacheStats(onHeapCache.stats(), onDiskCache.get().stats());
    }

    // Package private for testing
    ICache<K, V> getOnHeapCache() {
        return onHeapCache;
    }

    // Package private for testing
    Optional<ICache<K, V>> getOnDiskCache() {
        return onDiskCache;
    }

    @Override
    public V get(ICacheKey<K> key) {
        V cacheValue = getValueFromTieredCache(true).apply(key);
        return cacheValue;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        try (ReleasableLock ignore = writeLock.acquire()) {
            onHeapCache.put(key, value);
        }
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        // We are skipping calling event listeners at this step as we do another get inside below computeIfAbsent.
        // Where we might end up calling onMiss twice for a key not present in onHeap cache.
        // Similary we might end up calling both onMiss and onHit for a key, in case we are receiving concurrent
        // requests for the same key which requires loading only once.
        V cacheValue = getValueFromTieredCache(false).apply(key);
        if (cacheValue == null) {
            // Add the value to the onHeap cache. We are calling computeIfAbsent which does another get inside.
            // This is needed as there can be many requests for the same key at the same time and we only want to load
            // the value once.
            V value = null;
            try (ReleasableLock ignore = writeLock.acquire()) {
                value = onHeapCache.computeIfAbsent(key, loader);
            }
            if (loader.isLoaded()) {
                //listener.onMiss(key, CacheStoreType.ON_HEAP);
                //onDiskCache.ifPresent(diskTier -> listener.onMiss(key, CacheStoreType.DISK));
                //listener.onCached(key, value, CacheStoreType.ON_HEAP);
            } else {
                //listener.onHit(key, value, CacheStoreType.ON_HEAP);
            }
            return value;
        }
        //listener.onHit(key, cacheValue.getValue(), cacheValue.getCacheStoreType());
        /*if (cacheValue.getCacheStoreType().equals(CacheStoreType.DISK)) {
            //listener.onMiss(key, CacheStoreType.ON_HEAP);
        }*/
        return cacheValue; //cacheValue.getValue();
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        // We are trying to invalidate the key from all caches though it would be present in only of them.
        // Doing this as we don't know where it is located. We could do a get from both and check that, but what will
        // also trigger a hit/miss listener event, so ignoring it for now.
        try (ReleasableLock ignore = writeLock.acquire()) {
            for (ICache<K, V> storeAwareCache : cacheList) {
                storeAwareCache.invalidate(key);
            }
        }
    }

    @Override
    public void invalidateAll() {
        try (ReleasableLock ignore = writeLock.acquire()) {
            for (ICache<K, V> storeAwareCache : cacheList) {
                storeAwareCache.invalidateAll();
            }
        }
    }

    /**
     * Provides an iteration over both onHeap and disk keys. This is not protected from any mutations to the cache.
     * @return An iterable over (onHeap + disk) keys
     */
    @Override
    public Iterable<ICacheKey<K>> keys() {
        Iterable<ICacheKey<K>> onDiskKeysIterable;
        if (onDiskCache.isPresent()) {
            onDiskKeysIterable = onDiskCache.get().keys();
        } else {
            onDiskKeysIterable = Collections::emptyIterator;
        }
        return Iterables.concat(onHeapCache.keys(), onDiskKeysIterable);
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public void refresh() {
        try (ReleasableLock ignore = writeLock.acquire()) {
            for (ICache<K, V> cache : cacheList) {
                cache.refresh();
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (ICache<K, V> cache : cacheList) {
            cache.close();
        }
    }

    @Override
    public CacheStats stats() {
        return stats;
    }

    /*@Override
    public void onRemoval(StoreAwareCacheRemovalNotification<K, V> notification) {
        if (RemovalReason.EVICTED.equals(notification.getRemovalReason())
            || RemovalReason.CAPACITY.equals(notification.getRemovalReason())) {
            switch (notification.getCacheStoreType()) {
                case ON_HEAP:
                    try (ReleasableLock ignore = writeLock.acquire()) {
                        onDiskCache.ifPresent(diskTier -> { diskTier.put(notification.getKey(), notification.getValue()); });
                    }
                    onDiskCache.ifPresent(
                        diskTier -> listener.onCached(notification.getKey(), notification.getValue(), CacheStoreType.DISK)
                    );
                    break;
                default:
                    break;
            }
        }
        listener.onRemoval(notification);
    }*/

    private Function<ICacheKey<K>, V> getValueFromTieredCache(boolean triggerEventListener) {
        return key -> {
            try (ReleasableLock ignore = readLock.acquire()) {
                for (ICache<K, V> cache : cacheList) {
                    V value = cache.get(key);
                    if (value != null) {
                        if (triggerEventListener) {
                            //listener.onHit(key, value, cache.getTierType());
                        }
                        //return new StoreAwareCacheValue<>(value, cache.getTierType());
                        return value;
                    } else {
                        if (triggerEventListener) {
                            //listener.onMiss(key, cache.getTierType());
                        }
                    }
                }
            }
            return null;
        };
    }

    // A class which receives removal events from a tier present in the spillover cache.
    private class TierRemovalListener<K, V> implements RemovalListener<ICacheKey<K>, V> {

        @Override
        public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
            // TODO
        }
    }

    /**
     * Builder object for tiered spillover cache.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> {
        private ICacheBuilder<K, V> onHeapCacheBuilder;
        private ICacheBuilder<K, V> onDiskCacheBuilder;
        private RemovalListener<ICacheKey<K>, V> removalListener;

        public Builder() {}

        public Builder<K, V> setOnHeapCacheBuilder(ICacheBuilder<K, V> onHeapCacheBuilder) {
            this.onHeapCacheBuilder = onHeapCacheBuilder;
            return this;
        }

        public Builder<K, V> setOnDiskCacheBuilder(ICacheBuilder<K, V> onDiskCacheBuilder) {
            this.onDiskCacheBuilder = onDiskCacheBuilder;
            return this;
        }

        public Builder<K, V> setRemovalListener(RemovalListener<ICacheKey<K>, V> listener) {
            this.removalListener = listener;
            return this;
        }

        public TieredSpilloverCache<K, V> build() {
            return new TieredSpilloverCache<>(this);
        }
    }
}
