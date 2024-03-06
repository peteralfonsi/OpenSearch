/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.stats.CacheStats;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.opensearch.common.cache.stats.MultiDimensionCacheStats;
import org.opensearch.common.cache.stats.StatsHolder;
import org.opensearch.common.collect.Tuple;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.ToLongBiFunction;

public class CaffeineCache<K, V> implements ICache<K, V> {
    public static final String TIER_DIMENSION_VALUE = "on_heap";
    private final Cache<ICacheKey<K>, V> cache;
    private final StatsHolder statsHolder;
    private final ToLongBiFunction<ICacheKey<K>, V> weigher;
    Map<ICacheKey<K>, CompletableFuture<Tuple<ICacheKey<K>, V>>> completableFutureMap = new ConcurrentHashMap<>();
    public CaffeineCache(ToLongBiFunction<ICacheKey<K>, V> weigher) {
        this.weigher = weigher;

        this.cache = Caffeine.newBuilder()
            .maximumWeight(1_000_000)
            .weigher((k, v) -> (int) weigher.applyAsLong((ICacheKey<K>) k, (V) v))
            .removalListener((ICacheKey<K> key, V value, RemovalCause cause) -> onInternalRemoval(key, value, cause))
            .build();
        this.statsHolder = new StatsHolder(List.of("dummy_dimension_name"));
    }

    @Override
    public V get(ICacheKey<K> key) {
        V value;
        value = cache.getIfPresent(key);
        if (value == null) {
            statsHolder.incrementMissesByDimensions(key.dimensions);
        } else {
            statsHolder.incrementHitsByDimensions(key.dimensions);
        }
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        // (I think) we have to do a get first to know if this key was already in the cache
        V existingValue = cache.getIfPresent(key);
        cache.put(key, value);
        if (existingValue != null) {
            statsHolder.incrementMemorySizeByDimensions(key.dimensions, -weigher.applyAsLong(key, existingValue));
        } else {
            statsHolder.incrementEntriesByDimensions(key.dimensions);
        }
        statsHolder.incrementMemorySizeByDimensions(key.dimensions, weigher.applyAsLong(key, value));
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        V value = cache.getIfPresent(key);
        if (value == null) {
            value = compute(key, loader);
        }
        if (!loader.isLoaded()) {
            statsHolder.incrementHitsByDimensions(key.dimensions);
        } else {
            statsHolder.incrementMissesByDimensions(key.dimensions);
        }
        return value;
    }

    // Duplicated from EhcacheDiskCache implementation.
    private V compute(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        // A future that returns a pair of key/value.
        CompletableFuture<Tuple<ICacheKey<K>, V>> completableFuture = new CompletableFuture<>();
        // Only one of the threads will succeed putting a future into map for the same key.
        // Rest will fetch existing future.
        CompletableFuture<Tuple<ICacheKey<K>, V>> future = completableFutureMap.putIfAbsent(key, completableFuture);
        // Handler to handle results post processing. Takes a tuple<key, value> or exception as an input and returns
        // the value. Also before returning value, puts the value in cache.
        BiFunction<Tuple<ICacheKey<K>, V>, Throwable, V> handler = (pair, ex) -> {
            V value = null;
            if (pair != null) {
                cache.put(pair.v1(), pair.v2());
                value = pair.v2(); // Returning a value itself assuming that a next get should return the same. Should
                // be safe to assume if we got no exception and reached here.
            }
            completableFutureMap.remove(key); // Remove key from map as not needed anymore.
            return value;
        };
        CompletableFuture<V> completableValue;
        if (future == null) {
            future = completableFuture;
            completableValue = future.handle(handler);
            V value;
            try {
                value = loader.load(key);
            } catch (Exception ex) {
                future.completeExceptionally(ex);
                throw new ExecutionException(ex);
            }
            if (value == null) {
                NullPointerException npe = new NullPointerException("loader returned a null value");
                future.completeExceptionally(npe);
                throw new ExecutionException(npe);
            } else {
                future.complete(new Tuple<>(key, value));
            }

        } else {
            completableValue = future.handle(handler);
        }
        V value;
        try {
            value = completableValue.get();
            if (future.isCompletedExceptionally()) {
                future.get(); // call get to force the exception to be thrown for other concurrent callers
                throw new IllegalStateException("Future completed exceptionally but no error thrown");
            }
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
        return value;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
        return null;
    }

    @Override
    public long count() {
        return statsHolder.count();
    }

    @Override
    public void refresh() {

    }

    @Override
    public CacheStats stats() {
        return new MultiDimensionCacheStats(statsHolder, TIER_DIMENSION_VALUE);
    }

    @Override
    public void close() throws IOException {

    }

    private void onInternalRemoval(ICacheKey<K> key, V value, RemovalCause removalCause) {
        statsHolder.decrementEntriesByDimensions(key.dimensions);
        statsHolder.incrementMemorySizeByDimensions(key.dimensions, -weigher.applyAsLong(key, value));
        if (removalCause.wasEvicted()) {
            statsHolder.incrementEntriesByDimensions(key.dimensions);
        }
    }
}
