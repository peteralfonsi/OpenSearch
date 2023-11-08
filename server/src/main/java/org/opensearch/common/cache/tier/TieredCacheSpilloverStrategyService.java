/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.search.query.QuerySearchResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * This service spillover the evicted items from upper tier to lower tier. For now, we are spilling the in-memory
 * cache items to disk tier cache.
 * @param <K> Type of key
 * @param <V> Type of value
 * @param <W> Type that V can be unpacked into for inspection by policies. Can be the same as V.
 */
public class TieredCacheSpilloverStrategyService<K, V, W> implements TieredCacheService<K, V, W>, RemovalListener<K, V> {

    private final OnHeapCachingTier<K, V> onHeapCachingTier;

    /**
     * Optional in case tiered caching is turned off.
     */
    private final Optional<DiskCachingTier<K, V>> diskCachingTier;
    private final TieredCacheEventListener<K, V> tieredCacheEventListener;

    /**
     * Maintains caching tiers in order of get calls.
     */
    private final List<CachingTier<K, V>> cachingTierList;
    private final List<CacheTierPolicy<W>> policies;
    private final Function<V, W> transformationFunction;

    private TieredCacheSpilloverStrategyService(Builder<K, V, W> builder) {
        this.onHeapCachingTier = Objects.requireNonNull(builder.onHeapCachingTier);
        this.diskCachingTier = Optional.ofNullable(builder.diskCachingTier);
        this.tieredCacheEventListener = Objects.requireNonNull(builder.tieredCacheEventListener);
        this.cachingTierList = this.diskCachingTier.map(diskTier -> Arrays.asList(onHeapCachingTier, diskTier))
            .orElse(List.of(onHeapCachingTier));
        this.policies = Objects.requireNonNull(builder.policies);
        this.transformationFunction = Objects.requireNonNull(builder.transformationFunction);
        setRemovalListeners();
    }

    /**
     * This method logic is divided into 2 parts:
     * 1. First check whether key is present or not in desired tier. If yes, return the value.
     * 2. If the key is not present, then add the key/value pair to onHeap cache.
     * @param key Key for lookup.
     * @param loader Used to load value in case it is not present in any tier.
     * @return value
     * @throws Exception exception thrown
     */
    @Override
    public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
        CacheValue<V> cacheValue = getValueFromTierCache(true).apply(key);
        if (cacheValue == null) {
            // Add the value to the onHeap cache. Any items if evicted will be moved to lower tier.
            V value = onHeapCachingTier.compute(key, loader);
            tieredCacheEventListener.onCached(key, value, TierType.ON_HEAP);
            return value;
        }
        return cacheValue.value;
    }

    @Override
    public V get(K key) {
        CacheValue<V> cacheValue = getValueFromTierCache(true).apply(key);
        if (cacheValue == null) {
            return null;
        }
        return cacheValue.value;
    }

    /**
     * First fetches the tier type which has this key. And then invalidate accordingly.
     * @param key key to invalidate
     */
    @Override
    public void invalidate(K key) {
        // We don't need to track hits/misses in this case.
        CacheValue<V> cacheValue = getValueFromTierCache(false).apply(key);
        if (cacheValue != null) {
            switch (cacheValue.source) {
                case ON_HEAP:
                    onHeapCachingTier.invalidate(key);
                    break;
                case DISK:
                    diskCachingTier.ifPresent(diskTier -> diskTier.invalidate(key));
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void invalidateAll() {
        for (CachingTier<K, V> cachingTier : cachingTierList) {
            cachingTier.invalidateAll();
        }
    }

    /**
     * Returns the total count of items present in all cache tiers.
     * @return total count of items in cache
     */
    @Override
    public long count() {
        long totalCount = 0;
        for (CachingTier<K, V> cachingTier : cachingTierList) {
            totalCount += cachingTier.count();
        }
        return totalCount;
    }

    /**
     * Called whenever an item is evicted from any cache tier. If the item was evicted from onHeap cache, it is moved
     * to disk tier cache. In case it was evicted from disk tier cache, it will discarded.
     * @param notification Contains info about the removal like reason, key/value etc.
     */
    @Override
    public void onRemoval(RemovalNotification<K, V> notification) {
        if (RemovalReason.EVICTED.equals(notification.getRemovalReason())) {
            switch (notification.getTierType()) {
                case ON_HEAP:
                    if (checkPolicies(notification.getValue())) {
                        diskCachingTier.ifPresent(diskTier -> {
                            diskTier.put(notification.getKey(), notification.getValue());
                            tieredCacheEventListener.onCached(notification.getKey(), notification.getValue(), TierType.DISK);
                        });
                    }
                    break;
                default:
                    break;
            }
        }
        tieredCacheEventListener.onRemoval(notification);
    }

    @Override
    public OnHeapCachingTier<K, V> getOnHeapCachingTier() {
        return this.onHeapCachingTier;
    }

    @Override
    public Optional<DiskCachingTier<K, V>> getDiskCachingTier() {
        return this.diskCachingTier;
    }

    @Override
    public W preDiskCachingPolicyFunction(V value) {
        return transformationFunction.apply(value);
    }

    boolean checkPolicies(V value) {
        W unpacked = preDiskCachingPolicyFunction(value);
        for (CacheTierPolicy<W> policy : policies) {
            if (!policy.checkData(unpacked)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Register this service as a listener to removal events from different caching tiers.
     */
    private void setRemovalListeners() {
        for (CachingTier<K, V> cachingTier : cachingTierList) {
            cachingTier.setRemovalListener(this);
        }
    }

    private Function<K, CacheValue<V>> getValueFromTierCache(boolean trackStats) {
        return key -> {
            for (CachingTier<K, V> cachingTier : cachingTierList) {
                V value = cachingTier.get(key);
                if (value != null) {
                    if (trackStats) {
                        tieredCacheEventListener.onHit(key, value, cachingTier.getTierType());
                    }
                    return new CacheValue<>(value, cachingTier.getTierType());
                }
                if (trackStats) {
                    tieredCacheEventListener.onMiss(key, cachingTier.getTierType());
                }
            }
            return null;
        };
    }

    /**
     * Represents a cache value along with its associated tier type where it is stored.
     * @param <V> Type of value.
     */
    public static class CacheValue<V> {
        V value;
        TierType source;

        CacheValue(V value, TierType source) {
            this.value = value;
            this.source = source;
        }
    }

    /**
     * Builder object
     * @param <K> Type of key
     * @param <V> Type of value
     * @param <W> Type that V can be unpacked into for inspection by policies. Can be the same as V.
     */
    public static class Builder<K, V, W> {
        private OnHeapCachingTier<K, V> onHeapCachingTier;
        private DiskCachingTier<K, V> diskCachingTier;
        private TieredCacheEventListener<K, V> tieredCacheEventListener;
        private ArrayList<CacheTierPolicy<W>> policies = new ArrayList<>();
        private Function<V, W> transformationFunction;

        public Builder() {}

        public Builder<K, V, W> setOnHeapCachingTier(OnHeapCachingTier<K, V> onHeapCachingTier) {
            this.onHeapCachingTier = onHeapCachingTier;
            return this;
        }

        public Builder<K, V, W> setOnDiskCachingTier(DiskCachingTier<K, V> diskCachingTier) {
            this.diskCachingTier = diskCachingTier;
            return this;
        }

        public Builder<K, V, W> setTieredCacheEventListener(TieredCacheEventListener<K, V> tieredCacheEventListener) {
            this.tieredCacheEventListener = tieredCacheEventListener;
            return this;
        }

        public Builder<K, V, W> withPolicy(CacheTierPolicy<W> policy) {
            this.policies.add(policy);
            return this;
        }

        // Add multiple policies at once
        public Builder<K, V, W> withPolicies(List<CacheTierPolicy<W>> policiesList) {
            this.policies.addAll(policiesList);
            return this;
        }

        public Builder<K, V, W> withPreDiskCachingPolicyFunction(Function<V, W> function) {
            this.transformationFunction = function;
            return this;
        }

        public TieredCacheSpilloverStrategyService<K, V, W> build() {
            return new TieredCacheSpilloverStrategyService<K, V, W>(this);
        }
    }

}
