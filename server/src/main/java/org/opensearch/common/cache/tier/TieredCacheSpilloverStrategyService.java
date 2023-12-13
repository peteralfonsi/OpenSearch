/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.indices.IndicesService;

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
 */
public class TieredCacheSpilloverStrategyService<K, V> implements TieredCacheService<K, V>, RemovalListener<K, V> {

    private final OnHeapCachingTier<K, V> onHeapCachingTier;

    /**
     * Optional in case tiered caching is turned off.
     */
    private Optional<DiskCachingTier<K, V>> diskCachingTier;
    private final TieredCacheEventListener<K, V> tieredCacheEventListener;

    /**
     * Maintains caching tiers in order of get calls.
     */
    private List<CachingTier<K, V>> cachingTierList;
    private boolean diskTierInUse; // True when we are using the disk tier, false when it is deactivated but not deleted
    private DiskTierProvider<K, V> diskTierProvider; // Used to obtain new instances of DiskTier when the setting is dynamically turned on

    private TieredCacheSpilloverStrategyService(Builder<K, V> builder) {
        this.onHeapCachingTier = Objects.requireNonNull(builder.onHeapCachingTier);
        this.diskCachingTier = Optional.ofNullable(builder.diskCachingTier);
        diskTierInUse = diskCachingTier.isPresent();
        this.tieredCacheEventListener = Objects.requireNonNull(builder.tieredCacheEventListener);
        this.cachingTierList = this.diskCachingTier.map(diskTier -> Arrays.asList(onHeapCachingTier, diskTier))
            .orElse(List.of(onHeapCachingTier));
        ClusterSettings clusterSettings = Objects.requireNonNull(builder.clusterSettings);
        if (FeatureFlags.isEnabled(FeatureFlags.TIERED_CACHING)) {
            clusterSettings.addSettingsUpdateConsumer(IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED, this::toggleDiskTierEnabled);
            this.diskTierProvider = Objects.requireNonNull(builder.diskTierProvider);
        }
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
        if (!diskTierInUse && diskCachingTier.isPresent()) {
            // Invalidate all keys in an inactive disk tier as well
            diskCachingTier.get().invalidateAll();
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
                    if (diskTierInUse) { //(checkPolicies(notification.getValue()) && diskTierInUse) { // TODO: Uncomment when policy PR merged in
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
                CacheValue<V> cacheValue = cachingTier.get(key);
                if (cacheValue.value != null) {
                    if (trackStats) {
                        tieredCacheEventListener.onHit(key, cacheValue);
                    }
                    return cacheValue; //new CacheValue<>(value, cachingTier.getTierType());
                }
                if (trackStats) {
                    tieredCacheEventListener.onMiss(key, cacheValue);
                }
            }
            return null;
        };
    }



    /**
     * Dynamically add a new disk tier.
     */
    private void addNewDiskTier(DiskCachingTier<K, V> newTier) {
        assert cachingTierList.size() == 1 && diskCachingTier.isEmpty() && !diskTierInUse;
        // list only contains heap tier, and there is no inactive disk tier
        diskCachingTier = Optional.of(newTier);
        cachingTierList = List.of(onHeapCachingTier, diskCachingTier.get());
        diskTierInUse = true;
    }

    /**
     * Dynamically reenable an existing disk tier that has been disabled but not deleted.
     */
    private void enableInactiveDiskTier() {
        assert cachingTierList.size() == 1 && diskCachingTier.isPresent() && !diskTierInUse;
        // list only contains heap tier, and there is an inactive disk tier we can reactivate
        cachingTierList = List.of(onHeapCachingTier, diskCachingTier.get());
        diskTierInUse = true;
    }

    /**
     * Dynamically disable an existing disk tier.
     */
    private void disableDiskTier() {
        // We want to stop searching in or adding to the disk tier, but we don't delete its contents until cache clear API is run.
        // This is consistent with the heap tier's behavior.
        // To do this, we remove the disk tier from cachingTierList but not from diskCachingTier.
        assert cachingTierList.size() == 2 && diskTierInUse; // Contains both heap tier and disk tier
        cachingTierList = List.of(onHeapCachingTier);
        diskTierInUse = false; // disk tier is now inactive
    }

    /**
     * A function to run when disk tier enabled setting (IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED)
     * is enabled/disabled dynamically.
     * @param enableDiskTier the new value of the enable disk tier setting
     */
    private void toggleDiskTierEnabled(boolean enableDiskTier) {
        if (enableDiskTier && !diskTierInUse) {
            // Not currently using a disk tier.
            if (diskCachingTier.isPresent()) {
                // There is a deactivated disk tier already instantiated. Reactivate this one rather than creating a new one
                enableInactiveDiskTier();
            } else {
                // There is no disk tier to reactivate, create a new one now.
                DiskCachingTier<K, V> newTier = diskTierProvider.getDiskTier();
                addNewDiskTier(newTier);
            }
        } else if (!enableDiskTier && diskTierInUse) {
            // If a disk tier existed before, stop using it. Do not delete it or its contents until cache clear API is run
            disableDiskTier();
        }
    }

    /**
     * Builder object
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> {
        private OnHeapCachingTier<K, V> onHeapCachingTier;
        private DiskCachingTier<K, V> diskCachingTier;
        private TieredCacheEventListener<K, V> tieredCacheEventListener;
        private ClusterSettings clusterSettings;
        private DiskTierProvider<K, V> diskTierProvider;

        public Builder() {}

        public Builder<K, V> setOnHeapCachingTier(OnHeapCachingTier<K, V> onHeapCachingTier) {
            this.onHeapCachingTier = onHeapCachingTier;
            return this;
        }

        public Builder<K, V> setOnDiskCachingTier(DiskCachingTier<K, V> diskCachingTier) {
            this.diskCachingTier = diskCachingTier;
            return this;
        }

        public Builder<K, V> setTieredCacheEventListener(TieredCacheEventListener<K, V> tieredCacheEventListener) {
            this.tieredCacheEventListener = tieredCacheEventListener;
            return this;
        }

        public Builder<K, V> setClusterSettings(ClusterSettings clusterSettings) {
            this.clusterSettings = clusterSettings;
            return this;
        }

        public Builder<K, V> setDiskTierProvider(DiskTierProvider<K, V> provider) {
            this.diskTierProvider = provider;
            return this;
        }

        public TieredCacheSpilloverStrategyService<K, V> build() {
            return new TieredCacheSpilloverStrategyService<K, V>(this);
        }
    }

}
