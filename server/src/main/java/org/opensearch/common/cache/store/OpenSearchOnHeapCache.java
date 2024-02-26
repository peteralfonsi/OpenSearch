/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.SingleDimensionCacheStats;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.Objects;
import java.util.function.ToLongBiFunction;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Map;

import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

/**
 * This variant of on-heap cache uses OpenSearch custom cache implementation.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.experimental
 */
public class OpenSearchOnHeapCache<K, V> implements ICache<K, V>, RemovalListener<ICacheKey<K>, V> {
    private final Cache<ICacheKey<K>, V> cache;
    private final CacheStats stats;
    private final RemovalListener<ICacheKey<K>, V> removalListener;

    public OpenSearchOnHeapCache(Builder<K, V> builder) {
        CacheBuilder<ICacheKey<K>, V> cacheBuilder = CacheBuilder.<ICacheKey<K>, V>builder()
            .setMaximumWeight(builder.getMaxWeightInBytes())
            .weigher(builder.getWeigher())
            .removalListener(this);
        if (builder.getExpireAfterAcess() != null) {
            cacheBuilder.setExpireAfterAccess(builder.getExpireAfterAcess());
        }
        cache = cacheBuilder.build();
        String dimensionName = Objects.requireNonNull(builder.shardIdDimensionName, "Shard id dimension name can't be null");
        this.stats = new SingleDimensionCacheStats(dimensionName, CacheStatsDimension.TIER_DIMENSION_VALUE_ON_HEAP);
        this.removalListener = builder.getRemovalListener();
    }

    @Override
    public V get(ICacheKey<K> key) {
        V value = cache.get(key);
        if (value != null) {
            stats.incrementHitsByDimensions(key.dimensions);
        } else {
            stats.incrementMissesByDimensions(key.dimensions);
        }
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        cache.put(key, value);
        stats.incrementEntriesByDimensions(key.dimensions);
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        V value = cache.computeIfAbsent(key, key1 -> loader.load(key));
        if (!loader.isLoaded()) {
            stats.incrementHitsByDimensions(key.dimensions);
        } else {
            stats.incrementMissesByDimensions(key.dimensions);
            stats.incrementEntriesByDimensions(key.dimensions);
        }
        return value;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        cache.invalidate(key);
        stats.decrementEntriesByDimensions(key.dimensions);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
        return cache.keys();
    }

    @Override
    public long count() {
        return stats.getTotalEntries();
    }

    @Override
    public void refresh() {
        cache.refresh();
    }

    @Override
    public void close() {}

    @Override
    public CacheStats stats() {
        return stats;
    }

    @Override
    public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
        removalListener.onRemoval(notification);
    }

    /**
     * Factory to create OpenSearchOnheap cache.
     */
    public static class OpenSearchOnHeapCacheFactory implements Factory {

        public static final String NAME = "opensearch_onheap";

        @Override
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            Map<String, Setting<?>> settingList = OpenSearchOnHeapCacheSettings.getSettingListForCacheType(cacheType);
            Settings settings = config.getSettings();
            return new Builder<K, V>().setMaximumWeightInBytes(
                ((ByteSizeValue) settingList.get(MAXIMUM_SIZE_IN_BYTES_KEY).get(settings)).getBytes()
            ).setWeigher(config.getWeigher()).setRemovalListener(config.getRemovalListener()).build();
        }

        @Override
        public String getCacheName() {
            return NAME;
        }
    }

    /**
     * Builder object
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> extends ICacheBuilder<K, V> {

        private String shardIdDimensionName;

        public Builder<K, V> setShardIdDimensionName(String dimensionName) {
            this.shardIdDimensionName = dimensionName;
            return this;
        }
        @Override
        public ICache<K, V> build() {
            return new OpenSearchOnHeapCache<K, V>(this);
        }
    }
}
