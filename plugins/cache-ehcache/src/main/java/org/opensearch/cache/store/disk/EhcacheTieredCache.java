/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store.disk;

import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;

import org.opensearch.cache.EhcacheTieredCacheSettings;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.Map;

import static org.opensearch.cache.EhcacheTieredCacheSettings.HEAP_CACHE_MAX_SIZE_IN_BYTES_KEY;

/**
 * Tiered ehcache cache.
 * @param <K>
 * @param <V>
 */
public class EhcacheTieredCache<K, V> extends EhcacheDiskCache<K, V> {

    private final long maxHeapWeightInBytes;
    EhcacheTieredCache(Builder<K, V> builder) {
        super(builder);
        // TODO: Problem: cache is created during super's constructor. So it sees 0 for this value
        this.maxHeapWeightInBytes = builder.maxHeapWeightInBytes;
    }

    @Override
    // TODO: Hardcode 40 MB for initial tests. Fix this later if this cache is any good relative to TSC.
    protected ResourcePoolsBuilder getResourcePoolsBuilder() {
        //return ResourcePoolsBuilder.newResourcePoolsBuilder().heap(maxHeapWeightInBytes, MemoryUnit.B).disk(maxWeightInBytes, MemoryUnit.B);
        return ResourcePoolsBuilder.newResourcePoolsBuilder().heap(40, MemoryUnit.MB).disk(maxWeightInBytes, MemoryUnit.B);
    }

    /**
     * Factory to create an ehcache tiered cache.
     */
    public static class EhcacheTieredCacheFactory extends EhcacheDiskCache.EhcacheDiskCacheFactory {

        /**
         * Ehcache disk cache name.
         */
        public static final String EHCACHE_TIERED_CACHE_NAME = "ehcache_tiered";

        /**
         * Default constructor.
         */
        public EhcacheTieredCacheFactory() {}

        @Override
        protected <K, V> EhcacheDiskCache.Builder<K, V> setupBuilder(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            Builder<K, V> builder = new Builder<>(super.setupBuilder(config, cacheType, cacheFactories));
            Map<String, Setting<?>> tieredSpecificSettingList = EhcacheTieredCacheSettings.getSettingListForCacheType(cacheType);
            Settings settings = config.getSettings();
            long maxHeapWeight = (long) tieredSpecificSettingList.get(HEAP_CACHE_MAX_SIZE_IN_BYTES_KEY).get(settings);
            builder.setMaxHeapWeightInBytes(maxHeapWeight);
            return builder;
        }

        @Override
        public String getCacheName() {
            return EHCACHE_TIERED_CACHE_NAME;
        }
    }

    /**
     * Blorp
     * @param <K>
     * @param <V>
     */
    public static class Builder<K, V> extends EhcacheDiskCache.Builder<K, V> {
        private long maxHeapWeightInBytes;

        // Yuck!

        /**
         * Make from ehcache disk cache builder
         * @param baseBuilder
         */
        public Builder(EhcacheDiskCache.Builder<K, V> baseBuilder) {
            this.setMaximumWeightInBytes(baseBuilder.getMaxWeightInBytes());
            this.setWeigher(baseBuilder.getWeigher());
            this.setExpireAfterAccess(baseBuilder.getExpireAfterAcess());
            this.setSettings(baseBuilder.getSettings());
            this.setRemovalListener(baseBuilder.getRemovalListener());
            this.setStatsTrackingEnabled(baseBuilder.getStatsTrackingEnabled());
            this.setNumberOfSegments(baseBuilder.getNumberOfSegments());

            this.cacheType = baseBuilder.cacheType;
            this.storagePath = baseBuilder.storagePath;
            this.threadPoolAlias = baseBuilder.threadPoolAlias;
            this.diskCacheAlias = baseBuilder.diskCacheAlias;
            this.isEventListenerModeSync = baseBuilder.isEventListenerModeSync;
            this.keyType = baseBuilder.keyType;
            this.valueType = baseBuilder.valueType;
            this.dimensionNames = baseBuilder.dimensionNames;
            this.keySerializer = baseBuilder.keySerializer;
            this.valueSerializer = baseBuilder.valueSerializer;
        }

        /**
         * Blorp
         * @param maxHeapWeightInBytes
         * @return
         */
        public Builder<K, V> setMaxHeapWeightInBytes(long maxHeapWeightInBytes) {
            this.maxHeapWeightInBytes = maxHeapWeightInBytes;
            return this;
        }

        @Override
        public EhcacheTieredCache<K, V> build() {
            return new EhcacheTieredCache<>(this);
        }
    }
}
