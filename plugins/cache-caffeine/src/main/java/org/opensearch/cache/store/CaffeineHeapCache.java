/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;

import org.opensearch.OpenSearchException;
import org.opensearch.cache.CaffeineHeapCacheSettings;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.stats.CacheStatsHolder;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.stats.NoopCacheStatsHolder;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.ToLongBiFunction;

import static org.opensearch.cache.CaffeineHeapCacheSettings.EXPIRE_AFTER_ACCESS_KEY;
import static org.opensearch.cache.CaffeineHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

@ExperimentalApi
public class CaffeineHeapCache<K, V> implements ICache<K, V> {

    private final Cache<ICacheKey<K>, V> cache;
    private final CacheStatsHolder cacheStatsHolder;
    private final ToLongBiFunction<ICacheKey<K>, V> weigher;
    private final CaffeineRemovalListener caffeineRemovalListener;

    private CaffeineHeapCache(Builder<K, V> builder) {
        List<String> dimensionNames = Objects.requireNonNull(builder.dimensionNames, "Dimension names can't be null");
        if (builder.getStatsTrackingEnabled()) {
            // If this cache is being used, FeatureFlags.PLUGGABLE_CACHE is already on, so we can always use the DefaultCacheStatsHolder
            // unless statsTrackingEnabled is explicitly set to false in CacheConfig.
            this.cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, "caffeine_heap");
        } else {
            this.cacheStatsHolder = NoopCacheStatsHolder.getInstance();
        }
        this.weigher = Objects.requireNonNull(builder.getWeigher(), "Weigher can't be null");
        this.caffeineRemovalListener = new CaffeineRemovalListener(
            Objects.requireNonNull(builder.getRemovalListener(), "Removal listener can't be null")
        );

        cache = AccessController.doPrivileged(
            (PrivilegedAction<Cache<ICacheKey<K>, V>>) () -> Caffeine.newBuilder()
                .removalListener(this.caffeineRemovalListener)
                .maximumWeight(builder.getMaxWeightInBytes())
                .expireAfterAccess(builder.getExpireAfterAcess().duration(), builder.getExpireAfterAcess().timeUnit())
                .weigher(new CaffeineWeigher(this.weigher))
                .executor(Runnable::run)
                .scheduler(Scheduler.systemScheduler())
                .build()
        );
    }

    /**
     * Wrapper over ICache weigher to be used by Caffeine
     */
    private class CaffeineWeigher implements Weigher<ICacheKey<K>, V> {
        private final ToLongBiFunction<ICacheKey<K>, V> weigher;

        private CaffeineWeigher(ToLongBiFunction<ICacheKey<K>, V> weigher) {
            this.weigher = weigher;
        }

        @Override
        public int weigh(ICacheKey<K> key, V value) {
            return (int) this.weigher.applyAsLong(key, value);
        }
    }

    private class CaffeineRemovalListener implements RemovalListener<ICacheKey<K>, V> {
        private final org.opensearch.common.cache.RemovalListener<ICacheKey<K>, V> removalListener;

        CaffeineRemovalListener(org.opensearch.common.cache.RemovalListener<ICacheKey<K>, V> removalListener) {
            this.removalListener = removalListener;
        }

        @Override
        public void onRemoval(ICacheKey<K> key, V value, RemovalCause removalCause) {
            switch (removalCause) {
                case SIZE:
                    removalListener.onRemoval(new RemovalNotification<>(key, value, RemovalReason.EVICTED));
                    cacheStatsHolder.incrementEvictions(key.dimensions);
                    break;
                case EXPIRED:
                    removalListener.onRemoval(new RemovalNotification<>(key, value, RemovalReason.INVALIDATED));
                    cacheStatsHolder.incrementEvictions(key.dimensions);
                    break;
                case EXPLICIT:
                    removalListener.onRemoval(new RemovalNotification<>(key, value, RemovalReason.EXPLICIT));
                    break;
                case REPLACED:
                    removalListener.onRemoval(new RemovalNotification<>(key, value, RemovalReason.REPLACED));
                    break;
            }
            cacheStatsHolder.decrementItems(key.dimensions);
            cacheStatsHolder.decrementSizeInBytes(key.dimensions, weigher.applyAsLong(key, value));
        }
    }

    @Override
    public V get(ICacheKey<K> key) {
        if (key == null) {
            throw new IllegalArgumentException("Key passed to caffeine heap cache was null.");
        }
        V value;
        value = cache.getIfPresent(key);
        if (value != null) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
        }
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("Key passed to caffeine heap cache was null.");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value passed to caffeine heap cache was null.");
        }
        cache.put(key, value);
        cacheStatsHolder.incrementItems(key.dimensions);
        cacheStatsHolder.incrementSizeInBytes(key.dimensions, weigher.applyAsLong(key, value));
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) {
        V value;
        Function<ICacheKey<K>, V> mappingFunction = k -> {
            V loadedValue;
            try {
                loadedValue = loader.load(k);
            } catch (Exception ex) {
                throw new OpenSearchException("Exception occurred while getting value from cache loader.");
            }
            return loadedValue;
        };
        value = cache.get(key, mappingFunction);
        if (!loader.isLoaded()) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
            cacheStatsHolder.incrementItems(key.dimensions);
            cacheStatsHolder.incrementSizeInBytes(key.dimensions, weigher.applyAsLong(key, value));
        }
        return value;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        if (key == null) {
            throw new IllegalArgumentException("Key passed to caffeine heap cache was null.");
        }
        if (key.getDropStatsForDimensions()) {
            cacheStatsHolder.removeDimensions(key.dimensions);
        }
        if (key.key != null) {
            cache.invalidate(key);
        }
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
        cacheStatsHolder.reset();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
        ConcurrentMap<ICacheKey<K>, V> map = cache.asMap();
        return map.keySet();
    }

    @Override
    public long count() {
        return cacheStatsHolder.count();
    }

    @Override
    public void refresh() {
        // Left empty, as ehcache doesn't provide a refresh method either.
    }

    @Override
    public ImmutableCacheStatsHolder stats(String[] levels) {
        return cacheStatsHolder.getImmutableCacheStatsHolder(levels);
    }

    @Override
    public void close() {}

    public static class CaffeineHeapCacheFactory implements ICache.Factory {

        public static final String NAME = "caffeine_heap";

        @Override
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            Map<String, Setting<?>> settingList = CaffeineHeapCacheSettings.getSettingListForCacheType(cacheType);
            Settings settings = config.getSettings();

            return new Builder<K, V>().setDimensionNames(config.getDimensionNames())
                .setWeigher(config.getWeigher())
                .setRemovalListener(config.getRemovalListener())
                .setExpireAfterAccess((TimeValue) settingList.get(EXPIRE_AFTER_ACCESS_KEY).get(settings))
                .setMaximumWeightInBytes(((ByteSizeValue) settingList.get(MAXIMUM_SIZE_IN_BYTES_KEY).get(settings)).getBytes())
                .setSettings(settings)
                .build();
        }

        @Override
        public String getCacheName() {
            return NAME;
        }
    }

    public static class Builder<K, V> extends ICacheBuilder<K, V> {
        private List<String> dimensionNames;
        private Executor executor = ForkJoinPool.commonPool();

        public Builder() {}

        public Builder<K, V> setDimensionNames(List<String> dimensionNames) {
            this.dimensionNames = dimensionNames;
            return this;
        }

        public Builder<K, V> setExecutor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Executor getExecutor() {
            return executor;
        }

        public CaffeineHeapCache<K, V> build() {
            return new CaffeineHeapCache<>(this);
        }
    }

    /**
     * Manually performs Caffeine maintenance cycle, which includes removing expired entries from the cache.
     * Used for testing.
     */
    void cleanUp() {
        cache.cleanUp();
    }
}
