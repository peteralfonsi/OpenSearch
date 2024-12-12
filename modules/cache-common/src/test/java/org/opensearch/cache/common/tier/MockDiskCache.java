/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.serializer.ICacheKeySerializer;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.cache.stats.CacheStatsHolder;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.stats.NoopCacheStatsHolder;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class MockDiskCache<K, V> implements ICache<K, V> {

    Map<ByteArrayWrapper, ByteArrayWrapper> cache;
    int maxSize;
    long delay;

    private final RemovalListener<ICacheKey<K>, V> removalListener;
    private final CacheStatsHolder statsHolder; // Only update for number of entries; this is only used to test statsTrackingEnabled logic
                                                // in TSC

    private final Serializer<ICacheKey<K>, byte[]> keySerializer;
    private final Serializer<V, byte[]> valueSerializer;

    public MockDiskCache(
        int maxSize,
        long delay,
        RemovalListener<ICacheKey<K>, V> removalListener,
        boolean statsTrackingEnabled,
        Serializer<K, byte[]> keySerializer,
        Serializer<V, byte[]> valueSerializer
    ) {
        this.maxSize = maxSize;
        this.delay = delay;
        this.removalListener = removalListener;
        this.cache = new ConcurrentHashMap<ByteArrayWrapper, ByteArrayWrapper>();
        if (statsTrackingEnabled) {
            this.statsHolder = new DefaultCacheStatsHolder(List.of(), "mock_disk_cache");
        } else {
            this.statsHolder = NoopCacheStatsHolder.getInstance();
        }
        this.keySerializer = new ICacheKeySerializer<>(keySerializer);
        this.valueSerializer = valueSerializer;
    }

    @Override
    public V get(ICacheKey<K> key) {
        V value = deserializeValue(cache.get(serializeKey(key)));
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        if (this.cache.size() >= maxSize) { // For simplification
            this.removalListener.onRemoval(new RemovalNotification<>(key, value, RemovalReason.EVICTED));
            this.statsHolder.decrementItems(List.of());
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.cache.put(serializeKey(key), serializeValue(value));
        this.statsHolder.incrementItems(List.of());
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) {
        return deserializeValue(cache.computeIfAbsent(serializeKey(key), key1 -> {
            try {
                return serializeValue(loader.load(key));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        V value = deserializeValue(this.cache.remove(serializeKey(key)));
        if (value != null) {
            removalListener.onRemoval(
                new RemovalNotification<>(key, deserializeValue(cache.get(serializeKey(key))), RemovalReason.INVALIDATED)
            );
        }
    }

    @Override
    public void invalidateAll() {
        this.cache.clear();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
        return () -> new CacheKeyIterator<>(cache, removalListener, keySerializer, valueSerializer);
    }

    @Override
    public long count() {
        return this.cache.size();
    }

    @Override
    public void refresh() {}

    @Override
    public ImmutableCacheStatsHolder stats() {
        // To allow testing of statsTrackingEnabled logic in TSC, return a dummy ImmutableCacheStatsHolder with the
        // right number of entries, unless statsTrackingEnabled is false
        return statsHolder.getImmutableCacheStatsHolder(null);
    }

    @Override
    public ImmutableCacheStatsHolder stats(String[] levels) {
        return null;
    }

    @Override
    public long getMaxHeapBytes() {
        return 0;
    }

    @Override
    public void close() {

    }

    private ByteArrayWrapper serializeKey(ICacheKey<K> key) {
        return new ByteArrayWrapper(keySerializer.serialize(key));
    }

    private ICacheKey<K> deserializeKey(ByteArrayWrapper binary) {
        if (binary == null) {
            return null;
        }
        return keySerializer.deserialize(binary.value);
    }

    private ByteArrayWrapper serializeValue(V value) {
        return new ByteArrayWrapper(valueSerializer.serialize(value));
    }

    private V deserializeValue(ByteArrayWrapper binary) {
        if (binary == null) {
            return null;
        }
        return valueSerializer.deserialize(binary.value);
    }

    public static class MockDiskCacheFactory implements Factory {

        public static final String NAME = "mockDiskCache";
        final long delay;
        final int maxSize;
        final boolean statsTrackingEnabled;
        final int keyValueSize;

        public MockDiskCacheFactory(long delay, int maxSize, boolean statsTrackingEnabled, int keyValueSize) {
            this.delay = delay;
            this.maxSize = maxSize;
            this.statsTrackingEnabled = statsTrackingEnabled;
            this.keyValueSize = keyValueSize;
        }

        @Override
        @SuppressWarnings({ "unchecked" })
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            // As we can't directly IT with the tiered cache and ehcache, check that we receive non-null serializers, as an ehcache disk
            // cache would require.
            assert config.getKeySerializer() != null;
            assert config.getValueSerializer() != null;
            MockDiskCache.Builder<K, V> builder = (Builder<K, V>) new Builder<K, V>().setKeySerializer(
                (Serializer<K, byte[]>) config.getKeySerializer()
            )
                .setValueSerializer((Serializer<V, byte[]>) config.getValueSerializer())
                .setDeliberateDelay(delay)
                .setRemovalListener(config.getRemovalListener())
                .setStatsTrackingEnabled(config.getStatsTrackingEnabled());

            // For mock disk cache, size refers to number of entries for simplicity.
            if (config.getMaxSizeInBytes() > 0) {
                builder.setMaxSize(Math.toIntExact(config.getMaxSizeInBytes()));
            } else {
                builder.setMaxSize(maxSize);
            }
            return builder.build();
        }

        @Override
        public String getCacheName() {
            return NAME;
        }
    }

    public static class Builder<K, V> extends ICacheBuilder<K, V> {

        int maxSize;
        long delay;
        Serializer<K, byte[]> keySerializer;
        Serializer<V, byte[]> valueSerializer;

        @Override
        public ICache<K, V> build() {
            return new MockDiskCache<K, V>(
                this.maxSize,
                this.delay,
                this.getRemovalListener(),
                getStatsTrackingEnabled(),
                keySerializer,
                valueSerializer
            );
        }

        public Builder<K, V> setMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder<K, V> setDeliberateDelay(long millis) {
            this.delay = millis;
            return this;
        }

        public Builder<K, V> setKeySerializer(Serializer<K, byte[]> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder<K, V> setValueSerializer(Serializer<V, byte[]> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

    }

    /**
     * Provides a iterator over keys.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    static class CacheKeyIterator<K, V> implements Iterator<ICacheKey<K>> {
        private final Iterator<Map.Entry<ByteArrayWrapper, ByteArrayWrapper>> entryIterator;
        private final Map<ByteArrayWrapper, ByteArrayWrapper> cache;
        private final RemovalListener<ICacheKey<K>, V> removalListener;
        private ICacheKey<K> currentKey;
        private final Serializer<ICacheKey<K>, byte[]> keySerializer;
        private final Serializer<V, byte[]> valueSerializer;

        public CacheKeyIterator(
            Map<ByteArrayWrapper, ByteArrayWrapper> cache,
            RemovalListener<ICacheKey<K>, V> removalListener,
            Serializer<ICacheKey<K>, byte[]> keySerializer,
            Serializer<V, byte[]> valueSerializer
        ) {
            this.entryIterator = cache.entrySet().iterator();
            this.removalListener = removalListener;
            this.cache = cache;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public ICacheKey<K> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map.Entry<ByteArrayWrapper, ByteArrayWrapper> entry = entryIterator.next();
            currentKey = deserializeKey(entry.getKey());
            return currentKey;
        }

        @Override
        public void remove() {
            if (currentKey == null) {
                throw new IllegalStateException("No element to remove");
            }
            V value = deserializeValue(cache.get(serializeKey(currentKey)));
            cache.remove(serializeKey(currentKey));
            this.removalListener.onRemoval(new RemovalNotification<>(currentKey, value, RemovalReason.INVALIDATED));
            currentKey = null;
        }

        // TODO: Just duplicated these - sad!
        private ByteArrayWrapper serializeKey(ICacheKey<K> key) {
            return new ByteArrayWrapper(keySerializer.serialize(key));
        }

        private ICacheKey<K> deserializeKey(ByteArrayWrapper binary) {
            if (binary == null) {
                return null;
            }
            return keySerializer.deserialize(binary.value);
        }

        private ByteArrayWrapper serializeValue(V value) {
            return new ByteArrayWrapper(valueSerializer.serialize(value));
        }

        private V deserializeValue(ByteArrayWrapper binary) {
            if (binary == null) {
                return null;
            }
            return valueSerializer.deserialize(binary.value);
        }
    }

    // Duplicated from EhcacheDiskCache
    static class ByteArrayWrapper {
        private final byte[] value;

        public ByteArrayWrapper(byte[] value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != ByteArrayWrapper.class) {
                return false;
            }
            ByteArrayWrapper other = (ByteArrayWrapper) o;
            return Arrays.equals(this.value, other.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }
}
