/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.serialization.SerializerException;
import org.opensearch.OpenSearchException;
import org.opensearch.common.Randomness;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.tier.keystore.RBMIntKeyLookupStore;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import org.ehcache.Cache;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;

import org.ehcache.spi.serialization.SerializerException;

/**
 * @param <K> The key type of cache entries
 * @param <V> The value type of cache entries
 */
public class EhCacheDiskCachingTier<K, V> implements DiskCachingTier<K, V> {

    // Ehcache disk write minimum threads for its pool
    // All number values in setting constructors are default value, min value, and max value
    public static final Setting<Integer> REQUEST_CACHE_DISK_MIN_THREADS = Setting.intSetting(
        "indices.requests.cache.tiered.disk.ehcache.min_threads",
        2,
        1,
        5,
        Property.NodeScope
    );

    // Ehcache disk write maximum threads for its pool
    public static final Setting<Integer> REQUEST_CACHE_DISK_MAX_THREADS = Setting.intSetting(
        "indices.requests.cache.tiered.disk.ehcache.max_threads",
        2,
        1,
        20,
        Property.NodeScope
    );

    // Not be to confused with number of disk segments, this is different. Defines
    // distinct write queues created for disk store where a group of segments share a write queue. This is
    // implemented with ehcache using a partitioned thread pool exectutor By default all segments share a single write
    // queue ie write concurrency is 1. Check OffHeapDiskStoreConfiguration and DiskWriteThreadPool.
    public static final Setting<Integer> REQUEST_CACHE_DISK_WRITE_CONCURRENCY = Setting.intSetting(
        "indices.requests.cache.tiered.disk.ehcache.write_concurrency",
        2,
        1,
        3,
        Property.NodeScope
    );

    // Defines how many segments the disk cache is separated into. Higher number achieves greater concurrency but
    // will hold that many file pointers.
    public static final Setting<Integer> REQUEST_CACHE_DISK_SEGMENTS = Setting.intSetting(
        "indices.requests.cache.tiered.disk.ehcache.segments",
        16,
        1,
        32,
        Property.NodeScope
    );

    // A Cache manager can create many caches.
    private PersistentCacheManager cacheManager;

    // Disk cache
    private Cache<K, byte[]> cache;
    private final long maxWeightInBytes;
    private final String storagePath;

    private final Class<K> keyType;

    private final Class<V> valueType;

    private final TimeValue expireAfterAccess;

    private final EhCacheEventListener<K, V> ehCacheEventListener;

    private final String threadPoolAlias;
    private final ClusterSettings clusterSettings;

    private CounterMetric count = new CounterMetric();

    private final static String DISK_CACHE_ALIAS = "ehDiskCache";

    private final static String THREAD_POOL_ALIAS_PREFIX = "ehcachePool";

    private final static int MINIMUM_MAX_SIZE_IN_BYTES = 1024 * 100; // 100KB

    private final RBMIntKeyLookupStore keystore;

    private final Serializer<K, byte[]> keySerializer;
    private final Serializer<V, byte[]> valueSerializer;

    private EhCacheDiskCachingTier(Builder<K, V> builder) {
        this.keyType = Objects.requireNonNull(builder.keyType, "Key type shouldn't be null");
        this.valueType = Objects.requireNonNull(builder.valueType, "Value type shouldn't be null");
        this.expireAfterAccess = Objects.requireNonNull(builder.expireAfterAccess, "ExpireAfterAccess value shouldn't " + "be null");
        this.keySerializer = Objects.requireNonNull(builder.keySerializer, "Key serializer shouldn't be null");
        this.valueSerializer = Objects.requireNonNull(builder.valueSerializer, "Value serializer shouldn't be null");
        this.ehCacheEventListener = new EhCacheEventListener<K, V>(this.valueSerializer);
        this.maxWeightInBytes = builder.maxWeightInBytes;
        this.storagePath = Objects.requireNonNull(builder.storagePath, "Storage path shouldn't be null") + UUID.randomUUID(); // temporary fix
        if (builder.threadPoolAlias == null || builder.threadPoolAlias.isBlank()) {
            this.threadPoolAlias = THREAD_POOL_ALIAS_PREFIX + "DiskWrite";
        } else {
            this.threadPoolAlias = builder.threadPoolAlias;
        }
        this.clusterSettings = Objects.requireNonNull(builder.clusterSettings, "ClusterSettings object shouldn't be null");

        cacheManager = buildCacheManager();
        this.cache = buildCache(Duration.ofMillis(expireAfterAccess.getMillis()), builder);
        this.keystore = new RBMIntKeyLookupStore(clusterSettings);
    }

    private PersistentCacheManager buildCacheManager() {
        // In case we use multiple ehCaches, we can define this cache manager at a global level.
        return CacheManagerBuilder.newCacheManagerBuilder()
            .with(CacheManagerBuilder.persistence(new File(storagePath)))
            .using(
                PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder()
                    .defaultPool(THREAD_POOL_ALIAS_PREFIX + "Default", 1, 3) // Default pool used for other tasks like
                    // event listeners
                    .pool(
                        this.threadPoolAlias,
                        clusterSettings.get(REQUEST_CACHE_DISK_MIN_THREADS),
                        clusterSettings.get(REQUEST_CACHE_DISK_MAX_THREADS)
                    )
                    .build()
            )
            .build(true);
    }

    private Cache<K, byte[]> buildCache(Duration expireAfterAccess, Builder<K, V> builder) {
        return cacheManager.createCache(
            DISK_CACHE_ALIAS,
            CacheConfigurationBuilder.newCacheConfigurationBuilder(
                keyType,
                byte[].class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B)
            ).withExpiry(new ExpiryPolicy<K, byte[]>() {
                @Override
                public Duration getExpiryForCreation(K key, byte[] value) {
                    return INFINITE;
                }

                @Override
                public Duration getExpiryForAccess(K key, Supplier<? extends byte[]> value) {
                    return expireAfterAccess;
                }

                @Override
                public Duration getExpiryForUpdate(K key, Supplier<? extends byte[]> oldValue, byte[] newValue) {
                    return INFINITE;
                }
            })
                .withService(getListenerConfiguration(builder))
                .withService(
                    new OffHeapDiskStoreConfiguration(
                        this.threadPoolAlias,
                        clusterSettings.get(REQUEST_CACHE_DISK_WRITE_CONCURRENCY),
                        clusterSettings.get(REQUEST_CACHE_DISK_SEGMENTS)
                    )
                )
                .withKeySerializer(new KeySerializerWrapper<K>(keySerializer))
        );
    }

    private CacheEventListenerConfigurationBuilder getListenerConfiguration(Builder<K, V> builder) {
        CacheEventListenerConfigurationBuilder configurationBuilder = CacheEventListenerConfigurationBuilder.newEventListenerConfiguration(
            this.ehCacheEventListener,
            EventType.EVICTED,
            EventType.EXPIRED,
            EventType.REMOVED,
            EventType.UPDATED,
            EventType.CREATED
        ).unordered();
        if (builder.isEventListenerModeSync) {
            return configurationBuilder.synchronous();
        } else {
            return configurationBuilder.asynchronous();
        }
    }

    @Override
    public CacheValue<V> get(K key) {
        // Optimize it by adding key store.
        boolean reachedDisk = false;
        long now = System.nanoTime(); // Nanoseconds required; milliseconds might be too slow on an SSD

        V value = null;
        if (keystore.contains(key.hashCode())) { // Check in-memory store of key hashes to avoid unnecessary disk seek
            value = valueSerializer.deserialize(cache.get(key));
            reachedDisk = true;
        }

        long tookTime = -1L; // This value will be ignored by stats accumulator if reachedDisk is false anyway
        if (reachedDisk) {
            tookTime = System.nanoTime() - now;
        }
        DiskTierRequestStats stats = new DiskTierRequestStats(tookTime, reachedDisk);
        return new CacheValue<>(value, TierType.DISK, stats);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, valueSerializer.serialize(value));
        keystore.add(key.hashCode());
    }

    @Override
    public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
        // Ehcache doesn't offer any such function. Will have to implement our own if needed later on.
        throw new UnsupportedOperationException();
    }

    @Override
    public void invalidate(K key) {
        // There seems to be a thread leak issue while calling this and then closing cache.
        cache.remove(key);
        keystore.remove(key.hashCode());
    }

    @Override
    public V compute(K key, TieredCacheLoader<K, V> loader) throws Exception {
        // Ehcache doesn't offer any such function. Will have to implement our own if needed later on.
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRemovalListener(RemovalListener<K, V> removalListener) {
        ehCacheEventListener.setRemovalListener(removalListener);
    }

    @Override
    public void invalidateAll() {
        // Clear up files.
        keystore.clear();
    }

    @Override
    public Iterable<K> keys() {
        return () -> new EhCacheKeyIterator<>(cache.iterator());
    }

    @Override
    public int count() {
        return (int) count.count();
    }

    @Override
    public TierType getTierType() {
        return TierType.DISK;
    }

    @Override
    public void close() {
        try {
            cacheManager.destroyCache(DISK_CACHE_ALIAS);
            cacheManager.close();
            cacheManager = null;
        } catch (CachePersistenceException e) {
            throw new OpenSearchException("Exception occurred while destroying ehcache and associated data", e);
        } catch (NullPointerException ignored) {} // Another test node has already destroyed the cache manager
    }

    /**
     * Wrapper over Ehcache original listener to listen to desired events and notify desired subscribers.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    class EhCacheEventListener<K, V> implements CacheEventListener<K, byte[]> {

        private Optional<RemovalListener<K, V>> removalListener;
        // We need to pass the value serializer to this listener, as the removal listener is expecting
        // values of type K, V, not K, byte[]
        private Serializer<V, byte[]> valueSerializer;

        EhCacheEventListener(Serializer<V, byte[]> valueSerializer) {
            this.valueSerializer = valueSerializer;
        }

        public void setRemovalListener(RemovalListener<K, V> removalListener) {
            this.removalListener = Optional.ofNullable(removalListener);
        }

        @Override
        public void onEvent(CacheEvent<? extends K, ? extends byte[]> event) {
            switch (event.getType()) {
                case CREATED:
                    count.inc();
                    assert event.getOldValue() == null;
                    break;
                case EVICTED:
                    this.removalListener.ifPresent(
                        listener -> listener.onRemoval(
                            new RemovalNotification<>(
                                event.getKey(),
                                valueSerializer.deserialize(event.getOldValue()),
                                RemovalReason.EVICTED,
                                TierType.DISK
                            )
                        )
                    );
                    count.dec();
                    assert event.getNewValue() == null;
                    break;
                case REMOVED:
                    this.removalListener.ifPresent(
                        listener -> listener.onRemoval(
                            new RemovalNotification<>(
                                event.getKey(),
                                valueSerializer.deserialize(event.getOldValue()),
                                RemovalReason.INVALIDATED,
                                TierType.DISK
                            )
                        )
                    );
                    count.dec();
                    assert event.getNewValue() == null;
                    break;
                case EXPIRED:
                    this.removalListener.ifPresent(
                        listener -> listener.onRemoval(
                            new RemovalNotification<>(
                                event.getKey(),
                                valueSerializer.deserialize(event.getOldValue()),
                                RemovalReason.INVALIDATED,
                                TierType.DISK
                            )
                        )
                    );
                    count.dec();
                    assert event.getNewValue() == null;
                    break;
                case UPDATED:
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * This iterator wraps ehCache iterator and only iterates over its keys.
     * @param <K> Type of key
     */
    class EhCacheKeyIterator<K> implements Iterator<K> {

        Iterator<Cache.Entry<K, byte[]>> iterator;

        EhCacheKeyIterator(Iterator<Cache.Entry<K, byte[]>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public K next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next().getKey();
        }
    }

    /**
     * The wrapper for the key serializer which is passed directly to Ehcache.
     */
    private class KeySerializerWrapper<K> implements org.ehcache.spi.serialization.Serializer<K> {
        public Serializer<K, byte[]> serializer;
        public KeySerializerWrapper(Serializer<K, byte[]> serializer) {
            this.serializer = serializer;
        }

        // This constructor must be present, but does not have to work as we are not actually persisting the disk
        // cache after a restart.
        // See https://www.ehcache.org/documentation/3.0/serializers-copiers.html#persistent-vs-transient-caches
        public KeySerializerWrapper(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {}

        @Override
        public ByteBuffer serialize(K object) throws SerializerException {
            return ByteBuffer.wrap(serializer.serialize(object));
        }

        @Override
        public K read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            byte[] arr = new byte[binary.remaining()];
            binary.get(arr);
            return serializer.deserialize(arr);
        }

        @Override
        public boolean equals(K object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            byte[] arr = new byte[binary.remaining()];
            binary.get(arr);
            return serializer.equals(object, arr);
        }
    }

    /**
     * Builder object to build Ehcache disk tier.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> {
        private long maxWeightInBytes;
        private TimeValue expireAfterAccess;

        private Class<K> keyType;

        private Class<V> valueType;

        private String storagePath;

        private String threadPoolAlias;
        private ClusterSettings clusterSettings;

        private String diskCacheAlias;

        // Provides capability to make ehCache event listener to run in sync mode. Used for testing too.
        private boolean isEventListenerModeSync;
        private Serializer<K, byte[]> keySerializer;
        private Serializer<V, byte[]> valueSerializer;

        public Builder() {}

        public EhCacheDiskCachingTier.Builder<K, V> setMaximumWeightInBytes(long sizeInBytes) {
            if (sizeInBytes <= MINIMUM_MAX_SIZE_IN_BYTES) {
                throw new IllegalArgumentException("Ehcache Disk tier cache size should be greater than " + MINIMUM_MAX_SIZE_IN_BYTES);
            }
            this.maxWeightInBytes = sizeInBytes;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setExpireAfterAccess(TimeValue expireAfterAccess) {
            this.expireAfterAccess = expireAfterAccess;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setKeyType(Class<K> keyType) {
            this.keyType = keyType;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setValueType(Class<V> valueType) {
            this.valueType = valueType;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setStoragePath(String storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setThreadPoolAlias(String threadPoolAlias) {
            this.threadPoolAlias = threadPoolAlias;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setDiskCacheAlias(String diskCacheAlias) {
            this.diskCacheAlias = diskCacheAlias;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setIsEventListenerModeSync(boolean isEventListenerModeSync) {
            this.isEventListenerModeSync = isEventListenerModeSync;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setKeySerializer(Serializer<K, byte[]> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setValueSerializer(Serializer<V, byte[]> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setClusterSettings(ClusterSettings clusterSettings) {
            this.clusterSettings = clusterSettings;
            return this;
        }

        public EhCacheDiskCachingTier<K, V> build() {
            return new EhCacheDiskCachingTier<>(this);
        }
    }
}
