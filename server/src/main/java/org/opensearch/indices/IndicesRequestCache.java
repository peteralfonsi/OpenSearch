/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.tier.BytesReferenceSerializer;
import org.opensearch.common.cache.tier.CachePolicyInfoWrapper;
import org.opensearch.common.cache.tier.CacheValue;
import org.opensearch.common.cache.tier.DiskTierTookTimePolicy;
import org.opensearch.common.cache.tier.EhCacheDiskCachingTier;
import org.opensearch.common.cache.tier.OnHeapCachingTier;
import org.opensearch.common.cache.tier.OpenSearchOnHeapCache;
import org.opensearch.common.cache.tier.TierType;
import org.opensearch.common.cache.tier.TieredCacheEventListener;
import org.opensearch.common.cache.tier.TieredCacheLoader;
import org.opensearch.common.cache.tier.TieredCacheService;
import org.opensearch.common.cache.tier.TieredCacheSpilloverStrategyService;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * The indices request cache allows to cache a shard level request stage responses, helping with improving
 * similar requests that are potentially expensive (because of aggs for example). The cache is fully coherent
 * with the semantics of NRT (the index reader cache key is part of the cache key), and relies on size based
 * eviction to evict old reader associated cache entries as well as scheduler reaper to clean readers that
 * are no longer used or closed shards.
 * <p>
 * Currently, the cache is only enabled for count requests, and can only be opted in on an index
 * level setting that can be dynamically changed and defaults to false.
 * <p>
 * There are still several TODOs left in this class, some easily addressable, some more complex, but the support
 * is functional.
 *
 * @opensearch.internal
 */
public final class IndicesRequestCache implements TieredCacheEventListener<IndicesRequestCache.Key, BytesReference>, Closeable {

    private static final Logger logger = LogManager.getLogger(IndicesRequestCache.class);

    /**
     * A setting to enable or disable request caching on an index level. Its dynamic by default
     * since we are checking on the cluster state IndexMetadata always.
     */
    public static final Setting<Boolean> INDEX_CACHE_REQUEST_ENABLED_SETTING = Setting.boolSetting(
        "index.requests.cache.enable",
        true,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDICES_CACHE_QUERY_SIZE = Setting.memorySizeSetting(
        "indices.requests.cache.size",
        "1%",
        Property.NodeScope
    );
    public static final Setting<TimeValue> INDICES_CACHE_QUERY_EXPIRE = Setting.positiveTimeSetting(
        "indices.requests.cache.expire",
        new TimeValue(0),
        Property.NodeScope
    );

    public static final Setting<Boolean> INDICES_CACHE_DISK_TIER_ENABLED = Setting.boolSetting(
        "index.requests.cache.tiered.disk.enable",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> INDICES_CACHE_DISK_TIER_SIZE = Setting.memorySizeSetting(
        "index.requests.cache.tiered.disk.size",
        new ByteSizeValue(1, ByteSizeUnit.GB), // TODO: This is a random value - what should it be??
        Property.NodeScope
    );

    public static final Setting<Double> INDICES_CACHE_DISK_STALE_KEY_THRESHOLD = Setting.doubleSetting(
        "index.requests.cache.tiered.disk.stale_cleanup_threshold",
        0.5,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> INDICES_CACHE_KEYSTORE_SIZE = Setting.memorySizeSetting(
        "index.requests.cache.tiered.disk.keystore.size",
        "0.05%", // 5% of INDICES_CACHE_QUERY_SIZE
        Property.Dynamic,
        Property.NodeScope
    );

    private final ConcurrentMap<CleanupKey, Boolean> registeredClosedListeners = ConcurrentCollections.newConcurrentMap();
    private final Set<CleanupKey> keysToClean = ConcurrentCollections.newConcurrentSet();
    private final ByteSizeValue size;
    private final TimeValue expire;
    private final TieredCacheService<Key, BytesReference> tieredCacheService;
    private final IndicesService indicesService;
    private final Settings settings;

    IndicesRequestCache(Settings settings, IndicesService indicesService, ClusterSettings clusterSettings) {
        this.size = INDICES_CACHE_QUERY_SIZE.get(settings);
        this.expire = INDICES_CACHE_QUERY_EXPIRE.exists(settings) ? INDICES_CACHE_QUERY_EXPIRE.get(settings) : null;
        long sizeInBytes = size.getBytes();

        // Initialize onHeap cache tier first.
        OnHeapCachingTier<Key, BytesReference> openSearchOnHeapCache = new OpenSearchOnHeapCache.Builder<Key, BytesReference>().setWeigher(
            (k, v) -> k.ramBytesUsed() + v.ramBytesUsed()
        ).setMaximumWeight(sizeInBytes).setExpireAfterAccess(expire).build();

        Function<BytesReference, CachePolicyInfoWrapper> transformationFunction = (data) -> {
            try {
                return getPolicyInfo(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        // enabling this for testing purposes. Remove/tweak!!
        EhCacheDiskCachingTier<Key, BytesReference> ehcacheDiskTier = getDefaultDiskTier(settings);

        // Initialize tiered cache service. TODO: Enable Disk tier when tiered support is turned on.
        tieredCacheService = new TieredCacheSpilloverStrategyService.Builder<Key, BytesReference>().setOnHeapCachingTier(
            openSearchOnHeapCache
        )
            .setOnDiskCachingTier(ehcacheDiskTier)
            .setTieredCacheEventListener(this)
            .withPolicy(new DiskTierTookTimePolicy(settings, clusterSettings, transformationFunction))
            .build();
        this.indicesService = indicesService;
        this.settings = settings;
        clusterSettings.addSettingsUpdateConsumer(INDICES_CACHE_DISK_TIER_ENABLED, this::toggleDiskTierEnabled);
    }

    @Override
    public void close() {
        tieredCacheService.invalidateAll();
        if (tieredCacheService.getDiskCachingTier().isPresent()) {
            tieredCacheService.getDiskCachingTier().get().close();
        }
    }

    void clear(CacheEntity entity) {
        keysToClean.add(new CleanupKey(entity, null));
        cleanCache();
    }

    @Override
    public void onMiss(Key key, CacheValue<BytesReference> cacheValue) {
        key.entity.onMiss(cacheValue);
    }

    @Override
    public void onRemoval(RemovalNotification<Key, BytesReference> notification) {
        notification.getKey().entity.onRemoval(notification);
    }

    @Override
    public void onHit(Key key, CacheValue<BytesReference> cacheValue) {
        key.entity.onHit(cacheValue);
    }

    @Override
    public void onCached(Key key, BytesReference value, TierType tierType) {
        key.entity.onCached(key, value, tierType);
    }

    BytesReference getOrCompute(
        CacheEntity cacheEntity,
        CheckedSupplier<BytesReference, IOException> loader,
        DirectoryReader reader,
        BytesReference cacheKey
    ) throws Exception {
        assert reader.getReaderCacheHelper() != null;
        assert reader.getReaderCacheHelper() instanceof OpenSearchDirectoryReader.DelegatingCacheHelper;

        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper = (OpenSearchDirectoryReader.DelegatingCacheHelper) reader
            .getReaderCacheHelper();
        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
        assert readerCacheKeyId != null;
        final Key key = new Key(cacheEntity, cacheKey, readerCacheKeyId);
        Loader cacheLoader = new Loader(cacheEntity, loader);
        BytesReference value = tieredCacheService.computeIfAbsent(key, cacheLoader);
        if (cacheLoader.isLoaded()) {
            // see if its the first time we see this reader, and make sure to register a cleanup key
            CleanupKey cleanupKey = new CleanupKey(cacheEntity, readerCacheKeyId);
            if (!registeredClosedListeners.containsKey(cleanupKey)) {
                Boolean previous = registeredClosedListeners.putIfAbsent(cleanupKey, Boolean.TRUE);
                if (previous == null) {
                    OpenSearchDirectoryReader.addReaderCloseListener(reader, cleanupKey);
                }
            }
        }
        return value;
    }

    /**
     * Invalidates the given the cache entry for the given key and it's context
     * @param cacheEntity the cache entity to invalidate for
     * @param reader the reader to invalidate the cache entry for
     * @param cacheKey the cache key to invalidate
     */
    void invalidate(CacheEntity cacheEntity, DirectoryReader reader, BytesReference cacheKey) {
        assert reader.getReaderCacheHelper() != null;
        String readerCacheKeyId = null;
        if (reader instanceof OpenSearchDirectoryReader) {
            IndexReader.CacheHelper cacheHelper = ((OpenSearchDirectoryReader) reader).getDelegatingCacheHelper();
            readerCacheKeyId = ((OpenSearchDirectoryReader.DelegatingCacheHelper) cacheHelper).getDelegatingCacheKey().getId();
        }
        tieredCacheService.invalidate(new Key(cacheEntity, cacheKey, readerCacheKeyId));
    }

    public static CachePolicyInfoWrapper getPolicyInfo(BytesReference data) throws IOException {
        // Reads the policy info corresponding to this QSR, written in IndicesService$loadIntoContext,
        // without having to create a potentially large short-lived QSR object just for this purpose
        return new CachePolicyInfoWrapper(data.streamInput());
    }

    /**
     * Loader for the request cache
     *
     * @opensearch.internal
     */
    private static class Loader implements TieredCacheLoader<Key, BytesReference> {
        private final CacheEntity entity;
        private final CheckedSupplier<BytesReference, IOException> loader;
        private boolean loaded;

        Loader(CacheEntity entity, CheckedSupplier<BytesReference, IOException> loader) {
            this.entity = entity;
            this.loader = loader;
        }

        public boolean isLoaded() {
            return this.loaded;
        }

        @Override
        public BytesReference load(Key key) throws Exception {
            BytesReference value = loader.get();
            loaded = true;
            return value;
        }
    }

    /**
     * Basic interface to make this cache testable.
     */
    interface CacheEntity extends Accountable, Writeable {

        /**
         * Called after the value was loaded.
         */
        void onCached(Key key, BytesReference value, TierType tierType);

        /**
         * Returns <code>true</code> iff the resource behind this entity is still open ie.
         * entities associated with it can remain in the cache. ie. IndexShard is still open.
         */
        boolean isOpen();

        /**
         * Returns the cache identity. this is, similar to {@link #isOpen()} the resource identity behind this cache entity.
         * For instance IndexShard is the identity while a CacheEntity is per DirectoryReader. Yet, we group by IndexShard instance.
         */
        Object getCacheIdentity();

        /**
         * Called each time this entity has a cache hit.
         */
        void onHit(CacheValue<BytesReference> cacheValue);

        /**
         * Called each time this entity has a cache miss.
         */
        void onMiss(CacheValue<BytesReference> cacheValue);

        /**
         * Called when this entity instance is removed
         */
        void onRemoval(RemovalNotification<Key, BytesReference> notification);

    }

    /**
     * Unique key for the cache
     *
     * @opensearch.internal
     */
    class Key implements Accountable, Writeable {
        private final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Key.class);

        public final CacheEntity entity; // use as identity equality
        public final String readerCacheKeyId;
        public final BytesReference value;

        Key(CacheEntity entity, BytesReference value, String readerCacheKeyId) {
            this.entity = entity;
            this.value = value;
            this.readerCacheKeyId = Objects.requireNonNull(readerCacheKeyId);
        }

        Key(StreamInput in) throws IOException {
            this.entity = in.readOptionalWriteable(in1 -> indicesService.new IndexShardCacheEntity(in1));
            this.readerCacheKeyId = in.readOptionalString();
            this.value = in.readBytesReference();
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + entity.ramBytesUsed() + value.length();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            // TODO: more detailed ram usage?
            return Collections.emptyList();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            if (Objects.equals(readerCacheKeyId, key.readerCacheKeyId) == false) return false;
            if (!entity.getCacheIdentity().equals(key.entity.getCacheIdentity())) return false;
            if (!value.equals(key.value)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = entity.getCacheIdentity().hashCode();
            result = 31 * result + readerCacheKeyId.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(entity);
            out.writeOptionalString(readerCacheKeyId);
            out.writeBytesReference(value);
        }
    }

    private class CleanupKey implements IndexReader.ClosedListener {
        final CacheEntity entity;
        final String readerCacheKeyId;

        private CleanupKey(CacheEntity entity, String readerCacheKeyId) {
            this.entity = entity;
            this.readerCacheKeyId = readerCacheKeyId;
        }

        @Override
        public void onClose(IndexReader.CacheKey cacheKey) {
            Boolean remove = registeredClosedListeners.remove(this);
            if (remove != null) {
                keysToClean.add(this);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CleanupKey that = (CleanupKey) o;
            if (Objects.equals(readerCacheKeyId, that.readerCacheKeyId) == false) return false;
            if (!entity.getCacheIdentity().equals(that.entity.getCacheIdentity())) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = entity.getCacheIdentity().hashCode();
            result = 31 * result + Objects.hashCode(readerCacheKeyId);
            return result;
        }
    }

    /**
     * Logic to clean up in-memory cache.
     */
    synchronized void cleanCache() {
        final Set<CleanupKey> currentKeysToClean = new HashSet<>();
        final Set<Object> currentFullClean = new HashSet<>();
        currentKeysToClean.clear();
        currentFullClean.clear();
        for (Iterator<CleanupKey> iterator = keysToClean.iterator(); iterator.hasNext();) {
            CleanupKey cleanupKey = iterator.next();
            iterator.remove();
            if (cleanupKey.readerCacheKeyId == null || cleanupKey.entity.isOpen() == false) {
                // null indicates full cleanup, as does a closed shard
                currentFullClean.add(cleanupKey.entity.getCacheIdentity());
            } else {
                currentKeysToClean.add(cleanupKey);
            }
        }
        if (!currentKeysToClean.isEmpty() || !currentFullClean.isEmpty()) {
            for (Iterator<Key> iterator = tieredCacheService.getOnHeapCachingTier().keys().iterator(); iterator.hasNext();) {
                Key key = iterator.next();
                if (currentFullClean.contains(key.entity.getCacheIdentity())) {
                    iterator.remove();
                } else {
                    if (currentKeysToClean.contains(new CleanupKey(key.entity, key.readerCacheKeyId))) {
                        iterator.remove();
                    }
                }
            }
        }
        tieredCacheService.getOnHeapCachingTier().refresh();
    }

    /**
     * Returns the current size of the cache
     */
    long count() {
        return tieredCacheService.count();
    }

    int numRegisteredCloseListeners() { // for testing
        return registeredClosedListeners.size();
    }

    /**
     * Creates a new disk tier instance. Should only be run if the instance will actually be used!
     * @param settings The settings to use
     * @return A new disk tier instance
     */
    public EhCacheDiskCachingTier<Key, BytesReference> getDefaultDiskTier(Settings settings) {
        String SETTING_PREFIX = "indices.request.cache";
        long CACHE_SIZE_IN_BYTES = INDICES_CACHE_DISK_TIER_SIZE.get(settings).getBytes();
        String STORAGE_PATH = indicesService.getNodePaths()[0].indicesPath.toString() + "/request_cache";
        long keystoreMaxWeight = INDICES_CACHE_KEYSTORE_SIZE.get(settings).getBytes();

        EhCacheDiskCachingTier<Key, BytesReference> ehcacheDiskTier = new EhCacheDiskCachingTier.Builder<Key, BytesReference>()
            .setKeyType(Key.class)
            .setValueType(BytesReference.class)
            .setExpireAfterAccess(TimeValue.MAX_VALUE) // TODO: Is this meant to be the same as IRC expire or different?
            .setSettings(settings)
            .setThreadPoolAlias("ehcacheThreadpool")
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setStoragePath(STORAGE_PATH)
            .setSettingPrefix(SETTING_PREFIX)
            .setKeySerializer(new IRCKeyWriteableSerializer(this))
            .setValueSerializer(new BytesReferenceSerializer())
            .setKeyStoreMaxWeightInBytes(keystoreMaxWeight)
            .build();
        return ehcacheDiskTier;
    }

    /**
     * A function to run when disk tier enabled setting (IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED)
     * is enabled/disabled dynamically.
     * This function can't go in the TieredCacheSpilloverStrategyService because of issues with generics.
     * @param enableDiskTier the new value of the enable disk tier setting
     */
    private void toggleDiskTierEnabled(boolean enableDiskTier) {
        if (enableDiskTier && tieredCacheService.getDiskCachingTier().isEmpty()) {
            // If no disk tier existed before, create one now. Otherwise do nothing
            EhCacheDiskCachingTier<Key, BytesReference> diskCachingTier = getDefaultDiskTier(settings);
            tieredCacheService.addDiskTier(diskCachingTier);

        } else if (!enableDiskTier && tieredCacheService.getDiskCachingTier().isPresent()){
            // If a disk tier existed before, delete it (losing all the cached values). Otherwise do nothing
            tieredCacheService.removeDiskTier();
        }
    }
}
