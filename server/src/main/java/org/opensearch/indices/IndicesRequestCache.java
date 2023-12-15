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
import org.opensearch.common.cache.tier.CachingTier;
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
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;


import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final ConcurrentMap<CleanupKey, Boolean> registeredClosedListeners = ConcurrentCollections.newConcurrentMap();
    private final Map<CleanupKey, CleanupStatus> keysToClean = ConcurrentCollections.newConcurrentMap();
    // A map to keep track of the number of keys to be cleaned for a given ShardId and readerCacheKeyId
    private final ConcurrentMap<ShardId, ConcurrentMap<String , Integer>> diskCleanupKeyToCountMap = ConcurrentCollections.newConcurrentMap();
    private final AtomicInteger staleKeysInDiskCount = new AtomicInteger(0);
    private final ByteSizeValue size;
    private final TimeValue expire;
    private final TieredCacheService<Key, BytesReference> tieredCacheService;
    private final IndicesService indicesService;

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
        long CACHE_SIZE_IN_BYTES = 1000000L;
        String SETTING_PREFIX = "indices.request.cache";
        String STORAGE_PATH = indicesService.getNodePaths()[0].indicesPath.toString() + "/request_cache";

        double diskTierKeystoreWeightFraction = 0.05; // Allocate 5% of the on-heap weight to the disk tier's keystore
        long keystoreMaxWeight = (long) (diskTierKeystoreWeightFraction * INDICES_CACHE_QUERY_SIZE.get(settings).getBytes());

        EhCacheDiskCachingTier<Key, BytesReference> ehcacheDiskTier = new EhCacheDiskCachingTier.Builder<Key, BytesReference>().setKeyType(
            Key.class
        )
            .setValueType(BytesReference.class)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setSettings(settings)
            .setThreadPoolAlias("ehcacheTest")
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setStoragePath(STORAGE_PATH)
            .setSettingPrefix(SETTING_PREFIX)
            .setKeySerializer(new IRCKeyWriteableSerializer(this))
            .setValueSerializer(new BytesReferenceSerializer())
            .setKeyStoreMaxWeightInBytes(keystoreMaxWeight)
            .build();

        // Initialize tiered cache service. TODO: Enable Disk tier when tiered support is turned on.
        tieredCacheService = new TieredCacheSpilloverStrategyService.Builder<Key, BytesReference>().setOnHeapCachingTier(
            openSearchOnHeapCache
        )
            .setOnDiskCachingTier(ehcacheDiskTier)
            .setTieredCacheEventListener(this)
            .withPolicy(new DiskTierTookTimePolicy(settings, clusterSettings, transformationFunction))
            .build();

        this.indicesService = indicesService;
    }

    // added for testing
    IndicesRequestCache(
        Settings settings,
        IndicesService indicesService,
        TieredCacheService<Key, BytesReference> tieredCacheService
    ) {
        this.size = INDICES_CACHE_QUERY_SIZE.get(settings);
        this.expire = INDICES_CACHE_QUERY_EXPIRE.exists(settings) ? INDICES_CACHE_QUERY_EXPIRE.get(settings) : null;
        long sizeInBytes = size.getBytes();
        this.indicesService = indicesService;
        this.tieredCacheService = tieredCacheService;
    }

    @Override
    public void close() {
        tieredCacheService.invalidateAll();
        if (tieredCacheService.getDiskCachingTier().isPresent()) {
            tieredCacheService.getDiskCachingTier().get().close();
        }
    }

    void clear(CacheEntity entity) {
        CleanupKey cleanupKey = new CleanupKey(entity, null);
        keysToClean.put(cleanupKey, new CleanupStatus());
        updateStaleKeysInDiskCount(cleanupKey);
        cleanCache();
        /*
        this would be triggered by the cache clear API call
        we need to make sure we clean the disk cache as well
        hence passing threshold as 0
        */
        cleanDiskCache(0);
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
        updateDiskCleanupKeyToCountMap(new CleanupKey(key.entity, key.readerCacheKeyId), tierType);
    }

    /**
     * Updates the diskCleanupKeyToCountMap with the given CleanupKey and TierType.
     * If the TierType is not DISK, the method returns without making any changes.
     * If the ShardId associated with the CleanupKey does not exist in the map, a new entry is created.
     * The method increments the count of the CleanupKey in the map.
     *
     * Why use ShardID as the key ?
     * CacheEntity mainly contains IndexShard, both of these classes do not override equals() and hashCode() methods.
     * ShardID class properly overrides equals() and hashCode() methods.
     * Therefore, to avoid modifying CacheEntity and IndexShard classes to override these methods, we use ShardID as the key.
     *
     * @param cleanupKey the CleanupKey to be updated in the map
     * @param tierType the TierType of the CleanupKey
     */
    private void updateDiskCleanupKeyToCountMap(CleanupKey cleanupKey, TierType tierType) {
        if(!tierType.equals(TierType.DISK)) {
            return;
        }
        IndexShard indexShard = (IndexShard)cleanupKey.entity.getCacheIdentity();
        if(indexShard == null) {
            logger.warn("IndexShard is null for CleanupKey: {} while cleaning tier : {}",
                cleanupKey.readerCacheKeyId, tierType.getStringValue());
            return;
        }
        ShardId shardId = indexShard.shardId();

        diskCleanupKeyToCountMap
            .computeIfAbsent(shardId, k -> ConcurrentCollections.newConcurrentMap())
            .merge(cleanupKey.readerCacheKeyId, 1, Integer::sum);
    }

    /**
     * Updates the count of stale keys in the disk cache.
     * This method is called when a CleanupKey is added to the keysToClean set.
     * It increments the staleKeysInDiskCount by the count of the CleanupKey in the diskCleanupKeyToCountMap.
     * If the CleanupKey's readerCacheKeyId is null or the CleanupKey's entity is not open, it increments the staleKeysInDiskCount
     * by the total count of keys associated with the CleanupKey's ShardId in the diskCleanupKeyToCountMap and removes the ShardId from the map.
     *
     * @param cleanupKey the CleanupKey that has been marked for cleanup
     */
    private void updateStaleKeysInDiskCount(CleanupKey cleanupKey) {
        IndexShard indexShard = (IndexShard) cleanupKey.entity.getCacheIdentity();
        if(indexShard == null) {
            logger.warn("IndexShard is null for CleanupKey: {}",  cleanupKey.readerCacheKeyId);
            return;
        }
        ShardId shardId = indexShard.shardId();

        ConcurrentMap<String, Integer> countMap = diskCleanupKeyToCountMap.get(shardId);
        if (countMap == null) {
            return;
        }

        if (cleanupKey.readerCacheKeyId == null) {
            int totalSum = countMap.values().stream().mapToInt(Integer::intValue).sum();
            staleKeysInDiskCount.addAndGet(totalSum);
            diskCleanupKeyToCountMap.remove(shardId);
            return;
        }
        Integer count = countMap.get(cleanupKey.readerCacheKeyId);
        if (count == null) {
            return;
        }
        staleKeysInDiskCount.addAndGet(count);
        countMap.remove(cleanupKey.readerCacheKeyId);
        if (countMap.isEmpty()) {
            diskCleanupKeyToCountMap.remove(shardId);
        }
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
            // see if it's the first time we see this reader, and make sure to register a cleanup key
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
     *
     * @param cacheEntity the cache entity to invalidate for
     * @param reader      the reader to invalidate the cache entry for
     * @param cacheKey    the cache key to invalidate
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

    public class CleanupStatus {
        public boolean cleanedInHeap;
        public boolean cleanedOnDisk;
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
                keysToClean.put(this, new CleanupStatus());
                updateStaleKeysInDiskCount(this);
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
        cleanCache(TierType.ON_HEAP);
        tieredCacheService.getOnHeapCachingTier().refresh();
    }

    /**
     * Logic to clean up disk based cache.
     * <p>
     * TODO: cleanDiskCache is very specific to disk caching tier. We can refactor this to be more generic.
     */
    synchronized void cleanDiskCache(double diskCachesCleanThresholdPercent) {
        if (!canSkipDiskCacheCleanup(diskCachesCleanThresholdPercent)) {
            cleanCache(TierType.DISK);
        }
    }

    private synchronized void cleanCache(TierType cacheType) {
        final Set<CleanupKey> currentKeysToClean = new HashSet<>();
        final Set<Object> currentFullClean = new HashSet<>();

            /*
            Stores the keys that need to be removed from keysToClean
            This is done to avoid ConcurrentModificationException
            */
        final Set<CleanupKey> keysCleanedFromAllCaches = new HashSet<>();

        for (Map.Entry<CleanupKey, CleanupStatus> entry : keysToClean.entrySet()) {
            CleanupKey cleanupKey = entry.getKey();
            CleanupStatus cleanupStatus = entry.getValue();

            if (cleanupStatus.cleanedInHeap && cleanupStatus.cleanedOnDisk) {
                keysCleanedFromAllCaches.add(cleanupKey);
                continue;
            }

            if (cacheType == TierType.ON_HEAP && cleanupStatus.cleanedInHeap) continue;
            if (cacheType == TierType.DISK && cleanupStatus.cleanedOnDisk) continue;

            if (needsFullClean(cleanupKey)) {
                currentFullClean.add(cleanupKey.entity.getCacheIdentity());
            } else {
                currentKeysToClean.add(cleanupKey);
            }

            if (cacheType == TierType.ON_HEAP) {
                cleanupStatus.cleanedInHeap = true;
            } else if (cacheType == TierType.DISK) {
                cleanupStatus.cleanedOnDisk = true;
            }
        }

        // Remove keys that have been cleaned from all caches
        keysToClean.keySet().removeAll(keysCleanedFromAllCaches);

        // Early exit if no cleanup is needed
        if (currentKeysToClean.isEmpty() && currentFullClean.isEmpty()) {
            return;
        }

        CachingTier<Key, BytesReference> cachingTier;

        if (cacheType == TierType.ON_HEAP) {
            cachingTier = tieredCacheService.getOnHeapCachingTier();
        } else {
            cachingTier = tieredCacheService.getDiskCachingTier().get();
        }

        cleanUpKeys(
            cachingTier,
            currentKeysToClean,
            currentFullClean
        );
    }

    private synchronized boolean canSkipDiskCacheCleanup(double diskCachesCleanThresholdPercent) {
        if (tieredCacheService.getDiskCachingTier().isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping disk cache keys cleanup since disk caching tier is not present");
            }
            return true;
        }
        if (tieredCacheService.getDiskCachingTier().get().count() == 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping disk cache keys cleanup since disk caching tier is empty");
            }
            return true;
        }
        if (diskCleanupKeysPercentage() < diskCachesCleanThresholdPercent) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping disk cache keys cleanup since the percentage of stale keys in disk cache is less than the threshold");
            }
            return true;
        }
        return false;
    }

    synchronized double diskCleanupKeysPercentage() {
        int totalKeysInDiskCache = tieredCacheService.getDiskCachingTier()
            .map(CachingTier::count)
            .orElse(0);
        if (totalKeysInDiskCache == 0 || staleKeysInDiskCount.get() == 0) {
            return 0;
        }
        return ((double) staleKeysInDiskCount.get() / totalKeysInDiskCache);
    }

    synchronized void cleanUpKeys(
        CachingTier<Key, BytesReference> cachingTier,
        Set<CleanupKey> currentKeysToClean,
        Set<Object> currentFullClean
    ) {
        for (Key key : cachingTier.keys()) {
            CleanupKey cleanupKey = new CleanupKey(key.entity, key.readerCacheKeyId);
            if (currentFullClean.contains(key.entity.getCacheIdentity()) || currentKeysToClean.contains(cleanupKey)) {
                cachingTier.invalidate(key);
                if(cachingTier.getTierType() == TierType.DISK) {
                    staleKeysInDiskCount.decrementAndGet();
                }
            }
        }
    }

    private boolean needsFullClean(CleanupKey cleanupKey) {
        // null indicates full cleanup, as does a closed shard
        return cleanupKey.readerCacheKeyId == null || !cleanupKey.entity.isOpen();
    }

    /**
     * Returns the current size of the cache
     */
    long count() {
        return tieredCacheService.count();
    }

    // to be used for testing only
    int numRegisteredCloseListeners() { // for testing
        return registeredClosedListeners.size();
    }

    // to be used for testing only
    void addCleanupKeyForTesting(CacheEntity entity, String readerCacheKeyId) { // for testing
        keysToClean.put(new CleanupKey(entity, readerCacheKeyId), new CleanupStatus());
    }

    // to be used for testing only
    int getKeysToCleanSizeForTesting() { // for testing
        return keysToClean.size();
    }

    // to be used for testing only
    Key createKeyForTesting(CacheEntity entity, String readerCacheKeyId) { // for testing
        return new Key(entity, null, readerCacheKeyId);
    }

    // to be used for testing only
    void setStaleKeysInDiskCountForTesting(int count) {
        staleKeysInDiskCount.set(count);
    }
}
