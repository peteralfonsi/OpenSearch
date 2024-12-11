/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.store.Directory;
import org.opensearch.cache.common.tier.MockDiskCache;
import org.opensearch.cache.common.tier.TieredSpilloverCache;
import org.opensearch.cache.common.tier.TieredSpilloverCachePlugin;
import org.opensearch.cache.common.tier.TieredSpilloverCacheSettings;
import org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder;
import org.opensearch.cache.common.tier.TieredSpilloverCacheTests;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.module.CacheModule;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.stats.ImmutableCacheStats;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.indices.query.DummyQuery;
import org.opensearch.indices.query.PluggableQueryCache;
import org.opensearch.node.Node;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.indices.query.PluggableQueryCache.SHARD_ID_DIMENSION_NAME;

public class PluggableQueryCacheTSCTests extends OpenSearchSingleNodeTestCase {

    private ThreadPool threadPool;

    private ThreadPool getThreadPool() {
        return new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "default tracer tests").build());
    }

    @After
    public void cleanup() throws IOException {
        terminate(threadPool);
    }

    public void testBasics_WithTSC_WithSmallHeapSize() throws Exception {
        // TODO: Check all the logic works when TSC is innerCache and can only fit a few keys into its heap tier (aka test the serializers
        // work.)
        threadPool = getThreadPool();
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(new Document());
        DirectoryReader r = DirectoryReader.open(w);
        w.close();
        ShardId shard = new ShardId("index", "_na_", 0);
        r = OpenSearchDirectoryReader.wrap(r, shard);
        IndexSearcher s = new IndexSearcher(r);
        s.setQueryCachingPolicy(alwaysCachePolicy());

        PluggableQueryCache cache = getQueryCache(getTSCSettings(1000));
        s.setQueryCache(cache);

        ICache<PluggableQueryCache.CompositeKey, PluggableQueryCache.CacheAndCount> innerCache = cache.getInnerCache();
        assertTrue(innerCache instanceof TieredSpilloverCache);

        testBasicsDummyQuery(cache, s, shard);

        // Explicitly check disk cache had items and hits
        TieredSpilloverCache<PluggableQueryCache.CompositeKey, PluggableQueryCache.CacheAndCount> tsc = (TieredSpilloverCache<
            PluggableQueryCache.CompositeKey,
            PluggableQueryCache.CacheAndCount>) cache.getInnerCache();
        ImmutableCacheStats diskTierStats = TieredSpilloverCacheTests.getStatsSnapshotForTier(
            tsc,
            TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_DISK,
            List.of(SHARD_ID_DIMENSION_NAME),
            List.of(shard.toString())
        );
        assertTrue(diskTierStats.getItems() > 0);
        assertTrue(diskTierStats.getHits() > 0);

        cache.close();
        IOUtils.close(r, dir);
    }

    // Duplicated from TieredQueryCacheTests.java
    private void testBasicsDummyQuery(PluggableQueryCache cache, IndexSearcher s, ShardId shard) throws IOException {
        checkStats(cache.getStats(shard), 0, 0, 0, 0, false);

        assertEquals(1, s.count(new DummyQuery(0)));
        checkStats(cache.getStats(shard), 1, 1, 0, 2, true);

        int numEntries = 20;
        for (int i = 1; i < numEntries; ++i) {
            assertEquals(1, s.count(new DummyQuery(i)));
        }
        checkStats(cache.getStats(shard), 10, numEntries, 0, 2 * numEntries, true);

        s.count(new DummyQuery(1)); // Pick 1 so the hit comes from disk
        checkStats(cache.getStats(shard), 10, numEntries, 1, 2 * numEntries, true);
    }

    private void checkStats(
        QueryCacheStats stats,
        long expectedSize,
        long expectedCount,
        long expectedHits,
        long expectedMisses,
        boolean checkMemoryAboveZero
    ) {
        // assertEquals(expectedSize, stats.getCacheSize());
        assertEquals(expectedCount, stats.getCacheCount());
        assertEquals(expectedHits, stats.getHitCount());
        assertEquals(expectedMisses, stats.getMissCount());
        if (checkMemoryAboveZero) {
            assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);
        }
    }

    private Settings getTSCSettings(int heapBytes) {
        return Settings.builder()
            .put(
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_QUERY_CACHE).getKey(),
                TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_QUERY_CACHE.getSettingPrefix()
                ).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_QUERY_CACHE.getSettingPrefix()
                ).getKey(),
                MockDiskCache.MockDiskCacheFactory.NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_SIZE.getConcreteSettingForNamespace(
                    CacheType.INDICES_QUERY_CACHE.getSettingPrefix()
                ).getKey(),
                heapBytes + "b"
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_SEGMENTS.getConcreteSettingForNamespace(
                    CacheType.INDICES_QUERY_CACHE.getSettingPrefix()
                ).getKey(),
                1
            )
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .build();
    }

    private static QueryCachingPolicy alwaysCachePolicy() {
        return new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {}

            @Override
            public boolean shouldCache(Query query) {
                return true;
            }
        };
    }

    private PluggableQueryCache getQueryCache(Settings settings) throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            clusterService.getClusterSettings()
                .registerSetting(TieredSpilloverCacheSettings.DISK_CACHE_ENABLED_SETTING_MAP.get(CacheType.INDICES_QUERY_CACHE));
            return new PluggableQueryCache(
                new CacheModule(List.of(new TieredSpilloverCachePlugin(settings), new MockDiskCachePlugin()), settings).getCacheService(),
                settings,
                clusterService,
                env
            );
        }
    }

    // Duplicated from TieredSpilloverCacheIT.java
    public static class MockDiskCachePlugin extends Plugin implements CachePlugin {

        public MockDiskCachePlugin() {}

        @Override
        public Map<String, ICache.Factory> getCacheFactoryMap() {
            return Map.of(MockDiskCache.MockDiskCacheFactory.NAME, new MockDiskCache.MockDiskCacheFactory(0, 10000, false, 1));
        }

        @Override
        public String getName() {
            return "mock_disk_plugin";
        }
    }
}
