/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
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
import org.opensearch.cache.common.tier.TieredSpilloverCacheTests;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Randomness;
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
import org.opensearch.indices.IndicesQueryCache;
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
import java.util.Random;

import static org.opensearch.cache.common.query.TieredQueryCache.SHARD_ID_DIMENSION_NAME;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.DISK_CACHE_ENABLED_SETTING_MAP;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_DISK;

public class TieredQueryCacheTests extends OpenSearchSingleNodeTestCase {

    private ThreadPool threadPool;

    static final String field = "field";

    private ThreadPool getThreadPool() {
        return new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "default tracer tests").build());
    }

    @After
    public void cleanup() throws IOException {
        terminate(threadPool);
    }

    // From IndicesQueryCacheTests

    public void testBasics_WithOpenSearchOnHeapCache() throws IOException {
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

        Settings settings = Settings.builder()
            .put(
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_QUERY_CACHE).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            )
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .build();
        TieredQueryCache cache = getQueryCache(settings);
        s.setQueryCache(cache);

        ICache<TieredQueryCache.CompositeKey, TieredQueryCache.CacheAndCount> innerCache = cache.getInnerCache();
        assertTrue(innerCache instanceof OpenSearchOnHeapCache);

        testBasicsDummyQuery(cache, s, shard);

        // TODO: not implementing shard closing logic for the PoC
        /*IOUtils.close(r, dir);

        // got emptied, but no changes to other metrics
        stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(20L, stats.getCacheCount());
        assertEquals(1L, stats.getHitCount());
        assertEquals(40L, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);

        cache.onClose(shard);

        // forgot everything
        stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(0L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(0L, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() >= 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);*/

        cache.close(); // this triggers some assertions
        IOUtils.close(r, dir);
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

        TieredQueryCache cache = getQueryCache(getTSCSettings(1000));
        s.setQueryCache(cache);

        ICache<TieredQueryCache.CompositeKey, TieredQueryCache.CacheAndCount> innerCache = cache.getInnerCache();
        assertTrue(innerCache instanceof TieredSpilloverCache);

        testBasicsDummyQuery(cache, s, shard);

        // Explicitly check disk cache had items and hits
        TieredSpilloverCache<TieredQueryCache.CompositeKey, TieredQueryCache.CacheAndCount> tsc = (TieredSpilloverCache<
            TieredQueryCache.CompositeKey,
            TieredQueryCache.CacheAndCount>) cache.getInnerCache();
        ImmutableCacheStats diskTierStats = TieredSpilloverCacheTests.getStatsSnapshotForTier(
            tsc,
            TIER_DIMENSION_VALUE_DISK,
            List.of(SHARD_ID_DIMENSION_NAME),
            List.of(shard.toString())
        );
        assertTrue(diskTierStats.getItems() > 0);
        assertTrue(diskTierStats.getHits() > 0);

        cache.close();
        IOUtils.close(r, dir);
    }

    private void testBasicsDummyQuery(TieredQueryCache cache, IndexSearcher s, ShardId shard) throws IOException {
        QueryCacheStats stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(0L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(0L, stats.getMissCount());
        assertEquals(0L, stats.getMemorySizeInBytes());

        assertEquals(1, s.count(new DummyQuery(0)));

        stats = cache.getStats(shard);
        assertEquals(1L, stats.getCacheSize());
        assertEquals(1L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(2L, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);

        int numEntries = 20;

        for (int i = 1; i < numEntries; ++i) {
            assertEquals(1, s.count(new DummyQuery(i)));
        }

        stats = cache.getStats(shard);
        // assertEquals(10L, stats.getCacheSize()); // TODO: this is 10 bc it's expecting evictions after 10.
        assertEquals(numEntries, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(2 * numEntries, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);

        s.count(new DummyQuery(1)); // Pick 1 so the hit comes from disk

        stats = cache.getStats(shard);
        // assertEquals(10L, stats.getCacheSize());
        assertEquals(numEntries, stats.getCacheCount());
        assertEquals(1L, stats.getHitCount());
        assertEquals(2 * numEntries, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);
    }

    // Modified from IndicesQueryCacheTests
    // Removed logic around closing shards as thats not yet implemented
    public void testTwoShards() throws IOException {
        threadPool = getThreadPool();
        Directory dir1 = newDirectory();
        IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
        w1.addDocument(new Document());
        DirectoryReader r1 = DirectoryReader.open(w1);
        w1.close();
        ShardId shard1 = new ShardId("index", "_na_", 0);
        r1 = OpenSearchDirectoryReader.wrap(r1, shard1);
        IndexSearcher s1 = new IndexSearcher(r1);
        s1.setQueryCachingPolicy(alwaysCachePolicy());

        Directory dir2 = newDirectory();
        IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig());
        w2.addDocument(new Document());
        DirectoryReader r2 = DirectoryReader.open(w2);
        w2.close();
        ShardId shard2 = new ShardId("index", "_na_", 1);
        r2 = OpenSearchDirectoryReader.wrap(r2, shard2);
        IndexSearcher s2 = new IndexSearcher(r2);
        s2.setQueryCachingPolicy(alwaysCachePolicy());

        TieredQueryCache cache = getQueryCache(getTSCSettings(1000));
        s1.setQueryCache(cache);
        s2.setQueryCache(cache);

        assertEquals(1, s1.count(new DummyQuery(0)));

        QueryCacheStats stats1 = cache.getStats(shard1);
        assertEquals(1L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());
        assertTrue(stats1.getMemorySizeInBytes() >= 0L && stats1.getMemorySizeInBytes() < Long.MAX_VALUE);

        QueryCacheStats stats2 = cache.getStats(shard2);
        assertEquals(0L, stats2.getCacheSize());
        assertEquals(0L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(0L, stats2.getMissCount());
        assertTrue(stats2.getMemorySizeInBytes() >= 0L && stats2.getMemorySizeInBytes() < Long.MAX_VALUE);

        assertEquals(1, s2.count(new DummyQuery(0)));

        stats1 = cache.getStats(shard1);
        assertEquals(1L, stats1.getCacheSize());
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());
        assertTrue(stats1.getMemorySizeInBytes() >= 0L && stats1.getMemorySizeInBytes() < Long.MAX_VALUE);

        stats2 = cache.getStats(shard2);
        assertEquals(1L, stats2.getCacheSize());
        assertEquals(1L, stats2.getCacheCount());
        assertEquals(0L, stats2.getHitCount());
        assertEquals(2L, stats2.getMissCount());
        assertTrue(stats2.getMemorySizeInBytes() >= 0L && stats2.getMemorySizeInBytes() < Long.MAX_VALUE);

        for (int i = 0; i < 20; ++i) {
            assertEquals(1, s2.count(new DummyQuery(i)));
        }

        stats1 = cache.getStats(shard1);
        //assertEquals(0L, stats1.getCacheSize()); // evicted
        assertEquals(1L, stats1.getCacheCount());
        assertEquals(0L, stats1.getHitCount());
        assertEquals(2L, stats1.getMissCount());
        assertTrue(stats1.getMemorySizeInBytes() >= 0L && stats1.getMemorySizeInBytes() < Long.MAX_VALUE);

        stats2 = cache.getStats(shard2);
        //assertEquals(10L, stats2.getCacheSize());
        assertEquals(20L, stats2.getCacheCount());
        assertEquals(1L, stats2.getHitCount());
        assertEquals(40L, stats2.getMissCount());
        assertTrue(stats2.getMemorySizeInBytes() >= 0L && stats2.getMemorySizeInBytes() < Long.MAX_VALUE);

        cache.close(); // this triggers some assertions
        IOUtils.close(r1, dir1, r2, dir2);
    }

    private void addRandomDocs(int numDocs, IndexWriter w, Random rand, int numDims, long lowerBound, long upperBound) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            Document d = new Document();
            long[] values = new long[numDims];
            for (int j = 0; j < numDims; j++) {
                values[j] = (long) rand.nextInt((int) upperBound);
            }
            d.add(new LongPoint(field, values));
            w.addDocument(d);
        }
    }

    public void testBasicsWithLongPointRangeQuery() throws Exception {
        threadPool = getThreadPool();
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        // TODO: Adding a bunch of random docs in a >1D space appears to mean Lucene can't search it sublinearly (>1D AND we have Relation.CELL_CROSSES_QUERY).
        // This means we DO actually use the query cache - sublinear queries are not cached for performance reasons since caching is O(N).
        // But, idk if this will be consistent. Need to learn more about how it decides where the BKD tree cells have their boundaries.
        addRandomDocs(1000, w, Randomness.get(), 2, 0, 100);
        DirectoryReader r = DirectoryReader.open(w);
        w.close();
        ShardId shard = new ShardId("index", "_na_", 0);
        r = OpenSearchDirectoryReader.wrap(r, shard);
        IndexSearcher s = new IndexSearcher(r);
        s.setQueryCachingPolicy(alwaysCachePolicy());

        TieredQueryCache cache = getQueryCache(getTSCSettings(1000));
        s.setQueryCache(cache);

        QueryCacheStats stats = cache.getStats(shard);
        assertEquals(0L, stats.getCacheSize());
        assertEquals(0L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(0L, stats.getMissCount());
        assertEquals(0L, stats.getMemorySizeInBytes());

        assertEquals(1, s.count(getRangeQuery(0, 2)));

        stats = cache.getStats(shard);
        assertEquals(1L, stats.getCacheSize());
        assertEquals(1L, stats.getCacheCount());
        assertEquals(0L, stats.getHitCount());
        assertEquals(2L, stats.getMissCount());
        assertTrue(stats.getMemorySizeInBytes() > 0L && stats.getMemorySizeInBytes() < Long.MAX_VALUE);
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

    private TieredQueryCache getQueryCache(Settings settings) throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            clusterService.getClusterSettings().registerSetting(DISK_CACHE_ENABLED_SETTING_MAP.get(CacheType.INDICES_QUERY_CACHE));
            return new TieredQueryCache(
                new CacheModule(List.of(new TieredSpilloverCachePlugin(settings), new MockDiskCachePlugin()), settings).getCacheService(),
                settings,
                clusterService,
                env
            );
        }
    }

    private Query getRangeQuery(long low, long high) {
        return LongPoint.newRangeQuery(field, new long[] { low, low+2 }, new long[] { high, high+2 });
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
