/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.store.Directory;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Randomness;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.module.CacheModule;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.node.Node;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class PluggableQueryCacheTests extends OpenSearchSingleNodeTestCase {

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
        PluggableQueryCache cache = getQueryCache(settings);
        s.setQueryCache(cache);

        ICache<PluggableQueryCache.CompositeKey, PluggableQueryCache.CacheAndCount> innerCache = cache.getInnerCache();
        assertTrue(innerCache instanceof OpenSearchOnHeapCache);

        testBasicsDummyQuery(cache, s, shard);

        cache.close(); // this triggers some assertions
        IOUtils.close(r, dir);
    }

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

        PluggableQueryCache cache = getQueryCache(getOnHeapSettings(1000));
        s1.setQueryCache(cache);
        s2.setQueryCache(cache);

        assertEquals(1, s1.count(new DummyQuery(0)));
        checkStats(cache.getStats(shard1), 1, 1, 0, 2, true);
        checkStats(cache.getStats(shard2), 0, 0, 0, 0, false);

        assertEquals(1, s2.count(new DummyQuery(0)));
        checkStats(cache.getStats(shard1), 1, 1, 0, 2, true);
        checkStats(cache.getStats(shard2), 1, 1, 0, 2, true);

        for (int i = 0; i < 20; ++i) {
            assertEquals(1, s2.count(new DummyQuery(i)));
        }
        checkStats(cache.getStats(shard1), 0, 1, 0, 2, false);
        checkStats(cache.getStats(shard2), 10, 20, 1, 40, true);

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
        // TODO: Adding a bunch of random docs in a >1D space appears to mean Lucene can't search it sublinearly (>1D AND we have
        // Relation.CELL_CROSSES_QUERY).
        // This means we DO actually use the query cache - sublinear queries are not cached for performance reasons since caching is O(N).
        // But, idk if this will be consistent. Need to learn more about how it decides where the BKD tree cells have their boundaries.
        addRandomDocs(1000, w, Randomness.get(), 2, 0, 100);
        Document d = new Document();
        d.add(new LongPoint(field, 1, 2)); // this one will always match test query
        w.addDocument(d);
        w.forceMerge(1); // Force merge down to 1 segment, so we only have one CacheHelper key and can accurately predict stats.
        DirectoryReader r = DirectoryReader.open(w);
        w.close();
        ShardId shard = new ShardId("index", "_na_", 0);
        r = OpenSearchDirectoryReader.wrap(r, shard);
        IndexSearcher s = new IndexSearcher(r);
        s.setQueryCachingPolicy(alwaysCachePolicy());

        PluggableQueryCache cache = getQueryCache(getOnHeapSettings(100_000));
        s.setQueryCache(cache);

        checkStats(cache.getStats(shard), 0, 0, 0, 0, false);

        assertTrue(s.count(getRangeQuery(0, 2)) >= 1);
        checkStats(cache.getStats(shard), 1, 1, 0, 2, true);

        int numEntries = 20;
        for (int i = 1; i < numEntries; ++i) {
            s.count(getRangeQuery(i, i + 2)); // TODO: this will be flaky as maybe some queries get lucky and are all contained in a BKD
                                              // cell -> skip query cache.
        }
        checkStats(cache.getStats(shard), 10, numEntries, 0, 2 * numEntries, true);

        s.count(getRangeQuery(0, 2)); // The original document should always be present
        checkStats(cache.getStats(shard), 10, numEntries, 1, 2 * numEntries, true);

        cache.close(); // this triggers some assertions
        IOUtils.close(r, dir);
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

    private Settings getOnHeapSettings(int maxBytes) {
        return Settings.builder()
            .put(
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_QUERY_CACHE).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            )
            .put(OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY, maxBytes)
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .build();
    }

    private PluggableQueryCache getQueryCache(Settings settings) throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            // clusterService.getClusterSettings().registerSetting(TieredSpilloverCacheSettings.DISK_CACHE_ENABLED_SETTING_MAP.get(CacheType.INDICES_QUERY_CACHE));
            return new PluggableQueryCache(
                // new CacheModule(List.of(new TieredSpilloverCachePlugin(settings), new MockDiskCachePlugin()),
                // settings).getCacheService(),
                new CacheModule(List.of(), settings).getCacheService(),
                settings,
                clusterService,
                env
            );
        }
    }

    private Query getRangeQuery(long low, long high) {
        return LongPoint.newRangeQuery(field, new long[] { low, low + 2 }, new long[] { high, high + 2 });
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
}