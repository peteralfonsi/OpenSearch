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

import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.tier.DiskTierTookTimePolicy;
import org.opensearch.common.cache.tier.TierType;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.time.Duration;
import java.time.Instant;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

// This is a separate file from IndicesRequestCacheIT because we only want to run our test
// on a node with a maximum request cache size that we set.
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndicesRequestCacheDiskTierIT extends OpenSearchIntegTestCase {
    public void testDiskTierStats() throws Exception {
        int heapSizeBytes = 9876;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), new ByteSizeValue(heapSizeBytes))
                .put(DiskTierTookTimePolicy.DISK_TOOKTIME_THRESHOLD_SETTING.getKey(), TimeValue.ZERO) // allow into disk cache regardless of took time
        );
        Client client = client(node);

        Settings.Builder indicesSettingBuilder = Settings.builder()
            .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        assertAcked(
            client.admin().indices().prepareCreate("index").setMapping("k", "type=keyword").setSettings(indicesSettingBuilder).get()
        );
        indexRandom(true, client.prepareIndex("index").setSource("k", "hello"));
        ensureSearchable("index");
        SearchResponse resp;

        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + 0)).get();
        int requestSize = (int) getCacheSizeBytes(client, "index", TierType.ON_HEAP);
        assertTrue(heapSizeBytes > requestSize);
        // If this fails, increase heapSizeBytes! We can't adjust it after getting the size of one query
        // as the cache size setting is not dynamic

        int numOnDisk = 5;
        int numRequests = heapSizeBytes / requestSize + numOnDisk;
        System.out.println("Size " + requestSize + " numRequests " + numRequests);
        for (int i = 1; i < numRequests; i++) {
            resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + i)).get();
            assertSearchResponse(resp);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.ON_HEAP, false);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.DISK, false);
        }

        // So far, disk-specific stats should be 0, as keystore has prevented any actual disk reaches
        long tookTimeSoFar = assertDiskTierSpecificStats(client, "index", 0, -1, 0);

        // the first request, for "hello0", should have been evicted to the disk tier
        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello0")).get();
        IndicesRequestCacheIT.assertCacheState(client, "index", 0, numRequests + 1, TierType.ON_HEAP, false);
        IndicesRequestCacheIT.assertCacheState(client, "index", 1, numRequests, TierType.DISK, false);
        tookTimeSoFar = assertDiskTierSpecificStats(client, "index", 1, 0, -1);

        // We make another actual request that should be in the disk tier. Disk specific stats should keep incrementing
        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello1")).get();
        IndicesRequestCacheIT.assertCacheState(client, "index", 0,  numRequests + 2, TierType.ON_HEAP, false);
        IndicesRequestCacheIT.assertCacheState(client, "index", 2, numRequests, TierType.DISK, false);
        tookTimeSoFar = assertDiskTierSpecificStats(client, "index", 2, tookTimeSoFar, -1);

        // A final request for something in neither tier shouldn't increment disk specific stats
        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + numRequests)).get();
        IndicesRequestCacheIT.assertCacheState(client, "index", 0,  numRequests + 3, TierType.ON_HEAP, false);
        IndicesRequestCacheIT.assertCacheState(client, "index", 2, numRequests + 1, TierType.DISK, false);
        assertDiskTierSpecificStats(client, "index", 2, tookTimeSoFar, tookTimeSoFar);
    }

    public void testDiskTierInvalidationByCleanCacheAPI() throws Exception {
        int cleanupIntervalInMillis = 10_000_000; // setting this intentionally high so that we don't get background cleanups
        int heapSizeBytes = 9876;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesService.INDICES_REQUEST_CACHE_DISK_CLEAN_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(cleanupIntervalInMillis))
                .put(IndicesService.INDICES_REQUEST_CACHE_DISK_CLEAN_THRESHOLD_SETTING.getKey(), "0%")
                .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), new ByteSizeValue(heapSizeBytes))
                .put(DiskTierTookTimePolicy.DISK_TOOKTIME_THRESHOLD_SETTING.getKey(), TimeValue.ZERO) // allow into disk cache regardless of took time
        );
        Client client = client(node);

        Settings.Builder indicesSettingBuilder = Settings.builder()
            .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        assertAcked(
            client.admin().indices().prepareCreate("index").setMapping("k", "type=keyword").setSettings(indicesSettingBuilder).get()
        );

        indexRandom(true, client.prepareIndex("index").setSource("k", "hello"));
        ensureSearchable("index");
        SearchResponse resp;

        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + 0)).get();
        int requestSize = (int) getCacheSizeBytes(client, "index", TierType.ON_HEAP);
        assertTrue(heapSizeBytes > requestSize);
        // If this fails, increase heapSizeBytes! We can't adjust it after getting the size of one query
        // as the cache size setting is not dynamic
        int numOnDisk = 2;
        int numRequests = heapSizeBytes / requestSize + numOnDisk;
        RequestCacheStats requestCacheStats = client.admin()
            .indices()
            .prepareStats("index")
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();
        for (int i = 1; i < numRequests; i++) {
            requestCacheStats = client.admin()
                .indices()
                .prepareStats("index")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache();
            resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + i)).get();
            assertSearchResponse(resp);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.ON_HEAP, false);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.DISK, false);
        }

        requestCacheStats = client.admin()
            .indices()
            .prepareStats("index")
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();

        long entries = requestCacheStats.getEntries(TierType.DISK);
        // make sure we have 2 entries in disk.
        assertEquals(2, entries);

        // call clear cache api
        client.admin().indices().prepareClearCache().setIndices("index").setRequestCache(true).get();
        // fetch the stats again
        requestCacheStats = client.admin()
            .indices()
            .prepareStats("index")
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();
        entries = requestCacheStats.getEntries(TierType.DISK);
        // make sure we have 0 entries in disk.
        assertEquals(0, entries);
    }

    // When entire disk tier is stale, test whether cache cleaner cleans up everything from disk
    public void testDiskTierInvalidationByCacheCleanerEntireDiskTier() throws Exception {
        int thresholdInMillis = 4_000;
        Instant start = Instant.now();
        int heapSizeBytes = 9876;
        String node = internalCluster().startNode(
        Settings.builder()
            .put(IndicesService.INDICES_REQUEST_CACHE_DISK_CLEAN_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(thresholdInMillis))
            .put(IndicesService.INDICES_REQUEST_CACHE_DISK_CLEAN_THRESHOLD_SETTING.getKey(), "1%")
            .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), new ByteSizeValue(heapSizeBytes))
            .put(DiskTierTookTimePolicy.DISK_TOOKTIME_THRESHOLD_SETTING.getKey(), TimeValue.ZERO) // allow into disk cache regardless of took time
        );
        Client client = client(node);

        Settings.Builder indicesSettingBuilder = Settings.builder()
            .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        assertAcked(
            client.admin().indices().prepareCreate("index").setMapping("k", "type=keyword").setSettings(indicesSettingBuilder).get()
        );

        indexRandom(true, client.prepareIndex("index").setSource("k", "hello"));
        ensureSearchable("index");
        SearchResponse resp;

        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + 0)).get();
        int requestSize = (int) getCacheSizeBytes(client, "index", TierType.ON_HEAP);
        assertTrue(heapSizeBytes > requestSize);

        int numOnDisk = 2;
        int numRequests = heapSizeBytes / requestSize + numOnDisk;
        for (int i = 1; i < numRequests; i++) {
            resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + i)).get();
            assertSearchResponse(resp);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.ON_HEAP, false);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.DISK, false);
        }

        RequestCacheStats requestCacheStats = client.admin()
            .indices()
            .prepareStats("index")
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();

        // make sure we have 2 entries in disk.
         long entries = requestCacheStats.getEntries(TierType.DISK);
        assertEquals(2, entries);

        // index a doc and force refresh so that the cache cleaner can clean the cache
        indexRandom(true, client.prepareIndex("index").setSource("k", "hello"));
        ensureSearchable("index");

        // sleep for the threshold time, so that the cache cleaner can clean the cache
        Instant end = Instant.now();
        long elapsedTimeMillis = Duration.between(start, end).toMillis();
        // if this test is flaky, increase the sleep time.
        long sleepTime = (thresholdInMillis - elapsedTimeMillis) + 1_000;
        Thread.sleep(sleepTime);

        // by now cache cleaner would have run and cleaned up stale keys
        // fetch the stats again
        requestCacheStats = client.admin()
            .indices()
            .prepareStats("index")
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();

        // make sure we have 0 entries in disk.
        entries = requestCacheStats.getEntries(TierType.DISK);
        assertEquals(0, entries);
    }

    // When part of disk tier is stale, test whether cache cleaner cleans up only stale items from disk
    public void testDiskTierInvalidationByCacheCleanerPartOfDiskTier() throws Exception {
        int thresholdInMillis = 4_000;
        Instant start = Instant.now();
        int heapSizeBytes = 987;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesService.INDICES_REQUEST_CACHE_DISK_CLEAN_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(thresholdInMillis))
                .put(IndicesService.INDICES_REQUEST_CACHE_DISK_CLEAN_THRESHOLD_SETTING.getKey(), "1%")
                .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), new ByteSizeValue(heapSizeBytes))
                .put(DiskTierTookTimePolicy.DISK_TOOKTIME_THRESHOLD_SETTING.getKey(), TimeValue.ZERO) // allow into disk cache regardless of took time
        );
        Client client = client(node);

        Settings.Builder indicesSettingBuilder = Settings.builder()
            .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        assertAcked(
            client.admin().indices().prepareCreate("index").setMapping("k", "type=text").setSettings(indicesSettingBuilder).get()
        );

        indexRandom(true, client.prepareIndex("index").setSource("k", "hello"));
        ensureSearchable("index");
        SearchResponse resp;

        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + 0)).get();
        int requestSize = (int) getCacheSizeBytes(client, "index", TierType.ON_HEAP);
        assertTrue(heapSizeBytes > requestSize);

        int numOnDisk = 2;
        int numRequests = heapSizeBytes / requestSize + numOnDisk;
        for (int i = 1; i < numRequests; i++) {
            resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + i)).get();
            assertSearchResponse(resp);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.ON_HEAP, false);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.DISK, false);
        }

        RequestCacheStats requestCacheStats = client.admin()
            .indices()
            .prepareStats("index")
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();

        // make sure we have 2 entries in disk.
        long entries = requestCacheStats.getEntries(TierType.DISK);
        assertEquals(2, entries);

        // force refresh so that it creates stale keys in the cache for the cache cleaner to pick up.
        flushAndRefresh("index");
        client().prepareIndex("index").setId("1").setSource("k", "good bye");
        ensureSearchable("index");

        for (int i = 0; i < 6; i++) { // index 5 items with the new readerCacheKeyId
            client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + i)).get();
        }

        // fetch the stats again
        requestCacheStats = client.admin()
            .indices()
            .prepareStats("index")
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();

        entries = requestCacheStats.getEntries(TierType.DISK);

        // sleep for the threshold time, so that the cache cleaner can clean the cache
        Instant end = Instant.now();
        long elapsedTimeMillis = Duration.between(start, end).toMillis();
        // if this test is flaky, increase the sleep time.
        long sleepTime = (thresholdInMillis - elapsedTimeMillis) + 5_000;
        Thread.sleep(sleepTime);

        // fetch the stats again
        requestCacheStats = client.admin()
            .indices()
            .prepareStats("index")
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();

        // make sure we have 5 entries in disk.
        entries = requestCacheStats.getEntries(TierType.DISK);
        assertEquals(5, entries);
    }

    private long getCacheSizeBytes(Client client, String index, TierType tierType) {
        RequestCacheStats requestCacheStats = client.admin()
            .indices()
            .prepareStats(index)
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();
        return requestCacheStats.getMemorySizeInBytes(tierType);
    }

    private long assertDiskTierSpecificStats(Client client, String index, long totalDiskReaches, long totalGetTimeLowerBound, long totalGetTimeUpperBound) {
        // set bounds to -1 to ignore them
        RequestCacheStats requestCacheStats = client.admin()
            .indices()
            .prepareStats(index)
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();
        ShardRequestCache.DiskStatsAccumulator specStats = requestCacheStats.getDiskSpecificStats();
        assertEquals(totalDiskReaches, specStats.getTotalDiskReaches());
        long tookTime = specStats.getTotalGetTime();
        assertTrue(tookTime >= totalGetTimeLowerBound || totalGetTimeLowerBound < 0);
        assertTrue(tookTime <= totalGetTimeUpperBound || totalGetTimeUpperBound < 0);
        return tookTime; // Return for use in next check
    }
}
