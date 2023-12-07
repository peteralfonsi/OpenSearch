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
import org.opensearch.common.cache.tier.keystore.RBMIntKeyLookupStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

// This is a separate file from IndicesRequestCacheIT because we only want to run our test
// on a node with a maximum request cache size that we set.

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndicesRequestCacheDiskTierIT extends OpenSearchIntegTestCase {
    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings())
            .put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true")
            .put(FeatureFlags.TIERED_CACHING, "true")
            .build();
    }
    public void testDiskTierStats() throws Exception {
        int heapSizeBytes = 9876;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), new ByteSizeValue(heapSizeBytes))
                .put(DiskTierTookTimePolicy.DISK_TOOKTIME_THRESHOLD_SETTING.getKey(), TimeValue.ZERO) // allow into disk cache regardless of
                                                                                                      // took time
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
        IndicesRequestCacheIT.assertCacheState(client, "index", 0, numRequests + 2, TierType.ON_HEAP, false);
        IndicesRequestCacheIT.assertCacheState(client, "index", 2, numRequests, TierType.DISK, false);
        tookTimeSoFar = assertDiskTierSpecificStats(client, "index", 2, tookTimeSoFar, -1);

        // A final request for something in neither tier shouldn't increment disk specific stats
        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + numRequests)).get();
        IndicesRequestCacheIT.assertCacheState(client, "index", 0, numRequests + 3, TierType.ON_HEAP, false);
        IndicesRequestCacheIT.assertCacheState(client, "index", 2, numRequests + 1, TierType.DISK, false);
        assertDiskTierSpecificStats(client, "index", 2, tookTimeSoFar, tookTimeSoFar);

    }

    public void testRBMSizeSetting() throws Exception {
        int heapSizeBytes = 0;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), new ByteSizeValue(heapSizeBytes))
                .put(DiskTierTookTimePolicy.DISK_TOOKTIME_THRESHOLD_SETTING.getKey(), TimeValue.ZERO) // allow into disk cache regardless of
            // took time
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

        for (int i = 0; i < 10000; i++) { // RBMIntKeyLookupStore.REFRESH_SIZE_EST_INTERVAL + 10
            resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hellohellohellohellohellohellohellohello" + i)).get();
        }

        RequestCacheStats stats = client.admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache();
        long currentKeystoreSize = stats.getDiskSpecificStats().getKeystoreWeight();
        System.out.println("HEAP ENTRIES " + stats.getEntries(TierType.ON_HEAP));
        System.out.println("DISK ENTRIES " + stats.getEntries(TierType.DISK));
        System.out.println("DISK SIZE " + stats.getMemorySize(TierType.DISK));

        //client.close();
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

    private long assertDiskTierSpecificStats(
        Client client,
        String index,
        long totalDiskReaches,
        long totalGetTimeLowerBound,
        long totalGetTimeUpperBound
    ) {
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
