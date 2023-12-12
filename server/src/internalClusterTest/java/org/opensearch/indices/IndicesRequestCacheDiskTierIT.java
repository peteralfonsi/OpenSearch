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

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.tier.DiskTierTookTimePolicy;
import org.opensearch.common.cache.tier.TierType;
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
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true")
            .put(FeatureFlags.TIERED_CACHING, "true")
            .build();
    }

    public void testDiskTierStats() throws Exception {
        int heapSizeBytes = 9876;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), new ByteSizeValue(heapSizeBytes))
                .put(DiskTierTookTimePolicy.DISK_TOOKTIME_THRESHOLD_SETTING.getKey(), TimeValue.ZERO) // allow into disk cache regardless of took time
                .put(IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED.getKey(), true)
        );
        Client client = client(node);
        startIndex(client);
        int requestSize = getRequestSize(client, heapSizeBytes);

        int numOnDisk = 5;
        int numRequests = heapSizeBytes / requestSize + numOnDisk;
        SearchResponse resp;
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

    public void testTogglingDiskTierSetting() throws Exception {
        // For this test, feature flag tiered caching setting is on, but we toggle IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED
        // and cover all possible transitions
        // start with it off (default), then turn it on dynamically

        // setup
        int heapSizeBytes = 5000;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), new ByteSizeValue(heapSizeBytes))
                .put(DiskTierTookTimePolicy.DISK_TOOKTIME_THRESHOLD_SETTING.getKey(), TimeValue.ZERO) // allow into disk cache regardless of took time
        );
        Client client = client(node);
        startIndex(client);
        int requestSize = getRequestSize(client, heapSizeBytes);

        // Make more queries than fit in the heap cache, and confirm these do not spill over to a disk tier, as we haven't made one yet.

        int numEvicted = 5;
        int numRequests = heapSizeBytes / requestSize + numEvicted;
        SearchResponse resp;
        for (int i = 1; i < numRequests; i++) {
            resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + i)).get();
            assertSearchResponse(resp);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.ON_HEAP, false);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, 0, TierType.DISK, false);
        }
        IndicesRequestCacheIT.assertNumEvictions(client, "index", numEvicted, TierType.ON_HEAP);
        // The heap tier should now contain requests # numEvicted through numRequests. 0 through numEvicted have been evicted and lost.

        // Set IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED = false. Nothing should happen
        setDiskCacheEnabled(client, false);

        // Set IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED = true, creating a new disk tier
        setDiskCacheEnabled(client, true);

        // Make some new requests. These should cause evictions from the heap tier which spill over to the disk tier (requests between numEvicted and numEvicted + numOnDisk)
        int numOnDisk = 3;
        for (int i = numRequests; i < numRequests + numOnDisk; i++) {
            resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + i)).get();
            assertSearchResponse(resp);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 1, TierType.ON_HEAP, false);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i - numRequests + 1, TierType.DISK, false);
        }
        IndicesRequestCacheIT.assertNumEvictions(client, "index", numEvicted + numOnDisk, TierType.ON_HEAP);
        IndicesRequestCacheIT.assertNumCacheEntries(client, "index", heapSizeBytes / requestSize, TierType.ON_HEAP);
        IndicesRequestCacheIT.assertNumCacheEntries(client, "index", numOnDisk, TierType.DISK);
        // Confirm request # numEvicted + 1 is a disk tier hit
        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + (numEvicted + 1))).get();
        assertSearchResponse(resp);
        IndicesRequestCacheIT.assertCacheState(client, "index", 0, numRequests + numOnDisk + 1, TierType.ON_HEAP, false);
        IndicesRequestCacheIT.assertCacheState(client, "index", 1, numOnDisk, TierType.DISK, false);

        // Set IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED = true. Nothing should happen
        setDiskCacheEnabled(client, true);

        // Set IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED = false. The disk tier should be deactivated but not deleted
        setDiskCacheEnabled(client, false);
        IndicesRequestCacheIT.assertNumCacheEntries(client, "index", numOnDisk, TierType.DISK);

        // Make some new requests. These should cause evictions from the heap tier (numEvicted + numOnDisk through numEvicted + numOnDisk + numNewRequests), but not reach the deactivated disk tier
        int numNewRequests = 5;
        assertTrue(numNewRequests > 3); // necessary for checks later in the test
        for (int i = numRequests + numOnDisk; i < numRequests + numOnDisk + numNewRequests; i++) {
            resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + i)).get();
            assertSearchResponse(resp);
            IndicesRequestCacheIT.assertCacheState(client, "index", 0, i + 2, TierType.ON_HEAP, false);
            IndicesRequestCacheIT.assertCacheState(client, "index", 1, numOnDisk, TierType.DISK, false);
        }
        IndicesRequestCacheIT.assertNumEvictions(client, "index", numEvicted + numOnDisk + numNewRequests, TierType.ON_HEAP);
        IndicesRequestCacheIT.assertNumCacheEntries(client, "index", heapSizeBytes / requestSize, TierType.ON_HEAP);
        IndicesRequestCacheIT.assertNumCacheEntries(client, "index", numOnDisk, TierType.DISK);
        // Now the heap tier contains requests numEvicted + numOnDisk + numNewRequests through numEvicted + numOnDisk + numNewRequests + num

        // Check a request (# numEvicted) which was on the disk tier is now a cache miss. This evicts #
        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + numEvicted)).get();
        assertSearchResponse(resp);
        IndicesRequestCacheIT.assertCacheState(client, "index", 0, numRequests + numOnDisk + numNewRequests + 2, TierType.ON_HEAP, false);
        IndicesRequestCacheIT.assertCacheState(client, "index", 1, numOnDisk, TierType.DISK, false);


        // Re-enable the disk tier. We should get the same previously deactivated disk tier, not a new one
        setDiskCacheEnabled(client, true);
        IndicesRequestCacheIT.assertNumCacheEntries(client, "index", numOnDisk, TierType.DISK);
        // Request # numEvicted + 2 should be a disk tier hit
        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + (numEvicted + 2))).get();
        assertSearchResponse(resp);
        IndicesRequestCacheIT.assertCacheState(client, "index", 0, numRequests + numOnDisk + numNewRequests + 3, TierType.ON_HEAP, false);
        IndicesRequestCacheIT.assertCacheState(client, "index", 2, numOnDisk, TierType.DISK, false);
        // New queries should cause spillover to the disk tier again
        int reqNum = numRequests + numOnDisk + numNewRequests;
        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + reqNum)).get();
        assertSearchResponse(resp);
        IndicesRequestCacheIT.assertCacheState(client, "index", 0, numRequests + numOnDisk + numNewRequests + 4, TierType.ON_HEAP, false);
        IndicesRequestCacheIT.assertCacheState(client, "index", 2, numOnDisk + 1, TierType.DISK, false);
        IndicesRequestCacheIT.assertNumCacheEntries(client, "index", numOnDisk + 1, TierType.DISK);

        // TODO: Once Kiran's cache clear PR is done, add a case where we deactivate, clear disk cache, reactivate, and see a new empty disk tier.
    }

    private void startIndex(Client client) throws Exception {
        Settings.Builder indicesSettingBuilder = Settings.builder()
            .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        assertAcked(
            client.admin().indices().prepareCreate("index").setMapping("k", "type=keyword").setSettings(indicesSettingBuilder).get()
        );
        indexRandom(true, client.prepareIndex("index").setSource("k", "hello"));
        ensureSearchable("index");
    }

    private int getRequestSize(Client client, int heapSizeBytes) throws Exception {
        // Setup function to get the maximum number of entries of the form "hello" + i
        // that will fit in the heap cache, by making a query and checking size afterwards

        SearchResponse resp;
        resp = client.prepareSearch("index").setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello" + 0)).get();
        int requestSize = (int) getCacheSizeBytes(client, "index", TierType.ON_HEAP);
        assertTrue(heapSizeBytes > requestSize);
        // If this fails, increase heapSizeBytes! We can't adjust it after getting the size of one query
        // as the cache size setting is not dynamic
        return requestSize;
    }

    private void setDiskCacheEnabled(Client client, boolean newSetting) {
        ClusterUpdateSettingsRequest clusterSettingUpdate = new ClusterUpdateSettingsRequest();
        clusterSettingUpdate.persistentSettings(Settings.builder().put(IndicesRequestCache.INDICES_CACHE_DISK_TIER_ENABLED.getKey(), newSetting));
        assertAcked(client.admin().cluster().updateSettings(clusterSettingUpdate).actionGet());
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
