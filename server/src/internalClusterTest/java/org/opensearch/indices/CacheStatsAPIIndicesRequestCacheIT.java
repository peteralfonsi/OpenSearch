/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.service.NodeCacheStats;
import org.opensearch.common.cache.stats.CacheStatsCounterSnapshot;
import org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

// Use a single data node to simplify logic about cache stats across different shards.
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CacheStatsAPIIndicesRequestCacheIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    public CacheStatsAPIIndicesRequestCacheIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(FeatureFlags.PLUGGABLE_CACHE, "true").build() },
            new Object[] { Settings.builder().put(FeatureFlags.PLUGGABLE_CACHE, "false").build() }
        );
    }

    public void testCacheStatsAPIWIthOnHeapCache() throws Exception {
        String index1Name = "index1";
        String index2Name = "index2";
        Client client = client();

        startIndex(client, index1Name);
        startIndex(client, index2Name);

        // Search twice for the same doc in index 1
        for (int i = 0; i < 2; i++) {
            SearchResponse resp = client.prepareSearch(index1Name)
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k", "hello"))
                .get();
            assertSearchResponse(resp);
            OpenSearchAssertions.assertAllSuccessful(resp);
            assertEquals(1, resp.getHits().getTotalHits().value);
        }

        // Search once for a doc in index 2
        SearchResponse resp = client.prepareSearch(index2Name).setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello")).get();
        assertSearchResponse(resp);
        OpenSearchAssertions.assertAllSuccessful(resp);
        assertEquals(1, resp.getHits().getTotalHits().value);

        // First, aggregate by indices only
        Map<String, Object> xContentMap = getNodeCacheStatsXContentMap(client, "", List.of(IndicesRequestCache.INDEX_DIMENSION_NAME));

        List<String> index1Keys = List.of(
            CacheType.INDICES_REQUEST_CACHE.getApiRepresentation(),
            IndicesRequestCache.INDEX_DIMENSION_NAME,
            index1Name
        );
        // Since we searched twice, we expect to see 1 hit, 1 miss and 1 entry for index 1
        CacheStatsCounterSnapshot expectedStats = new CacheStatsCounterSnapshot(1, 1, 0, 0, 1);
        checkCacheStatsAPIResponse(xContentMap, index1Keys, expectedStats, false);
        // Get the request size for one request, so we can reuse it for next index
        int requestSize = (int) ((Map<String, Object>) MultiDimensionCacheStatsTests.getValueFromNestedXContentMap(xContentMap, index1Keys))
            .get(CacheStatsCounterSnapshot.Fields.MEMORY_SIZE_IN_BYTES);
        assertTrue(requestSize > 0);

        List<String> index2Keys = List.of(
            CacheType.INDICES_REQUEST_CACHE.getApiRepresentation(),
            IndicesRequestCache.INDEX_DIMENSION_NAME,
            index2Name
        );
        // We searched once in index 2, we expect 1 miss + 1 entry
        expectedStats = new CacheStatsCounterSnapshot(0, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(xContentMap, index2Keys, expectedStats, true);

        // The total stats for the node should be 1 hit, 2 misses, and 2 entries
        expectedStats = new CacheStatsCounterSnapshot(1, 2, 0, 2 * requestSize, 2);
        List<String> totalStatsKeys = List.of(CacheType.INDICES_REQUEST_CACHE.getApiRepresentation());
        checkCacheStatsAPIResponse(xContentMap, totalStatsKeys, expectedStats, true);

        // Aggregate by shards only
        xContentMap = getNodeCacheStatsXContentMap(client, "", List.of(IndicesRequestCache.SHARD_ID_DIMENSION_NAME));

        List<String> index1Shard0Keys = List.of(
            CacheType.INDICES_REQUEST_CACHE.getApiRepresentation(),
            IndicesRequestCache.SHARD_ID_DIMENSION_NAME,
            "[" + index1Name + "][0]"
        );

        expectedStats = new CacheStatsCounterSnapshot(1, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(xContentMap, index1Shard0Keys, expectedStats, true);

        List<String> index2Shard0Keys = List.of(
            CacheType.INDICES_REQUEST_CACHE.getApiRepresentation(),
            IndicesRequestCache.SHARD_ID_DIMENSION_NAME,
            "[" + index2Name + "][0]"
        );
        expectedStats = new CacheStatsCounterSnapshot(0, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(xContentMap, index2Shard0Keys, expectedStats, true);

        // Aggregate by indices and shards
        xContentMap = getNodeCacheStatsXContentMap(
            client,
            "",
            List.of(IndicesRequestCache.INDEX_DIMENSION_NAME, IndicesRequestCache.SHARD_ID_DIMENSION_NAME)
        );

        index1Keys = List.of(
            CacheType.INDICES_REQUEST_CACHE.getApiRepresentation(),
            IndicesRequestCache.INDEX_DIMENSION_NAME,
            index1Name,
            IndicesRequestCache.SHARD_ID_DIMENSION_NAME,
            "[" + index1Name + "][0]"
        );

        expectedStats = new CacheStatsCounterSnapshot(1, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(xContentMap, index1Keys, expectedStats, true);

        index2Keys = List.of(
            CacheType.INDICES_REQUEST_CACHE.getApiRepresentation(),
            IndicesRequestCache.INDEX_DIMENSION_NAME,
            index2Name,
            IndicesRequestCache.SHARD_ID_DIMENSION_NAME,
            "[" + index2Name + "][0]"
        );

        expectedStats = new CacheStatsCounterSnapshot(0, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(xContentMap, index2Keys, expectedStats, true);

    }

    // TODO: Add testCacheStatsAPIWithTieredCache when TSC stats implementation PR is merged

    private void startIndex(Client client, String indexName) throws InterruptedException {
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(indexName)
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .get()
        );
        indexRandom(true, client.prepareIndex(indexName).setSource("k", "hello"));
        ensureSearchable(indexName);
    }

    private static Map<String, Object> getNodeCacheStatsXContentMap(Client client, String nodeId, List<String> aggregationLevels)
        throws IOException {

        CommonStatsFlags statsFlags = new CommonStatsFlags();
        statsFlags.includeAllCacheTypes();

        NodesStatsResponse nodeStatsResponse = client.admin()
            .cluster()
            .prepareNodesStats("data:true")
            .addMetric(NodesStatsRequest.Metric.CACHE_STATS.metricName())
            .setIndices(statsFlags)
            .get();
        // Can always get the first data node as there's only one in this test suite
        assertEquals(1, nodeStatsResponse.getNodes().size());
        NodeCacheStats ncs = nodeStatsResponse.getNodes().get(0).getNodeCacheStats();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        Map<String, String> paramMap = Map.of("level", String.join(",", aggregationLevels));
        ToXContent.Params params = new ToXContent.MapParams(paramMap);

        builder.startObject();
        ncs.toXContent(builder, params);
        builder.endObject();

        String resultString = builder.toString();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), resultString, true);
    }

    private static void checkCacheStatsAPIResponse(
        Map<String, Object> xContentMap,
        List<String> xContentMapKeys,
        CacheStatsCounterSnapshot expectedStats,
        boolean checkMemorySize
    ) {
        // Assumes the keys point to a level whose keys are the field values ("size_in_bytes", "evictions", etc) and whose values store
        // those stats
        Map<String, Object> aggregatedStatsResponse = (Map<String, Object>) MultiDimensionCacheStatsTests.getValueFromNestedXContentMap(
            xContentMap,
            xContentMapKeys
        );
        assertNotNull(aggregatedStatsResponse);
        assertEquals(expectedStats.getHits(), (int) aggregatedStatsResponse.get(CacheStatsCounterSnapshot.Fields.HIT_COUNT));
        assertEquals(expectedStats.getMisses(), (int) aggregatedStatsResponse.get(CacheStatsCounterSnapshot.Fields.MISS_COUNT));
        assertEquals(expectedStats.getEvictions(), (int) aggregatedStatsResponse.get(CacheStatsCounterSnapshot.Fields.EVICTIONS));
        if (checkMemorySize) {
            assertEquals(
                expectedStats.getSizeInBytes(),
                (int) aggregatedStatsResponse.get(CacheStatsCounterSnapshot.Fields.MEMORY_SIZE_IN_BYTES)
            );
        }
        assertEquals(expectedStats.getEntries(), (int) aggregatedStatsResponse.get(CacheStatsCounterSnapshot.Fields.ENTRIES));
    }
}
