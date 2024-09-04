/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Because of differences in how integ tests work between 2.11 and 2.15, this test was moved from IndicesRequestCacheIT.java
 * into its own file with a TEST-level scope when backporting tiered caching to 2.11. When the original file has this scope,
 * the first test to run can't load plugin settings correctly and fails.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class IndicesRequestCacheSingleNodeIT extends OpenSearchIntegTestCase {
    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.PLUGGABLE_CACHE, "true").build();
    } // For now hardcode TC feature flag as true. Attempt to backport the changes allowing us to parameterize it

    public void testDeleteAndCreateSameIndexShardOnSameNode() throws Exception {
        String node_1 = internalCluster().startNode(Settings.builder().build());
        Client client = client(node_1);

        logger.info("Starting a node in the cluster");

        assertThat(cluster().size(), equalTo(1));
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("1").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        String indexName = "test";

        logger.info("Creating an index: {} with 2 shards", indexName);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                // Disable index refreshing to avoid cache being invalidated mid-test
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                .build()
        );

        ensureGreen(indexName);

        logger.info("Writing few docs and searching those which will cache items in RequestCache");
        indexRandom(true, client.prepareIndex(indexName).setSource("k", "hello"));
        indexRandom(true, client.prepareIndex(indexName).setSource("y", "hello again"));
        ensureSearchable(indexName);
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, indexName);
        SearchResponse resp = client.prepareSearch(indexName).setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello")).get();
        assertSearchResponse(resp);
        resp = client.prepareSearch(indexName).setRequestCache(true).setQuery(QueryBuilders.termQuery("y", "hello")).get();

        RequestCacheStats stats = getNodeCacheStats(client);
        assertTrue(stats.getMemorySizeInBytes() > 0);

        logger.info("Disabling allocation");
        Settings newSettings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE.name())
            .build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(newSettings).execute().actionGet();

        logger.info("Starting a second node");
        String node_2 = internalCluster().startDataOnlyNode(Settings.builder().build());
        assertThat(cluster().size(), equalTo(2));
        healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("Moving the shard:{} from node:{} to node:{}", indexName + "#0", node_1, node_2);
        MoveAllocationCommand cmd = new MoveAllocationCommand(indexName, 0, node_1, node_2);
        internalCluster().client().admin().cluster().prepareReroute().add(cmd).get();
        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        final Index index = state.metadata().index(indexName).getIndex();

        assertBusy(() -> {
            assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(false));
            assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        });

        logger.info("Moving the shard: {} again from node:{} to node:{}", indexName + "#0", node_2, node_1);
        cmd = new MoveAllocationCommand(indexName, 0, node_2, node_1);
        internalCluster().client().admin().cluster().prepareReroute().add(cmd).get();
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));

        assertBusy(() -> {
            assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
            assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(false));
        });

        logger.info("Clearing the cache for index:{}. And verify the request stats doesn't go negative", indexName);
        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(indexName);
        client.admin().indices().clearCache(clearIndicesCacheRequest).actionGet();

        stats = getNodeCacheStats(client(node_1));
        assertTrue(stats.getMemorySizeInBytes() == 0);
        stats = getNodeCacheStats(client(node_2));
        assertTrue(stats.getMemorySizeInBytes() == 0);
    }

    private Path shardDirectory(String server, Index index, int shard) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        final Path[] paths = env.availableShardPaths(new ShardId(index, shard));
        assert paths.length == 1;
        return paths[0];
    }

    private static RequestCacheStats getNodeCacheStats(Client client) {
        NodesStatsResponse stats = client.admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : stats.getNodes()) {
            if (stat.getNode().isDataNode()) {
                return stat.getIndices().getRequestCache();
            }
        }
        return null;
    }

    private void forceMerge(Client client, String index) {
        ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge(index).setFlush(true).get();
        OpenSearchAssertions.assertAllSuccessful(forceMergeResponse);
        refresh();
    }
}
