/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nodestats;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyMap;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeStatsSerializationFailureIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BadStatsInjectorPlugin.class);
    }

    public static class BadStatsInjectorPlugin extends Plugin implements NetworkPlugin {

        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return List.of(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    String executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    return new TransportRequestHandler<T>() {
                        @Override
                        public void messageReceived(
                            T request,
                            org.opensearch.transport.TransportChannel channel,
                            org.opensearch.tasks.Task task
                        ) throws Exception {
                            actualHandler.messageReceived(request, new org.opensearch.transport.TransportChannel() {
                                @Override
                                public String getProfileName() {
                                    return channel.getProfileName();
                                }

                                @Override
                                public String getChannelType() {
                                    return channel.getChannelType();
                                }

                                @Override
                                public void sendResponse(TransportResponse response) throws java.io.IOException {
                                    if (response instanceof NodeStats) {
                                        NodeStats nodeStats = (NodeStats) response;
                                        if (nodeStats.getNode().getName().contains("0") && nodeStats.getIndices() != null) {
                                            CommonStats badCommonStats = new CommonStats();
                                            badCommonStats.fieldData = new FieldDataStats.Builder().memorySize(-100L)
                                                .evictions(10L)
                                                .build();
                                            NodeIndicesStats badIndices = new NodeIndicesStats(
                                                badCommonStats,
                                                emptyMap(),
                                                null,
                                                null,
                                                null
                                            );

                                            response = new NodeStats(
                                                nodeStats.getNode(),
                                                nodeStats.getTimestamp(),
                                                badIndices,
                                                nodeStats.getOs(),
                                                nodeStats.getProcess(),
                                                nodeStats.getJvm(),
                                                nodeStats.getThreadPool(),
                                                nodeStats.getFs(),
                                                nodeStats.getTransport(),
                                                nodeStats.getHttp(),
                                                nodeStats.getBreaker(),
                                                nodeStats.getScriptStats(),
                                                nodeStats.getDiscoveryStats(),
                                                nodeStats.getIngestStats(),
                                                nodeStats.getAdaptiveSelectionStats(),
                                                nodeStats.getResourceUsageStats(),
                                                nodeStats.getScriptCacheStats(),
                                                nodeStats.getIndexingPressureStats(),
                                                nodeStats.getShardIndexingPressureStats(),
                                                nodeStats.getSearchBackpressureStats(),
                                                nodeStats.getClusterManagerThrottlingStats(),
                                                nodeStats.getWeightedRoutingStats(),
                                                nodeStats.getFileCacheStats(),
                                                nodeStats.getTaskCancellationStats(),
                                                nodeStats.getSearchPipelineStats(),
                                                nodeStats.getSegmentReplicationRejectionStats(),
                                                nodeStats.getRepositoriesStats(),
                                                nodeStats.getAdmissionControlStats(),
                                                nodeStats.getNodeCacheStats(),
                                                nodeStats.getRemoteStoreNodeStats()
                                            );
                                        }
                                    }
                                    channel.sendResponse(response);
                                }

                                @Override
                                public void sendResponse(Exception exception) throws java.io.IOException {
                                    channel.sendResponse(exception);
                                }

                                @Override
                                public Version getVersion() {
                                    return channel.getVersion();
                                }
                            }, task);
                        }
                    };
                }
            });
        }
    }

    public void testClusterReturnsNodeStatsWithBadData() throws Exception {
        internalCluster().startNodes(3);

        assertAcked(
            prepareCreate("test-index").setSettings(
                org.opensearch.common.settings.Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1)
            )
        );
        ensureGreen();

        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test-index").setSource("field", "value" + i).get();
        }
        refresh();

        NodesStatsResponse response = client().admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertFalse(response.hasFailures());
        assertEquals(3, response.getNodes().size());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                NodesStatsResponse deserialized = new NodesStatsResponse(in);
                assertEquals(3, deserialized.getNodes().size());

                int nodesWithIndices = 0;
                for (NodeStats nodeStats : deserialized.getNodes()) {
                    if (nodeStats.getIndices() != null) {
                        nodesWithIndices++;
                    }
                }

                assertEquals(2, nodesWithIndices);
            }
        }
    }
}
