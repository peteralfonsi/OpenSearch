/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.monitor.process.ProcessStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.test.VersionUtils.randomVersion;

public class NodeStatsSerializationFailureTests extends OpenSearchTestCase {

    public void testSerializationWithNegativeFieldDataMemorySize() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            randomVersion(random())
        );

        // Create CommonStats with negative memorySize which will fail writeVLong
        CommonStats badCommonStats = new CommonStats();
        badCommonStats.fieldData = new FieldDataStats.Builder().memorySize(-100L).evictions(10L).build();

        NodeIndicesStats indicesStats = new NodeIndicesStats(badCommonStats, java.util.Collections.emptyMap(), null, null, null);

        NodeStats nodeStats = new NodeStats(
            node,
            System.currentTimeMillis(),
            indicesStats,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        // Serialize - should not throw exception, should skip the failing stat
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeStats.writeTo(out);

            // Deserialize
            try (StreamInput in = out.bytes().streamInput()) {
                NodeStats deserialized = new NodeStats(in);

                // Verify node info is preserved
                assertEquals(node.getId(), deserialized.getNode().getId());
                assertEquals(nodeStats.getTimestamp(), deserialized.getTimestamp());

                // The indices stat should be null since serialization failed
                assertNull(deserialized.getIndices());
            }
        }
    }

    public void testBadIndicesWithGoodOtherStats() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            randomVersion(random())
        );

        // Create bad indices stats
        CommonStats badCommonStats = new CommonStats();
        badCommonStats.fieldData = new FieldDataStats.Builder().memorySize(100L).evictions(-1L).build();
        NodeIndicesStats badIndices = new NodeIndicesStats(badCommonStats, emptyMap(), null, null, null);

        // Create good OS stats
        OsStats osStats = new OsStats(
            System.currentTimeMillis(),
            new OsStats.Cpu((short) 50, null),
            new OsStats.Mem(1000, 500),
            new OsStats.Swap(500, 250),
            null
        );

        // Create good Process stats
        ProcessStats processStats = new ProcessStats(
            System.currentTimeMillis(),
            123,
            456,
            new ProcessStats.Cpu((short) 10, 1000),
            new ProcessStats.Mem(2000)
        );

        NodeStats nodeStats = new NodeStats(
            node,
            System.currentTimeMillis(),
            badIndices,
            osStats,
            processStats,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        // Serialize
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeStats.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                NodeStats deserialized = new NodeStats(in);

                // Bad indices should be null
                assertNull(deserialized.getIndices());

                // Good stats should be preserved
                assertNotNull(deserialized.getOs());
                assertEquals(50, deserialized.getOs().getCpu().getPercent());
                assertNotNull(deserialized.getProcess());
                assertEquals(123, deserialized.getProcess().getOpenFileDescriptors());
            }
        }
    }

    public void testMultipleNodesWithMixedStats() throws IOException {
        // Node 1: Good stats
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), randomVersion(random()));
        CommonStats goodCommonStats = new CommonStats();
        goodCommonStats.fieldData = new FieldDataStats.Builder().memorySize(100L).evictions(5L).build();
        NodeIndicesStats goodIndices = new NodeIndicesStats(goodCommonStats, emptyMap(), null, null, null);
        OsStats osStats1 = new OsStats(
            System.currentTimeMillis(),
            new OsStats.Cpu((short) 30, null),
            new OsStats.Mem(2000, 1000),
            new OsStats.Swap(1000, 500),
            null
        );

        NodeStats goodNodeStats = new NodeStats(
            node1,
            System.currentTimeMillis(),
            goodIndices,
            osStats1,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        // Node 2: Bad indices, good OS
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), randomVersion(random()));
        CommonStats badCommonStats = new CommonStats();
        badCommonStats.fieldData = new FieldDataStats.Builder().memorySize(-200L).build();
        NodeIndicesStats badIndices = new NodeIndicesStats(badCommonStats, emptyMap(), null, null, null);
        OsStats osStats2 = new OsStats(
            System.currentTimeMillis(),
            new OsStats.Cpu((short) 40, null),
            new OsStats.Mem(3000, 1500),
            new OsStats.Swap(1500, 750),
            null
        );

        NodeStats badNodeStats = new NodeStats(
            node2,
            System.currentTimeMillis(),
            badIndices,
            osStats2,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        // Node 3: All good
        DiscoveryNode node3 = new DiscoveryNode("node3", buildNewFakeTransportAddress(), emptyMap(), emptySet(), randomVersion(random()));
        CommonStats goodCommonStats2 = new CommonStats();
        goodCommonStats2.fieldData = new FieldDataStats.Builder().memorySize(150L).evictions(3L).build();
        NodeIndicesStats goodIndices2 = new NodeIndicesStats(goodCommonStats2, emptyMap(), null, null, null);
        OsStats osStats3 = new OsStats(
            System.currentTimeMillis(),
            new OsStats.Cpu((short) 25, null),
            new OsStats.Mem(2500, 1200),
            new OsStats.Swap(1200, 600),
            null
        );

        NodeStats goodNodeStats2 = new NodeStats(
            node3,
            System.currentTimeMillis(),
            goodIndices2,
            osStats3,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        // Serialize all nodes
        List<NodeStats> allNodes = List.of(goodNodeStats, badNodeStats, goodNodeStats2);
        for (NodeStats nodeStats : allNodes) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                nodeStats.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    NodeStats deserialized = new NodeStats(in);

                    // All should have OS stats preserved
                    assertNotNull(deserialized.getOs());

                    if (deserialized.getNode().getId().equals("node1")) {
                        assertNotNull(deserialized.getIndices());
                        assertEquals(100L, deserialized.getIndices().getFieldData().getMemorySizeInBytes());
                        assertEquals(30, deserialized.getOs().getCpu().getPercent());
                    } else if (deserialized.getNode().getId().equals("node2")) {
                        // Bad indices should be null
                        assertNull(deserialized.getIndices());
                        // Good OS preserved
                        assertEquals(40, deserialized.getOs().getCpu().getPercent());
                    } else if (deserialized.getNode().getId().equals("node3")) {
                        assertNotNull(deserialized.getIndices());
                        assertEquals(150L, deserialized.getIndices().getFieldData().getMemorySizeInBytes());
                        assertEquals(25, deserialized.getOs().getCpu().getPercent());
                    }
                }
            }
        }

        // Create NodesStatsResponse to test aggregation
        NodesStatsResponse response = new NodesStatsResponse(
            new org.opensearch.cluster.ClusterName("test"),
            List.of(goodNodeStats, badNodeStats, goodNodeStats2),
            List.of()
        );

        // Serialize response
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                NodesStatsResponse deserialized = new NodesStatsResponse(in);

                assertEquals(3, deserialized.getNodes().size());
                assertFalse(deserialized.hasFailures());

                // Count nodes with non-null indices stats
                int nodesWithIndices = 0;
                for (NodeStats node : deserialized.getNodes()) {
                    assertNotNull(node.getOs());
                    if (node.getIndices() != null) {
                        nodesWithIndices++;
                    }
                }
                // 2 nodes should have non-null indices (node1 and node3)
                assertEquals(2, nodesWithIndices);
            }
        }
    }

    public void testBadOsStatsWithGoodIndices() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            randomVersion(random())
        );

        // Create good indices stats
        CommonStats goodCommonStats = new CommonStats();
        goodCommonStats.fieldData = new FieldDataStats.Builder().memorySize(100L).evictions(5L).build();
        NodeIndicesStats goodIndices = new NodeIndicesStats(goodCommonStats, emptyMap(), null, null, null);

        // Create bad OsStats with negative timestamp (will fail writeVLong)
        OsStats badOsStats = new OsStats.Builder().timestamp(-100L)
            .cpu(new OsStats.Cpu((short) 50, null))
            .mem(new OsStats.Mem(1000, 500))
            .swap(new OsStats.Swap(500, 250))
            .build();

        NodeStats nodeStats = new NodeStats(
            node,
            System.currentTimeMillis(),
            goodIndices,
            badOsStats,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        // Serialize
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeStats.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                NodeStats deserialized = new NodeStats(in);

                // Good indices should be preserved
                assertNotNull(deserialized.getIndices());
                assertNotNull(deserialized.getIndices().getFieldData());
                assertEquals(100L, deserialized.getIndices().getFieldData().getMemorySizeInBytes());

                // Bad OS stats should be null
                assertNull(deserialized.getOs());
            }
        }
    }

    public void testNullIndicesStats() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            randomVersion(random())
        );

        NodeStats nodeStats = new NodeStats(
            node,
            System.currentTimeMillis(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeStats.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                NodeStats deserialized = new NodeStats(in);
                assertNull(deserialized.getIndices());
            }
        }
    }
}
