/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

@ClusterScope(scope = Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class VirtualThreadSchedulerStatsIT extends OpenSearchIntegTestCase {

    public void testVirtualThreadSchedulerStatsReturnedFromNodesStats() {
        NodesStatsResponse response = client().admin()
            .cluster()
            .prepareNodesStats()
            .addMetric(NodesStatsRequest.Metric.VIRTUAL_THREAD_SCHEDULER.metricName())
            .get();
        assertFalse(response.hasFailures());

        NodeStats nodeStats = response.getNodes().get(0);
        assertNotNull(nodeStats);

        VirtualThreadSchedulerStats stats = nodeStats.getVirtualThreadSchedulerStats();
        assertNotNull("VirtualThreadSchedulerStats should always be present in nodes stats", stats);

        // All four values must be present; -1 is acceptable when the JDK does not expose the bean
        assertTrue("queuedVirtualThreadCount must be >= -1", stats.getQueuedVirtualThreadCount() >= -1);
        assertTrue("mountedVirtualThreadCount must be >= -1", stats.getMountedVirtualThreadCount() >= -1);
        assertTrue("parallelism must be >= -1", stats.getParallelism() >= -1);
        assertTrue("poolSize must be >= -1", stats.getPoolSize() >= -1);
    }
}
