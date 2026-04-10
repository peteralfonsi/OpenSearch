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
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

/**
 * Integration tests for the {@code search_virtual_threads.parallelism} cluster setting.
 *
 * <p>The JDK virtual thread scheduler bean ({@code jdk.management.VirtualThreadSchedulerMXBean})
 * is not available on all JDKs. Each test checks for availability at the start and passes
 * immediately (with a log message) when the bean is absent, so the suite remains green on
 * JDKs that do not expose it.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class VirtualThreadsParallelismSettingIT extends OpenSearchIntegTestCase {

    /**
     * Verifies that updating {@code search_virtual_threads.parallelism} via the cluster settings
     * API is accepted without error and, on JDKs that support the bean, that the reported
     * parallelism in node stats reflects the new value.
     */
    public void testParallelismSettingUpdateAccepted() {
        if (VirtualThreadBeanHelper.VT_SCHEDULER_MXBEAN == null) {
            logger.info("VirtualThreadSchedulerMXBean not available on this JDK; skipping parallelism setting IT");
            return;
        }

        int initialParallelism = fetchParallelismFromNodeStats();

        // Choose a target that differs from the current value so we can detect the change.
        // We stay within a safe range (1..Runtime.getRuntime().availableProcessors()*2).
        int maxSafe = Math.max(1, Runtime.getRuntime().availableProcessors() * 2);
        int target = (initialParallelism > 1) ? Math.min(initialParallelism - 1, maxSafe) : Math.min(initialParallelism + 1, maxSafe);

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(VirtualThreadBeanHelper.SEARCH_VIRTUAL_THREADS_PARALLELISM.getKey(), target))
            .get();

        if (VirtualThreadBeanHelper.SET_PARALLELISM == null) {
            // setParallelism method not present; the update is a no-op — just verify no exception was thrown.
            logger.info("setParallelism method not available on this JDK; verified no-op update accepted");
            return;
        }

        // On JDKs that expose setParallelism, the reported parallelism should now equal target.
        int reportedParallelism = fetchParallelismFromNodeStats();
        assertEquals(
            "Virtual thread scheduler parallelism should have been updated to " + target,
            target,
            reportedParallelism
        );
    }

    /**
     * Verifies that the default value (-1) leaves the scheduler parallelism unchanged.
     * -1 is below VirtualThreadBeanHelper.MIN_PARALLELISM so the helper discards it before
     * touching the bean.
     */
    public void testDefaultValueIsNoOp() {
        if (VirtualThreadBeanHelper.VT_SCHEDULER_MXBEAN == null) {
            logger.info("VirtualThreadSchedulerMXBean not available on this JDK; skipping default-value IT");
            return;
        }

        int before = fetchParallelismFromNodeStats();

        // Explicitly set to -1 (the default / no-op sentinel)
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(VirtualThreadBeanHelper.SEARCH_VIRTUAL_THREADS_PARALLELISM.getKey(), -1))
            .get();

        int after = fetchParallelismFromNodeStats();
        assertEquals("Setting parallelism to -1 should be a no-op; parallelism must not change", before, after);
    }

    /**
     * Verifies that the cluster settings API rejects a value above the declared maximum (32767)
     * with an error. The setting's own max bound fires first (before the consumer runs), so
     * VirtualThreadBeanHelper.setParallelism is never reached. Even if it were, the helper's
     * own MAX_PARALLELISM guard would also reject it.
     */
    public void testParallelismSettingRejectsValueAboveMax() {
        try {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(VirtualThreadBeanHelper.SEARCH_VIRTUAL_THREADS_PARALLELISM.getKey(), 32768))
                .get();
            fail("Expected an exception when setting parallelism above the maximum");
        } catch (Exception e) {
            // The settings framework wraps the IllegalArgumentException; just verify something was thrown.
            // The bean's setParallelism is never called because validation rejects the value first.
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private int fetchParallelismFromNodeStats() {
        NodesStatsResponse response = client().admin()
            .cluster()
            .prepareNodesStats()
            .addMetric(NodesStatsRequest.Metric.VIRTUAL_THREAD_SCHEDULER.metricName())
            .get();
        assertFalse(response.hasFailures());
        NodeStats nodeStats = response.getNodes().get(0);
        assertNotNull(nodeStats);
        VirtualThreadSchedulerStats stats = nodeStats.getVirtualThreadSchedulerStats();
        assertNotNull(stats);
        return stats.getParallelism();
    }
}
