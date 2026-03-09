/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.test.OpenSearchTestCase;

public class VirtualThreadBeanHelperTests extends OpenSearchTestCase {

    /**
     * Tests public APIs of VirtualThreadBeanHelper.
     *
     * Three cases are possible depending on the JDK running the tests:
     * 1. jdk.management module absent: VirtualThreadSchedulerMXBean class not found — bean and all methods are null,
     *    all APIs return -1.
     * 2. Class found but bean unavailable or methods missing — bean or methods are null, all APIs return -1.
     * 3. Everything available — bean and all methods are non-null, APIs return non-negative values.
     *
     * Only one branch will execute per JDK; the test is structured this way because
     * the static initializer outcome is fixed at class-load time.
     */
    public void testVirtualThreadSchedulerStats() {
        if (VirtualThreadBeanHelper.VT_SCHEDULER_MXBEAN == null) {
            // Case 1 or 2: class not found, or bean/methods unavailable
            assertNull(VirtualThreadBeanHelper.GET_QUEUED_VT_COUNT);
            assertNull(VirtualThreadBeanHelper.GET_MOUNTED_VT_COUNT);
            assertNull(VirtualThreadBeanHelper.GET_PARALLELISM);
            assertNull(VirtualThreadBeanHelper.GET_POOL_SIZE);
            assertEquals(-1L, VirtualThreadBeanHelper.getQueuedVirtualThreadCount());
            assertEquals(-1L, VirtualThreadBeanHelper.getMountedVirtualThreadCount());
            assertEquals(-1, VirtualThreadBeanHelper.getParallelism());
            assertEquals(-1, VirtualThreadBeanHelper.getPoolSize());
        } else {
            // Case 3: bean available; check each method independently as not all may be present

            // parallelism() is always known when the bean is present
            if (VirtualThreadBeanHelper.GET_PARALLELISM != null) {
                assertTrue(VirtualThreadBeanHelper.getParallelism() > 0);
            } else {
                assertEquals(-1, VirtualThreadBeanHelper.getParallelism());
            }

            // getQueuedVirtualThreadCount(), getMountedVirtualThreadCount(), getPoolSize()
            // may return -1 even when the method is found, if the value is not known
            if (VirtualThreadBeanHelper.GET_QUEUED_VT_COUNT != null) {
                assertTrue(VirtualThreadBeanHelper.getQueuedVirtualThreadCount() >= -1);
            } else {
                assertEquals(-1L, VirtualThreadBeanHelper.getQueuedVirtualThreadCount());
            }

            if (VirtualThreadBeanHelper.GET_MOUNTED_VT_COUNT != null) {
                assertTrue(VirtualThreadBeanHelper.getMountedVirtualThreadCount() >= -1);
            } else {
                assertEquals(-1L, VirtualThreadBeanHelper.getMountedVirtualThreadCount());
            }

            if (VirtualThreadBeanHelper.GET_POOL_SIZE != null) {
                assertTrue(VirtualThreadBeanHelper.getPoolSize() >= -1);
            } else {
                assertEquals(-1, VirtualThreadBeanHelper.getPoolSize());
            }
        }
    }
}
