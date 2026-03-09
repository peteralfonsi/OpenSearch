/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;
import java.lang.reflect.Method;

public class VirtualThreadBeanHelper {
    static final Object VT_SCHEDULER_MXBEAN;
    static final Method GET_QUEUED_VT_COUNT;
    static final Method GET_MOUNTED_VT_COUNT;
    static final Method GET_PARALLELISM;
    static final Method GET_POOL_SIZE;
    private static final Logger logger = LogManager.getLogger(VirtualThreadBeanHelper.class);

    static {
        Object bean = null;
        Method queuedMethod = null;
        Method mountedMethod = null;
        Method parallelismMethod = null;
        Method poolSizeMethod = null;
        Class<? extends PlatformManagedObject> mxBeanClass = null;
        try {
            // Load class without linking at compile time
            mxBeanClass = (Class<? extends PlatformManagedObject>) Class.forName(
                "jdk.management.VirtualThreadSchedulerMXBean",
                false,
                ClassLoader.getSystemClassLoader()
            );

            bean = ManagementFactory.getPlatformMXBean(mxBeanClass);
        } catch (Exception e) {
            // Swallow exceptions if we can't access the bean through reflection, in this case APIs will return -1
            logger.warn("Could not access VirtualThreadSchedulerMXBean", e);
        }

        if (mxBeanClass != null) {
            try {
                // long getQueuedVirtualThreadCount()
                queuedMethod = mxBeanClass.getMethod("getQueuedVirtualThreadCount");
                // long getMountedVirtualThreadCount()
                mountedMethod = mxBeanClass.getMethod("getMountedVirtualThreadCount");
                // int getParallelism()
                parallelismMethod = mxBeanClass.getMethod("getParallelism");
                // int getPoolSize()
                poolSizeMethod = mxBeanClass.getMethod("getPoolSize");
            } catch (Exception e) {
                bean = null;
                queuedMethod = null;
                mountedMethod = null;
                parallelismMethod = null;
                poolSizeMethod = null;
                logger.warn("Could not access method(s) of VirtualThreadSchedulerMXBean", e);
            }
        }

        VT_SCHEDULER_MXBEAN = bean;
        GET_QUEUED_VT_COUNT = queuedMethod;
        GET_MOUNTED_VT_COUNT = mountedMethod;
        GET_PARALLELISM = parallelismMethod;
        GET_POOL_SIZE = poolSizeMethod;
    }

    private VirtualThreadBeanHelper() {}

    /**
     * @return queued virtual thread count, or -1 if unavailable
     */
    public static long getQueuedVirtualThreadCount() {
        if (VT_SCHEDULER_MXBEAN == null || GET_QUEUED_VT_COUNT == null) {
            return -1;
        }
        try {
            return (long) GET_QUEUED_VT_COUNT.invoke(VT_SCHEDULER_MXBEAN);
        } catch (Throwable t) {
            return -1;
        }
    }

    /**
     * @return mounted virtual thread count, or -1 if unavailable
     */
    public static long getMountedVirtualThreadCount() {
        if (VT_SCHEDULER_MXBEAN == null || GET_MOUNTED_VT_COUNT == null) {
            return -1;
        }
        try {
            return (long) GET_MOUNTED_VT_COUNT.invoke(VT_SCHEDULER_MXBEAN);
        } catch (Throwable t) {
            return -1;
        }
    }

    /**
     * @return parallelism, or -1 if unavailable
     */
    public static int getParallelism() {
        if (VT_SCHEDULER_MXBEAN == null || GET_PARALLELISM == null) {
            return -1;
        }
        try {
            return (int) GET_PARALLELISM.invoke(VT_SCHEDULER_MXBEAN);
        } catch (Throwable t) {
            return -1;
        }
    }

    /**
     * @return pool size, or -1 if unavailable
     */
    public static int getPoolSize() {
        if (VT_SCHEDULER_MXBEAN == null || GET_POOL_SIZE == null) {
            return -1;
        }
        try {
            return (int) GET_POOL_SIZE.invoke(VT_SCHEDULER_MXBEAN);
        } catch (Throwable t) {
            return -1;
        }
    }
}
