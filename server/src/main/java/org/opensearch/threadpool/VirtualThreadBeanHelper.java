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
import org.opensearch.common.settings.Setting;

import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;
import java.lang.reflect.Method;

public class VirtualThreadBeanHelper {
    static final Object VT_SCHEDULER_MXBEAN;
    static final Method GET_QUEUED_VT_COUNT;
    static final Method GET_MOUNTED_VT_COUNT;
    static final Method GET_PARALLELISM;
    static final Method GET_POOL_SIZE;
    static final Method SET_PARALLELISM;
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

        Method setParallelismMethod = null;
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
                // void setParallelism(int)
                setParallelismMethod = mxBeanClass.getMethod("setParallelism", int.class);
            } catch (Exception e) {
                bean = null;
                queuedMethod = null;
                mountedMethod = null;
                parallelismMethod = null;
                poolSizeMethod = null;
                setParallelismMethod = null;
                logger.warn("Could not access method(s) of VirtualThreadSchedulerMXBean", e);
            }
        }

        VT_SCHEDULER_MXBEAN = bean;
        GET_QUEUED_VT_COUNT = queuedMethod;
        GET_MOUNTED_VT_COUNT = mountedMethod;
        GET_PARALLELISM = parallelismMethod;
        GET_POOL_SIZE = poolSizeMethod;
        SET_PARALLELISM = setParallelismMethod;
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

    static final int MIN_PARALLELISM = 1;
    static final int MAX_PARALLELISM = 32767;

    /**
     * Cluster setting to dynamically adjust the JDK virtual thread scheduler parallelism.
     * A value of -1 (the default) means no override is applied.
     * If the JDK does not support setting parallelism, the update is a no-op and a warning is logged.
     */
    public static final Setting<Integer> SEARCH_VIRTUAL_THREADS_PARALLELISM = Setting.intSetting(
        "search_virtual_threads.parallelism",
        -1,
        -1,
        MAX_PARALLELISM,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Sets the JDK virtual thread scheduler parallelism.
     * Throws {@link IllegalArgumentException} for values outside [{@link #MIN_PARALLELISM}, {@link #MAX_PARALLELISM}].
     * Gracefully handles {@link UnsupportedOperationException} if the JDK does not support this operation.
     *
     * @param parallelism the desired parallelism value
     * @throws IllegalArgumentException if parallelism is outside the valid range
     */
    public static void setParallelism(int parallelism) {
        if (parallelism < MIN_PARALLELISM || parallelism > MAX_PARALLELISM) {
            throw new IllegalArgumentException(
                "Invalid virtual thread scheduler parallelism ["
                    + parallelism
                    + "]: must be between "
                    + MIN_PARALLELISM
                    + " and "
                    + MAX_PARALLELISM
            );
        }
        if (VT_SCHEDULER_MXBEAN == null || SET_PARALLELISM == null) {
            logger.warn("Cannot set virtual thread scheduler parallelism: VirtualThreadSchedulerMXBean is not available");
            return;
        }
        try {
            SET_PARALLELISM.invoke(VT_SCHEDULER_MXBEAN, parallelism);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnsupportedOperationException) {
                logger.warn("Cannot set virtual thread scheduler parallelism: operation not supported by this JDK", cause);
            } else {
                logger.warn("Failed to set virtual thread scheduler parallelism to [{}]", parallelism, cause);
            }
        } catch (Throwable t) {
            logger.warn("Failed to set virtual thread scheduler parallelism to [{}]", parallelism, t);
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
