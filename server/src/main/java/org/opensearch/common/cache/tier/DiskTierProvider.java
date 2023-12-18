/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

/**
 * An interface that provides new instances of a DiskTier.
 */
public interface DiskTierProvider<K, V> {
    DiskCachingTier<K, V> createNewDiskTier();
}
