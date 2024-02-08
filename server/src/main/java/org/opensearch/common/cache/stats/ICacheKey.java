/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.List;

public class ICacheKey<K> {
    public final K key;
    public final List<CacheStatsDimension> dimensions;

    public ICacheKey(K key, List<CacheStatsDimension> dimensions) {
        this.key = key;
        this.dimensions = dimensions;
    }
}
