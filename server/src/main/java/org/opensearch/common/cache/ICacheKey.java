/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.cache.stats.CacheStatsDimension;

import java.util.Collections;
import java.util.List;

public class ICacheKey<K> {
    public final K key; // K must implement equals()
    public final List<CacheStatsDimension> dimensions;

    public ICacheKey(K key, List<CacheStatsDimension> dimensions) {
        this.key = key;
        this.dimensions = dimensions;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (o.getClass() != ICacheKey.class) {
            return false;
        }
        ICacheKey other = (ICacheKey) o;
        return key.equals(other.key) && dimensions.equals(other.dimensions);
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + dimensions.hashCode();
    }

    public long dimensionBytesEstimate() {
        long estimate = 0L;
        for (CacheStatsDimension dim : dimensions) {
            estimate += dim.dimensionName.length() + dim.dimensionValue.length();
        }
        return estimate;
    }
}
