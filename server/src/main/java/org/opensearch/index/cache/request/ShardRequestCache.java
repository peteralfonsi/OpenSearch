/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.cache.request;

import org.apache.lucene.util.Accountable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.bytes.BytesReference;

import java.util.EnumMap;

/**
 * Tracks the portion of the request cache in use for a particular shard.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ShardRequestCache {

    // Holds stats common to all tiers
    private final EnumMap<CacheStoreType, StatsHolder> defaultStatsHolder = new EnumMap<>(CacheStoreType.class);

    public ShardRequestCache() {
        for (CacheStoreType cacheStoreType : CacheStoreType.values()) {
            defaultStatsHolder.put(cacheStoreType, new StatsHolder());
        }
    }

    public RequestCacheStats stats() {
        return new RequestCacheStats(defaultStatsHolder);
    }

    public void onHit(CacheStoreType cacheStoreType) {
        defaultStatsHolder.get(cacheStoreType).hitCount.inc();
    }

    public void onMiss(CacheStoreType cacheStoreType) {
        defaultStatsHolder.get(cacheStoreType).missCount.inc();
    }

    public void onCached(Accountable key, BytesReference value, CacheStoreType cacheStoreType) {
        long valueByteSize;
        if (cacheStoreType == CacheStoreType.DISK) {
            valueByteSize = value.length(); // Ehcache trims trailing zeros from incoming byte[]
        } else {
            valueByteSize = value.ramBytesUsed();
        }
        defaultStatsHolder.get(cacheStoreType).totalMetric.inc(key.ramBytesUsed() + valueByteSize);
        defaultStatsHolder.get(cacheStoreType).entries.inc();
    }

    public void onRemoval(Accountable key, BytesReference value, boolean evicted) {
        onRemoval(key, value, evicted, CacheStoreType.ON_HEAP); // By default On heap cache.
    }

    public void onRemoval(Accountable key, BytesReference value, boolean evicted, CacheStoreType cacheStoreType) {
        if (evicted) {
            defaultStatsHolder.get(cacheStoreType).evictionsMetric.inc();
        }
        long dec = 0;
        if (key != null) {
            dec += key.ramBytesUsed();
        }
        if (value != null) {
            dec += value.ramBytesUsed();
        }
        defaultStatsHolder.get(cacheStoreType).totalMetric.dec(dec);
        defaultStatsHolder.get(cacheStoreType).entries.dec();
    }
}
