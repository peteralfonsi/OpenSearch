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
import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.tier.CacheValue;
import org.opensearch.common.cache.tier.DiskTierRequestStats;
import org.opensearch.common.cache.tier.OnHeapTierRequestStats;
import org.opensearch.common.cache.tier.TierRequestStats;
import org.opensearch.common.cache.tier.enums.CacheStoreType;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.bytes.BytesReference;

import java.util.EnumMap;

/**
 * Tracks the portion of the request cache in use for a particular shard.
 *
 * @opensearch.internal
 */
public final class ShardRequestCache {

    // Holds stats common to all tiers
    private final EnumMap<CacheStoreType, StatsHolder> defaultStatsHolder = new EnumMap<>(CacheStoreType.class);

    // Holds tier-specific stats
    private final EnumMap<CacheStoreType, TierStatsAccumulator> tierSpecificStatsHolder = new EnumMap<>(CacheStoreType.class);

    public ShardRequestCache() {
        tierSpecificStatsHolder.put(CacheStoreType.ON_HEAP, new OnHeapStatsAccumulator());
        tierSpecificStatsHolder.put(CacheStoreType.DISK, new DiskStatsAccumulator());
        for (CacheStoreType cacheStoreType : CacheStoreType.values()) {
            defaultStatsHolder.put(cacheStoreType, new StatsHolder());
            if (tierSpecificStatsHolder.get(cacheStoreType) == null) {
                throw new OpenSearchException("Missing TierStatsAccumulator for TierType " + cacheStoreType.getStringValue());
            }
        }
    }

    public RequestCacheStats stats() {
        // TODO: Change RequestCacheStats to support disk tier stats.
        return new RequestCacheStats(
            statsHolder.get(TierType.ON_HEAP).totalMetric.count(),
            statsHolder.get(TierType.ON_HEAP).evictionsMetric.count(),
            statsHolder.get(TierType.ON_HEAP).hitCount.count(),
            statsHolder.get(TierType.ON_HEAP).missCount.count()
        );
    }

    public void onHit(CacheValue<BytesReference> cacheValue) {
        CacheStoreType source = cacheValue.getSource();
        defaultStatsHolder.get(source).hitCount.inc();
        tierSpecificStatsHolder.get(source).addRequestStats(cacheValue.getStats());
    }

    public void onMiss(CacheValue<BytesReference> cacheValue) {
        CacheStoreType source = cacheValue.getSource();
        defaultStatsHolder.get(source).missCount.inc();
        tierSpecificStatsHolder.get(source).addRequestStats(cacheValue.getStats());
    }

    public void onCached(Accountable key, BytesReference value, CacheStoreType cacheStoreType) {
        defaultStatsHolder.get(cacheStoreType).totalMetric.inc(key.ramBytesUsed() + value.ramBytesUsed());
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

    static class StatsHolder {

        final CounterMetric evictionsMetric = new CounterMetric();
        final CounterMetric totalMetric = new CounterMetric();
        final CounterMetric hitCount = new CounterMetric();
        final CounterMetric missCount = new CounterMetric();
    }
}
