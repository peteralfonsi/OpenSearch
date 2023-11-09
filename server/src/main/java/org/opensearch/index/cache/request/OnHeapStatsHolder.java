/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.request;

import org.opensearch.common.cache.tier.TierRequestStats;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;


/**
 * This class accumulates stats for a single shard, for the on-heap tier.
 * For now, on-heap tier has no unique stats, but future stats would be added here.
 */
public class OnHeapStatsHolder extends StatsHolder {
    public OnHeapStatsHolder() {
        super();
    }

    public OnHeapStatsHolder(long memorySize, long evictions, long hitCount, long missCount, long entries) {
        super(memorySize, evictions, hitCount, missCount, entries);
    }

    public OnHeapStatsHolder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void addRequestStats(TierRequestStats stats) {}

}
