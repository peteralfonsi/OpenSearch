/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.request;

import org.opensearch.common.cache.tier.DiskTierRequestStats;
import org.opensearch.test.OpenSearchTestCase;

public class ShardRequestCacheTests extends OpenSearchTestCase {
    // Serialization and getter logic is implicitly tested in RequestCacheStatsTests.java,
    // in this file, check logic for StatsHolder.TierStatsAccumulator implementations

    public void testInit() throws Exception {
        ShardRequestCache src = new ShardRequestCache();
        RequestCacheStats rcs = src.stats();
    }

    public void testDiskStatsAccumulator() throws Exception {
        ShardRequestCache.DiskStatsAccumulator acc = new ShardRequestCache.DiskStatsAccumulator();
        DiskTierRequestStats reachedDiskReqStats = new DiskTierRequestStats(145L, true, false, 100L, 0.7);
        acc.addRequestStats(reachedDiskReqStats);
        assertEquals(1, acc.getTotalDiskReaches());
        assertEquals(145, acc.getTotalGetTime());
        assertFalse(acc.keystoreIsFull);
        assertEquals(100L, acc.keystoreWeight);
        assertEquals(0.7, acc.staleKeyThreshold, 0.001);
        DiskTierRequestStats noDiskReqStats = new DiskTierRequestStats(391392L, false, true, 102L, 0.6);
        acc.addRequestStats(noDiskReqStats);
        assertEquals(1, acc.getTotalDiskReaches());
        assertEquals(145, acc.getTotalGetTime());
        assertTrue(acc.keystoreIsFull);
        assertEquals(102L, acc.keystoreWeight);
        assertEquals(0.6, acc.staleKeyThreshold, 0.001);

        ShardRequestCache.DiskStatsAccumulator other = new ShardRequestCache.DiskStatsAccumulator();
        other.addRequestStats(new DiskTierRequestStats(1L, true, true, 102L, 0.6));
        acc.add(other);
        assertEquals(146, acc.getTotalGetTime());
        assertEquals(2, acc.getTotalDiskReaches());
        assertTrue(acc.keystoreIsFull);
        assertEquals(102L, acc.keystoreWeight);
        assertEquals(0.6, acc.staleKeyThreshold, 0.001);
    }
}
