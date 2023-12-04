/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

/**
 * A class created by disk tier implementations containing disk-specific stats for a single request.
 */
public class DiskTierRequestStats implements TierRequestStats {

    private final long requestGetTimeNanos;
    private final boolean requestReachedDisk;

    public DiskTierRequestStats(long requestGetTimeNanos, boolean requestReachedDisk) {
        this.requestReachedDisk = requestReachedDisk;
        this.requestGetTimeNanos = requestGetTimeNanos;
    }

    @Override
    public TierType getTierType() {
        return TierType.DISK;
    }

    public long getRequestGetTimeNanos() {
        return requestGetTimeNanos;
    }

    public boolean getRequestReachedDisk() {
        return requestReachedDisk;
    }
}
