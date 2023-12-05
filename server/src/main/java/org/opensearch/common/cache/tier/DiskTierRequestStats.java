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

    // values specific to this single request
    private final long requestGetTimeNanos;
    private final boolean requestReachedDisk;

    // overall values, which are updated here
    private final boolean keystoreIsFull;
    private final long keystoreWeight;
    private final double staleKeyThreshold;

    public DiskTierRequestStats(long requestGetTimeNanos, boolean requestReachedDisk, boolean keystoreIsFull, long keystoreWeight, double staleKeyThreshold) {
        this.requestReachedDisk = requestReachedDisk;
        this.requestGetTimeNanos = requestGetTimeNanos;
        // The below values are not request-specific, but are being updated at the time of this request
        this.keystoreIsFull = keystoreIsFull;
        this.keystoreWeight = keystoreWeight;
        this.staleKeyThreshold = staleKeyThreshold;
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
    public boolean getKeystoreIsFull() {
        return keystoreIsFull;
    }
    public long getKeystoreWeight() {
        return keystoreWeight;
    }
    public double getStaleKeyThreshold() {
        return staleKeyThreshold;
    }
}
