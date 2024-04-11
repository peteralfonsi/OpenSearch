/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;

import java.io.IOException;

/**
 * Interface for access to any cache stats. Allows accessing stats by dimension values.
 * Stores an immutable snapshot of stats for a cache. The cache maintains its own live counters.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CacheStats extends Writeable, ToXContentFragment {

    // Method to get all 5 values at once
    CacheStatsCounterSnapshot getTotalStats();

    // Methods to get total values.
    long getTotalHits();

    long getTotalMisses();

    long getTotalEvictions();

    long getTotalSizeInBytes();

    long getTotalEntries();

    // Used for the readFromStream method to allow deserialization of generic CacheStats objects.
    String getClassName();

    void writeToWithClassName(StreamOutput out) throws IOException;

    static CacheStats readFromStreamWithClassName(StreamInput in) throws IOException {
        String className = in.readString();

        if (className.equals(MultiDimensionCacheStats.CLASS_NAME)) {
            return new MultiDimensionCacheStats(in);
        }
        return null;
    }
}
