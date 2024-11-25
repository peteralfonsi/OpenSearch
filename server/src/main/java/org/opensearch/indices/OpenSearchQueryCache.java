/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.apache.lucene.search.QueryCache;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.cache.query.QueryCacheStats;

import java.io.Closeable;

/**
 * An interface for implementations of the indices query cache.
 */
@ExperimentalApi
public interface OpenSearchQueryCache extends QueryCache, Closeable {

    /** Get usage statistics for the given shard. */
    QueryCacheStats getStats(ShardId shard);

    /** Clear all entries that belong to the given index. */
    void clearIndex(String index);

    void onClose(ShardId shardId);

}
