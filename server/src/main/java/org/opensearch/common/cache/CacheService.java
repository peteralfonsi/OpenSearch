/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.CacheStatsResponse;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A service used for aggregating and returning stats for OpenSearch caches.
 */
public class CacheService extends AbstractLifecycleComponent {
    // Common values for dimension names and values below:
    public static final String TIER_DIMENSION_NAME = "tier";
    public static final String TIER_DIMENSION_VALUE_ON_HEAP = "on_heap";
    public static final String TIER_DIMENSION_VALUE_DISK = "disk";
    public static final List<CacheStatsDimension> TIER_ON_HEAP_DIMS = List.of(new CacheStatsDimension(TIER_DIMENSION_NAME, TIER_DIMENSION_VALUE_ON_HEAP));
    public static final List<CacheStatsDimension> TIER_DISK_DIMS = List.of(new CacheStatsDimension(TIER_DIMENSION_NAME, TIER_DIMENSION_VALUE_DISK));

    // TODO: This is a placeholder - probably this should be defined elsewhere
    public static final List<String> API_SUPPORTED_TIERS = List.of(TIER_DIMENSION_VALUE_ON_HEAP, TIER_DIMENSION_VALUE_DISK);


    public static final String INDICES_DIMENSION_NAME = "indices";
    public static final String SHARDS_DIMENSION_NAME = "shardId";

    private EnumMap<CacheType, ICache> cacheMap;

    private IndicesService indicesService; // Used to map indices to shards for request cache

    public CacheService(IndicesService indicesService) {
        this.indicesService = indicesService;
    }
    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    public void registerCache(CacheType cacheType, ICache cache) {
        assert (!cacheMap.containsKey(cacheType)) : "Cache type " + cacheType.toString() + " already has a registered ICache";
        assert (cache != null) : "Registered cache cannot be null";
        cacheMap.put(cacheType, cache);
    }

    public void deregisterCache(CacheType cacheType) {
        // Is this needed?
        cacheMap.remove(cacheType);
    }

    private ICache getCache(CacheType cacheType) {
        assert cacheMap.containsKey(cacheType) : "No registered ICache for " + cacheType.toString();
        return cacheMap.get(cacheType);
    }

    /**
     * Returns a list of aggregated CacheStatsResponse objects, bundled with the dimensions that apply to them
     */
    List<ResponseAndDimensions> getStats(CacheType cacheType, List<CacheStatsDimension> dimensions) {
        // NodeCacheStats calls this when it's running toXContent
        switch (cacheType) {
            case INDICES_REQUEST_CACHE:
                return getRequestCacheStats(dimensions);
            default:
                return null;
        }
    }

    List<ResponseAndDimensions> getRequestCacheStats(List<CacheStatsDimension> dimensions) {
        // Allowed combinations are: empty list, "shardId", "tier", "indices", "tier" + "indices", "tier + shard"
        ICache cache = getCache(CacheType.INDICES_REQUEST_CACHE);

        HashSet<String> dimensionNames = new HashSet<>();
        List<String> allowedNames = List.of(INDICES_DIMENSION_NAME, SHARDS_DIMENSION_NAME, TIER_DIMENSION_NAME);
        for (CacheStatsDimension dim : dimensions) {
            if (allowedNames.contains(dim.dimensionValue)) {
                dimensionNames.add(dim.dimensionValue);
                // TODO: What to do with unrecognized names? For now, ignoring them.
            }
        }
        if (dimensionNames.isEmpty()) {
            // Don't aggregate by anything; return totals.
            return List.of(new ResponseAndDimensions(cache.stats().getTotalStats(), new HashSet<>()));
        } else if (dimensionNames.equals(Set.of(TIER_DIMENSION_NAME))) {
            // Aggregate by tiers
            return List.of(
                new ResponseAndDimensions(
                    cache.stats().getStatsByDimensions(TIER_ON_HEAP_DIMS),
                    Set.of(TIER_ON_HEAP_DIMS.get(0))
                ),
                new ResponseAndDimensions(
                    cache.stats().getStatsByDimensions(TIER_DISK_DIMS),
                    Set.of(TIER_DISK_DIMS.get(0))
                )
            );
        } else if (dimensionNames.equals(Set.of(SHARDS_DIMENSION_NAME))) {
            // Aggregate by shards
            List<ResponseAndDimensions> result = new ArrayList<>();
            for (final IndexService indexService : indicesService) {
                for (final IndexShard indexShard : indexService) {
                    String indexShardName = getShardName(indexShard);
                    CacheStatsDimension shardDimension = new CacheStatsDimension(SHARDS_DIMENSION_NAME, indexShardName);
                    result.add(
                        new ResponseAndDimensions(
                            cache.stats().getStatsByDimensions(List.of(shardDimension)),
                            Set.of(shardDimension)
                        )
                    );
                }
            }
            return result;
        } else if (dimensionNames.equals(Set.of(INDICES_DIMENSION_NAME))) {
            // Aggregate by indices
            List<ResponseAndDimensions> result = new ArrayList<>();
            for (final IndexService indexService : indicesService) {
                String indexName = getIndexName(indexService);
                CacheStatsResponse summedIndexShardResponses = null;
                for (final IndexShard indexShard : indexService) {
                    String indexShardName = getShardName(indexShard);
                    CacheStatsDimension shardDimension = new CacheStatsDimension(SHARDS_DIMENSION_NAME, indexShardName);
                    CacheStatsResponse response = cache.stats().getStatsByDimensions(List.of(shardDimension));
                    if (summedIndexShardResponses == null) {
                        summedIndexShardResponses = response;
                    } else {
                        summedIndexShardResponses = summedIndexShardResponses.add(response);
                    }
                }
                result.add(
                    new ResponseAndDimensions(
                        summedIndexShardResponses,
                        Set.of(new CacheStatsDimension(INDICES_DIMENSION_NAME, indexName))
                    )
                );
            }
            return result;
        } else if (dimensionNames.equals(Set.of(INDICES_DIMENSION_NAME, TIER_DIMENSION_NAME))) {
            // Aggregate by indices and tiers
            List<ResponseAndDimensions> result = new ArrayList<>();
            for (final IndexService indexService : indicesService) {
                String indexName = getIndexName(indexService);
                Map<String, CacheStatsResponse> summedIndexShardResponsesMap = new HashMap<>();
                for (final IndexShard indexShard : indexService) {
                    String indexShardName = getShardName(indexShard);
                    CacheStatsDimension shardDimension = new CacheStatsDimension(SHARDS_DIMENSION_NAME, indexShardName);
                    for (String tier : API_SUPPORTED_TIERS) {
                        CacheStatsDimension tierDimension = new CacheStatsDimension(TIER_DIMENSION_NAME, tier);
                        CacheStatsResponse response = cache.stats().getStatsByDimensions(List.of(shardDimension, tierDimension));

                        CacheStatsResponse currentSum = summedIndexShardResponsesMap.get(tier);
                        if (currentSum == null) {
                            summedIndexShardResponsesMap.put(tier, response);
                        } else {
                            summedIndexShardResponsesMap.put(tier, currentSum.add(response));
                        }
                    }
                }
                for (String tier : API_SUPPORTED_TIERS) {
                    result.add(
                        new ResponseAndDimensions(
                            summedIndexShardResponsesMap.get(tier),
                            Set.of(new CacheStatsDimension(INDICES_DIMENSION_NAME, indexName), new CacheStatsDimension(TIER_DIMENSION_NAME, tier))
                        )
                    );
                    // We could switch the order of the shard/tier for loops, but this way we keep the order it'll be in for the API response.
                    // TBD if that's gonna matter or not to the NodeCacheStats implementation.
                }
            }
            return result;
        } else if (dimensionNames.equals(Set.of(SHARDS_DIMENSION_NAME, TIER_DIMENSION_NAME))) {
            // Aggregate by shards and tiers
            List<ResponseAndDimensions> result = new ArrayList<>();
            for (final IndexService indexService : indicesService) {
                for (final IndexShard indexShard : indexService) {
                    String indexShardName = getShardName(indexShard);
                    CacheStatsDimension shardDimension = new CacheStatsDimension(SHARDS_DIMENSION_NAME, indexShardName);
                    for (String tier : API_SUPPORTED_TIERS) {
                        CacheStatsDimension tierDimension = new CacheStatsDimension(TIER_DIMENSION_NAME, tier);
                        result.add(
                            new ResponseAndDimensions(
                                cache.stats().getStatsByDimensions(List.of(shardDimension, tierDimension)),
                                Set.of(shardDimension, tierDimension)
                            )
                        );
                    }
                }
            }
            return result;
        }
        // TODO: Return some error
        return null;
    }

    private String getIndexName(IndexService indexService) {
        // TODO: Is this correct?
        return indexService.getIndexSettings().getIndex().getName();
    }

    private String getShardName(IndexShard indexShard) {
        // TODO: Is this correct?
        return indexShard.toString();
    }

    NodeCacheStats stats() {
        return new NodeCacheStats(this);
    }

    /**
     * Contains the CacheStatsResponse and the dimensions associated with that response.
     * This can't be bundled into the CacheStatsResponse, because (for example) the request
     * cache CacheStats implementations don't know what index a request was for.
     */
    class ResponseAndDimensions {
        CacheStatsResponse response;
        Set<CacheStatsDimension> dimensions;
        ResponseAndDimensions(CacheStatsResponse response, Set<CacheStatsDimension> dimensions) {
            this.response = response;
            this.dimensions = dimensions;
        }
    }
}
