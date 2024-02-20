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
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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


    private final EnumMap<CacheType, ICache> cacheMap;
    private final IndicesService indicesService; // Used to map indices to shards for request cache

    public CacheService(IndicesService indicesService) {
        this.indicesService = indicesService;
        this.cacheMap = new EnumMap<>(CacheType.class);
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

    // pkg private for testing
    ICache getCache(CacheType cacheType) {
        assert cacheMap.containsKey(cacheType) : "No registered ICache for " + cacheType.toString();
        return cacheMap.get(cacheType);
    }

    // for testing
    IndicesService getIndicesService() {
        return indicesService;
    }

    /**
     * Returns a list of aggregated CacheStatsResponse objects, bundled with the dimensions that apply to them
     */
    AggregatedStats getStats(CacheType cacheType, List<CacheStatsDimension> dimensions) {
        // NodeCacheStats calls this when it's running toXContent
        switch (cacheType) {
            case INDICES_REQUEST_CACHE:
                return getRequestCacheStats(dimensions);
            default:
                return null;
        }
    }

    AggregatedStats getRequestCacheStats(List<CacheStatsDimension> dimensions) {
        // Allowed combinations are: empty list, "shardId", "tier", "indices", "tier" + "indices", "tier + shard"
        ICache cache = getCache(CacheType.INDICES_REQUEST_CACHE);

        HashSet<String> dimensionNames = new HashSet<>();
        List<String> allowedNames = List.of(INDICES_DIMENSION_NAME, SHARDS_DIMENSION_NAME, TIER_DIMENSION_NAME);
        for (CacheStatsDimension dim : dimensions) {
            if (allowedNames.contains(dim.dimensionName)) {
                dimensionNames.add(dim.dimensionName);
                // TODO: What to do with unrecognized names? For now, ignoring them.
            }
        }

        if (dimensionNames.isEmpty()) {
            // Don't aggregate by anything; return totals.
            AggregatedStats totalStats = new AggregatedStats(List.of());
            totalStats.put(List.of(), cache.stats().getTotalStats());
            return totalStats; //List.of();
        }

        else if (dimensionNames.equals(Set.of(TIER_DIMENSION_NAME))) {
            // Aggregate by tiers
            AggregatedStats tierStats = new AggregatedStats(List.of(CacheService.TIER_DIMENSION_NAME));
            tierStats.put(List.of(CacheService.TIER_DIMENSION_VALUE_ON_HEAP), cache.stats().getStatsByDimensions(TIER_ON_HEAP_DIMS));
            tierStats.put(List.of(CacheService.TIER_DIMENSION_VALUE_DISK), cache.stats().getStatsByDimensions(TIER_DISK_DIMS));
            return tierStats;
        }

        else if (dimensionNames.equals(Set.of(SHARDS_DIMENSION_NAME))) {
            // Aggregate by shards
            AggregatedStats shardStats = new AggregatedStats(List.of(CacheService.SHARDS_DIMENSION_NAME));
            for (final IndexService indexService : indicesService) {
                for (final IndexShard indexShard : indexService) {
                    String indexShardName = getShardName(indexShard);
                    CacheStatsDimension shardDimension = new CacheStatsDimension(SHARDS_DIMENSION_NAME, indexShardName);
                    shardStats.put(List.of(indexShardName), cache.stats().getStatsByDimensions(List.of(shardDimension)));
                }
            }
            return shardStats;
        }

        else if (dimensionNames.equals(Set.of(INDICES_DIMENSION_NAME))) {
            // Aggregate by indices
            AggregatedStats indicesStats = new AggregatedStats(List.of(CacheService.INDICES_DIMENSION_NAME));
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
                indicesStats.put(List.of(indexName), summedIndexShardResponses);
            }
            return indicesStats;
        }

        else if (dimensionNames.equals(Set.of(INDICES_DIMENSION_NAME, TIER_DIMENSION_NAME))) {
            // Aggregate by indices and tiers
            AggregatedStats indicesAndTierStats = new AggregatedStats(List.of(CacheService.INDICES_DIMENSION_NAME, CacheService.TIER_DIMENSION_NAME));
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
                    indicesAndTierStats.put(List.of(indexName, tier), summedIndexShardResponsesMap.get(tier));
                }
            }
            return indicesAndTierStats;
        }

        else if (dimensionNames.equals(Set.of(SHARDS_DIMENSION_NAME, TIER_DIMENSION_NAME))) {
            // Aggregate by shards and tiers
            AggregatedStats shardsAndTierStats = new AggregatedStats(List.of(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_NAME));
            for (final IndexService indexService : indicesService) {
                for (final IndexShard indexShard : indexService) {
                    String indexShardName = getShardName(indexShard);
                    CacheStatsDimension shardDimension = new CacheStatsDimension(SHARDS_DIMENSION_NAME, indexShardName);
                    for (String tier : API_SUPPORTED_TIERS) {
                        CacheStatsDimension tierDimension = new CacheStatsDimension(TIER_DIMENSION_NAME, tier);
                        shardsAndTierStats.put(List.of(indexShardName, tier), cache.stats().getStatsByDimensions(List.of(shardDimension, tierDimension)));
                    }
                }
            }
            return shardsAndTierStats;
        }
        // TODO: Return some error
        return null;
    }

    // pkg-private for testing
    String getIndexName(IndexService indexService) {
        // TODO: Is this correct?
        return indexService.getIndexSettings().getIndex().getName();
    }

    // pkg-private for testing
    String getShardName(IndexShard indexShard) {
        // TODO: Is this correct?
        return indexShard.shardId().toString();
    }

    String getIndexNameFromShardName(String shardName) {
        String[] parts = shardName.split("\\[");
        return parts[1].split("]")[0];
    }

    NodeCacheStats stats() {
        return new NodeCacheStats(this);
    }

    /**
     * Holds potentially nested maps from the aggregated dimension values to the cache stats responses. The values are ordered by insertion order.
     * For example, if we aggregate by indices and tiers, the outer map's keys are index names, and the inner ones are tier names.
     */
    static class AggregatedStats {
        private final LinkedHashMap<String, Object> outerMap; // Object will be either another map or a CacheStatsResponse object for the innermost map

        // Ordered list of dimension names, for example "tier", "indices", "shards"
        // Not directly used to access values, but useful for consumers to know what dimension the values are associated with
        private final List<String> dimensionNames;

        private int size; // Total number of entries

        public AggregatedStats(List<String> dimensionNames) {
            this.dimensionNames = dimensionNames;
            this.outerMap = new LinkedHashMap<>();
            this.size = 0;
        }

        public List<String> getDimensionNames() {
            return dimensionNames;
        }

        public void put(List<String> dimensionValues, CacheStatsResponse response) {
            assert dimensionValues.size() == dimensionNames.size();
            LinkedHashMap<String, Object> currentMap = outerMap;
            if (dimensionNames.isEmpty()) {
                currentMap.put("", response);
                return;
            }
            for (int i = 0; i < dimensionValues.size(); i++) {
                String dimensionValue = dimensionValues.get(i);
                if (i < dimensionValues.size() - 1) {
                    // Walk through nested maps
                    Object entry = currentMap.get(dimensionValue);
                    if (entry == null) {
                        entry = new LinkedHashMap<String, Object>();
                        currentMap.put(dimensionValue, entry);
                    }
                    currentMap = (LinkedHashMap<String, Object>) entry;
                } else {
                    // Add the value to the innermost map, ensuring we don't overwrite an existing value
                    CacheStatsResponse previousValue = (CacheStatsResponse) currentMap.put(dimensionValue, response);
                    assert previousValue == null : "Cannot overwrite existing value for " + dimensionValues;
                    size++;
                }
            }
        }

        public CacheStatsResponse getResponse(List<String> dimensionValues) {
            assert dimensionValues.size() == dimensionNames.size();
            LinkedHashMap<String, Object> currentMap = outerMap;
            if (dimensionNames.isEmpty()) {
                return (CacheStatsResponse) currentMap.get("");
            }
            for (int i = 0; i < dimensionValues.size(); i++) {
                String dimensionValue = dimensionValues.get(i);
                Object entry = currentMap.get(dimensionValue);
                assert entry != null : "No nested map or response for value " + dimensionValue;
                if (i < dimensionValues.size() - 1) {
                    currentMap = (LinkedHashMap<String, Object>) entry;
                } else {
                    return (CacheStatsResponse) entry;
                }
            }
            return null;
        }

        /**
         * Returns a list of the keys of the map specified by dimensionValues.
         * The keys are returned in insertion order.
         */
        public List<String> getInnerMapKeySet(List<String> dimensionValues) {
            assert dimensionValues.size() < dimensionNames.size() : "Too many values passed in as nested keys";
            LinkedHashMap<String, Object> currentMap = outerMap;
            for (int i = 0; i < dimensionValues.size(); i++) {
                String dimensionValue = dimensionValues.get(i);
                Object entry = currentMap.get(dimensionValue);
                assert entry != null : "No nested map for value " + dimensionValue;
                currentMap = (LinkedHashMap<String, Object>) entry;
            }
            return new ArrayList<>(currentMap.keySet());
        }

        public int getSize() {
            return size;
        }
    }
}
