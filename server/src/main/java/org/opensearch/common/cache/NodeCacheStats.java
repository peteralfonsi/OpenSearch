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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NodeCacheStats implements ToXContentFragment, Writeable {
    // TODO: Rework this to take two enum maps from CacheType to stats/totalStats
    private CacheService.AggregatedStats requestStats;
    private CacheStatsResponse totalRequestStats; // Pass this in to avoid re-summing over all values
    // More stats will go here as more caches are integrated

    public NodeCacheStats(CacheService.AggregatedStats requestStats, CacheStatsResponse totalRequestStats) {
        this.requestStats = requestStats;
        this.totalRequestStats = totalRequestStats;
    }

    public NodeCacheStats(StreamInput in) throws IOException {
        this.requestStats = new CacheService.AggregatedStats(in);
        this.totalRequestStats = new CacheStatsResponse(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // TODO
        return null;
    }

    // For testing
    CacheStatsResponse getTotalRequestStats() {
        return totalRequestStats;
    }

    /**
     * Create a new AggregatedStats for the request cache, which is aggregated based on the levels specified.
     */
    CacheService.AggregatedStats aggregateRequestStatsByLevel(String[] levels) {
        checkRequestStatsDimensionNames();
        if (levels == null || checkLevelsEquality(levels, Set.of())) {
            CacheService.AggregatedStats totalStats = new CacheService.AggregatedStats(List.of());
            totalStats.put(List.of(), totalRequestStats);
            return totalStats;
        }
        if (checkLevelsEquality(levels, Set.of("tier"))) {
            CacheService.AggregatedStats tierStats = new CacheService.AggregatedStats(List.of(CacheService.TIER_DIMENSION_NAME));
            for (String tierName : CacheService.API_SUPPORTED_TIERS) {
                tierStats.put(List.of(tierName), new CacheStatsResponse());
            }

            for (String indexName : requestStats.getInnerMapKeySet(List.of())) {
                for (String shardName : requestStats.getInnerMapKeySet(List.of(indexName))) {
                    for (String tierName : requestStats.getInnerMapKeySet(List.of(indexName, shardName))) {
                        CacheStatsResponse response = requestStats.getResponse(List.of(indexName, shardName, tierName));
                        tierStats.addTo(List.of(tierName), response);
                    }
                }
            }
            return tierStats;
        }
        if (checkLevelsEquality(levels, Set.of("shards"))) {
            CacheService.AggregatedStats shardStats = new CacheService.AggregatedStats(List.of(CacheService.SHARDS_DIMENSION_NAME));
            for (String indexName : requestStats.getInnerMapKeySet(List.of())) {
                for (String shardName : requestStats.getInnerMapKeySet(List.of(indexName))) {
                    shardStats.put(List.of(shardName), sumRequestCacheShard(indexName, shardName));
                }
            }
            return shardStats;
        }
        if (checkLevelsEquality(levels, Set.of("indices"))) {
            CacheService.AggregatedStats indicesStats = new CacheService.AggregatedStats(List.of(CacheService.INDICES_DIMENSION_NAME));
            for (String indexName : requestStats.getInnerMapKeySet(List.of())) {
                indicesStats.put(List.of(indexName), new CacheStatsResponse());
                for (String shardName : requestStats.getInnerMapKeySet(List.of(indexName))) {
                    indicesStats.addTo(List.of(indexName), sumRequestCacheShard(indexName, shardName));
                }
            }
            return indicesStats;
        }
        if (checkLevelsEquality(levels, Set.of("shards", "tier"))) {
            CacheService.AggregatedStats shardAndTierStats = new CacheService.AggregatedStats(List.of(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_NAME));
            for (String indexName : requestStats.getInnerMapKeySet(List.of())) {
                for (String shardName : requestStats.getInnerMapKeySet(List.of(indexName))) {
                    for (String tierName : requestStats.getInnerMapKeySet(List.of(indexName, shardName))) {
                        shardAndTierStats.put(List.of(shardName, tierName), requestStats.getResponse(List.of(indexName, shardName, tierName)));
                    }
                }
            }
            return shardAndTierStats;
        }
        if (checkLevelsEquality(levels, Set.of("indices", "tier"))) {
            CacheService.AggregatedStats indicesAndTierStats = new CacheService.AggregatedStats(List.of(CacheService.INDICES_DIMENSION_NAME, CacheService.TIER_DIMENSION_NAME));
            for (String indexName : requestStats.getInnerMapKeySet(List.of())) {
                for (String tierName : CacheService.API_SUPPORTED_TIERS) {
                    indicesAndTierStats.put(List.of(indexName, tierName), new CacheStatsResponse());
                }
                for (String shardName : requestStats.getInnerMapKeySet(List.of(indexName))) {
                    for (String tierName : requestStats.getInnerMapKeySet(List.of(indexName, shardName))) {
                        indicesAndTierStats.addTo(List.of(indexName, tierName), requestStats.getResponse(List.of(indexName, shardName, tierName)));
                    }
                }
            }
            return indicesAndTierStats;
        }
        throw new IllegalArgumentException("Level must be one of 'tier', 'shards', 'indices', 'shards,tier', 'indices,tier', or empty, but was " + String.join(",", levels));
    }

    // Return a CacheStatsResponse which is the sum of the response for all tiers in a shard
    private CacheStatsResponse sumRequestCacheShard(String indexName, String shardName) {
        CacheStatsResponse total = new CacheStatsResponse();
        for (String tierName : requestStats.getInnerMapKeySet(List.of(indexName, shardName))) {
            CacheStatsResponse response = requestStats.getResponse(List.of(indexName, shardName, tierName));
            total = total.add(response);
        }
        return total;
    }

    private void checkRequestStatsDimensionNames() {
        assert List.of(CacheService.INDICES_DIMENSION_NAME, CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_NAME).equals(requestStats.getDimensionNames())
            : "request stats has unexpected dimension names " + requestStats.getDimensionNames();
    }

    private XContentBuilder requestCacheToXContent(XContentBuilder builder, Params params, CacheService.AggregatedStats aggregatedStats, CacheService.AggregatedStats nodeLevelStats) throws IOException {
        builder.startObject("request_cache");
        totalRequestStats.toXContent(builder, params); // Always write node level stats

        String[] levels = getLevels(params);
        if (levels != null) {
            CacheService.AggregatedStats aggregated = aggregateRequestStatsByLevel(levels);
            if (levels.length == 1) {
                singleDimensionXContentHelper(builder, params, aggregated, levels[0]);
            }
            // Handle the case for more than one level separately, because the input in levels might be in any order
            else if (checkLevelsEquality(levels, Set.of("shards, tier"))) {
                twoDimensionXContentHelper(builder, params, aggregated, "shards", "tier");
            }
            else if (checkLevelsEquality(levels, Set.of("indices, tier"))) {
                twoDimensionXContentHelper(builder, params, aggregated, "indices", "tier");
            }
            else {
                throw new IllegalArgumentException("Level must be one of 'tier', 'shards', 'indices', 'shards,tier', or 'indices,tier' but was " + params.param("level"));
            }
        }
        builder.endObject();
        return builder;
    }

    private void singleDimensionXContentHelper(XContentBuilder builder, Params params,
                                                          CacheService.AggregatedStats response, String fieldName) throws IOException {
        builder.startObject(fieldName);
        for (String name : response.getInnerMapKeySet(List.of())) {
            builder.startObject(name);
            response.getResponse(List.of(name)).toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
    }

    private void twoDimensionXContentHelper(XContentBuilder builder, Params params,
                                            CacheService.AggregatedStats response,
                                            String outerFieldName, String innerFieldName) throws IOException {

        builder.startObject(outerFieldName);
        for (String outerName : response.getInnerMapKeySet(List.of())) {
            builder.startObject(outerName);
            for (String innerName : response.getInnerMapKeySet(List.of(outerName))) {
                builder.startObject(innerFieldName);
                response.getResponse(List.of(outerName, innerName)).toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
    }

    /**
     * Check if the value passed into Params for levels matches a given set of levels (in lowercase)
     */
    private boolean checkLevelsEquality(String[] paramsLevels, Set<String> levels) {
        Set<String> paramsLevelsSet = new HashSet<>();
        for (String paramsLevel : paramsLevels) {
            paramsLevelsSet.add(paramsLevel.toLowerCase());
        }
        return levels.equals(paramsLevelsSet);


    }
    private String[] getLevels(Params params) {
        String levels = params.param("level");
        if (levels == null) {
            return null;
        }
        return levels.split(",");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        requestStats.writeTo(out);
    }
}
