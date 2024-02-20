/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NodeCacheStats implements ToXContentFragment {

    private final CacheService cacheService;

    NodeCacheStats(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // switch case for which cache to get stats for?

        return null;
    }

    private XContentBuilder requestCacheToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("request_cache");
        CacheService.AggregatedStats nodeLevelResponse = cacheService.getRequestCacheStats(new ArrayList<>());
        //nodeLevelResponse.get(0).response.toXContent(builder, params);

        List<CacheStatsDimension> aggregationDimensions = new ArrayList<>();
        String[] levels = getLevels(params);
        if (levels != null) {
            for (String level : levels) {
                aggregationDimensions.add(new CacheStatsDimension(level, ""));
                // Checks on this value are done within CacheService itself
            }
            //List<CacheService.ResponseAndDimensions> responseAndDimensions = cacheService.getRequestCacheStats(aggregationDimensions);
            if (checkLevelsEquality(levels, Set.of("tier"))) {
            } else if (checkLevelsEquality(levels, Set.of("shards"))) {
            } else if (checkLevelsEquality(levels, Set.of("indices"))) {
            } else if (checkLevelsEquality(levels, Set.of("shards, tier"))) {
            } else if (checkLevelsEquality(levels, Set.of("indices, tier"))) {
            }
        }


        builder.endObject();
        return null; //TODO
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
}
