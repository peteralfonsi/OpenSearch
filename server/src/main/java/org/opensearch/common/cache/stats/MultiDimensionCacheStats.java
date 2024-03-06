/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * A CacheStats object supporting multiple different dimensions.
 * Also keeps track of a tier dimension, which is assumed to be the same for all values in the stats object.
 * The tier dimension value should not be passed into the CacheStats API functions for updating values.
 */
public class MultiDimensionCacheStats implements CacheStats {

    /**
     * For memory purposes, don't track stats for more than this many distinct combinations of dimension values.
     */
    public final static int DEFAULT_MAX_DIMENSION_VALUES = 20_000;

    // pkg-private for testing
    final List<String> dimensionNames;

    // The value of the tier dimension for entries in this Stats object. This is handled separately for efficiency,
    // as it always has the same value for every entry in the stats object.
    // Package-private for testing.
    final String tierDimensionValue;

    // A map from a set of cache stats dimensions -> stats for that combination of dimensions. Does not include the tier dimension in its
    // keys.
    //final ConcurrentMap<Key, CacheStatsResponse> map;
    final AggregatedStats stats;

    final int maxDimensionValues;
    CacheStatsResponse totalStats;

    public MultiDimensionCacheStats(List<String> dimensionNames, String tierDimensionValue, int maxDimensionValues) {
        this.dimensionNames = dimensionNames;
        //this.map = new ConcurrentHashMap<>();
        this.stats = new AggregatedStats(dimensionNames);
        this.totalStats = new CacheStatsResponse();
        this.tierDimensionValue = tierDimensionValue;
        this.maxDimensionValues = maxDimensionValues;
    }

    public MultiDimensionCacheStats(List<String> dimensionNames, String tierDimensionValue) {
        this(dimensionNames, tierDimensionValue, DEFAULT_MAX_DIMENSION_VALUES);
    }

    public MultiDimensionCacheStats(StreamInput in) throws IOException {
        this.dimensionNames = List.of(in.readStringArray());
        this.tierDimensionValue = in.readString();
        /*Map<Key, CacheStatsResponse> readMap = in.readMap(
            i -> new Key(Set.of(i.readArray(CacheStatsDimension::new, CacheStatsDimension[]::new))),
            CacheStatsResponse::new
        );*/
        //this.map = new ConcurrentHashMap<Key, CacheStatsResponse>(readMap);
        this.stats = new AggregatedStats(in);
        this.totalStats = new CacheStatsResponse(in);
        this.maxDimensionValues = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        out.writeString(tierDimensionValue);
        /*out.writeMap(
            map,
            (o, key) -> o.writeArray((o1, dim) -> ((CacheStatsDimension) dim).writeTo(o1), key.dimensions.toArray()),
            (o, response) -> response.writeTo(o)
        );*/
        stats.writeTo(out);
        totalStats.writeTo(out);
        out.writeVInt(maxDimensionValues);
    }

    @Override
    public CacheStatsResponse getTotalStats() {
        return totalStats;
    }

    /**
     * Get the stats response aggregated by dimensions. If there are no values for the specified dimensions,
     * returns an all-zero response.
     */
    @Override
    public CacheStatsResponse getStatsByDimensions(List<CacheStatsDimension> dimensions) {
        if (!checkDimensionNames(dimensions)) {
            throw new IllegalArgumentException("Can't get stats for unrecognized dimensions");
        }

        CacheStatsDimension tierDim = getTierDimension(dimensions);
        if (tierDim == null || tierDim.dimensionValue.equals(tierDimensionValue)) {
            // If there is no tier dimension, or if the tier dimension value matches the one for this stats object, return an aggregated
            // response over the non-tier dimensions
            List<CacheStatsDimension> modifiedDimensions = new ArrayList<>(dimensions);
            if (tierDim != null) {
                modifiedDimensions.remove(tierDim);
            }

            List<String> completeDimValuesList = getCompleteDimValuesList(modifiedDimensions);

            if (modifiedDimensions.size() == dimensionNames.size()) {
                //return map.getOrDefault(new Key(modifiedDimensions), new CacheStatsResponse());
                try {
                    return stats.getResponse(completeDimValuesList);
                } catch (AssertionError e) {
                    // Thrown if there is no matching path in the stats object
                    return new CacheStatsResponse();
                }
            }

            return stats.getSummedResponse(completeDimValuesList);

            // I don't think there's a more efficient way to get arbitrary combinations of dimensions than to just keep a map
            // and iterate through it, checking if keys match. We can't pre-aggregate because it would consume a lot of memory.
            /*CacheStatsResponse response = new CacheStatsResponse();
            for (Key key : map.keySet()) {
                if (key.dimensions.containsAll(modifiedDimensions)) {
                    response.add(map.get(key));
                }
            }
            return response;*/
        }
        // If the tier dimension doesn't match, return an all-zero response
        return new CacheStatsResponse();
    }

    private CacheStatsDimension getTierDimension(List<CacheStatsDimension> dimensions) {
        for (CacheStatsDimension dim : dimensions) {
            if (dim.dimensionName.equals(CacheStatsDimension.TIER_DIMENSION_NAME)) {
                return dim;
            }
        }
        return null;
    }

    // Return a list of dimension values in order, and if any dimensions are missing from the input list, add them to the output
    // with a null dimension value.
    private List<String> getCompleteDimValuesList(List<CacheStatsDimension> dimensions) {
        List<String> output = new ArrayList<>();
        List<CacheStatsDimension> input = new ArrayList<>(dimensions); // Modifiable copy
        for (String dimName : dimensionNames) {
            CacheStatsDimension foundDim = null;
            for (CacheStatsDimension inputDim : input) {
                if (inputDim.dimensionName.equals(dimName)) {
                    foundDim = inputDim;
                    output.add(inputDim.dimensionValue);
                }
            }
            if (foundDim == null) {
                output.add(null);
                input.remove(foundDim);
                // When the value is null, we will add up all values for this dimension
            }
        }
        return output;
    }

    private boolean checkDimensionNames(List<CacheStatsDimension> dimensions) {
        for (CacheStatsDimension dim : dimensions) {
            if (!(dimensionNames.contains(dim.dimensionName) || dim.dimensionName.equals(CacheStatsDimension.TIER_DIMENSION_NAME))) {
                // Reject dimension names that aren't in the list and aren't the tier dimension
                return false;
            }
        }
        return true;
    }

    @Override
    public long getTotalHits() {
        return totalStats.getHits();
    }

    @Override
    public long getTotalMisses() {
        return totalStats.getMisses();
    }

    @Override
    public long getTotalEvictions() {
        return totalStats.getEvictions();
    }

    @Override
    public long getTotalMemorySize() {
        return totalStats.getMemorySize();
    }

    @Override
    public long getTotalEntries() {
        return totalStats.getEntries();
    }

    @Override
    public long getHitsByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getHits();
    }

    @Override
    public long getMissesByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getMisses();
    }

    @Override
    public long getEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getEvictions();
    }

    @Override
    public long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getMemorySize();
    }

    @Override
    public long getEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getEntries();
    }

    @Override
    public void incrementHitsByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.hits.inc(amount), 1);
    }

    @Override
    public void incrementMissesByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.misses.inc(amount), 1);
    }

    @Override
    public void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.evictions.inc(amount), 1);
    }

    @Override
    public void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes) {
        internalIncrement(dimensions, (response, amount) -> response.memorySize.inc(amount), amountBytes);
    }

    @Override
    public void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.entries.inc(amount), 1);
    }

    @Override
    public void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.entries.inc(amount), -1);
    }

    @Override
    public void reset() {
        Tuple<List<List<String>>, List<CacheStatsResponse>> pairs = stats.getAllPairs();
        /*for (Key key : map.keySet()) {
            CacheStatsResponse response = map.get(key);
            response.memorySize.dec(response.getMemorySize());
            response.entries.dec(response.getEntries());
        }*/
        for (CacheStatsResponse response : pairs.v2()) {
            response.memorySize.dec(response.getMemorySize());
            response.entries.dec(response.getEntries());
        }
        totalStats.memorySize.dec(totalStats.getMemorySize());
        totalStats.entries.dec(totalStats.getEntries());
    }

    private CacheStatsResponse internalGetStats(List<CacheStatsDimension> dimensions) {
        assert dimensions.size() == dimensionNames.size();
        List<String> dimValues = new ArrayList<>();
        for (CacheStatsDimension dim : dimensions) {
            dimValues.add(dim.dimensionValue);
        }
        CacheStatsResponse response;
        try {
            response = stats.getResponse(dimValues);
        } catch (AssertionError e) {
            // Thrown if there is no such path; in this case we should add a new empty response to the stats
            if (stats.getSize() < maxDimensionValues) {
                response = new CacheStatsResponse();
                stats.put(dimValues, response);
            } else {
                throw new RuntimeException("Cannot add new combination of dimension values to stats object; reached maximum");
            }
        }
        return response;
        /*CacheStatsResponse response = map.get(new Key(dimensions));
        if (response == null) {
            if (map.size() < maxDimensionValues) {
                response = new CacheStatsResponse();
                map.put(new Key(dimensions), response);
            } else {
                throw new RuntimeException("Cannot add new combination of dimension values to stats object; reached maximum");
            }
        }
        return response;*/
    }

    private void internalIncrement(List<CacheStatsDimension> dimensions, BiConsumer<CacheStatsResponse, Long> incrementer, long amount) {
        CacheStatsResponse stats = internalGetStats(dimensions);
        incrementer.accept(stats, amount);
        incrementer.accept(totalStats, amount);
    }
}
