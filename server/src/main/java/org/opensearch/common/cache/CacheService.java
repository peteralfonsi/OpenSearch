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
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

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

    // TODO: CacheService is now meant to also instantiate the caches

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

    AggregatedStats getRequestCacheStats() {
        // Return an AggregatedStats object which is split out by all three level options.
        // Then the NodeCacheStats object can sum later according to whatever level it receives.
        // (We don't know which levels at NodeCacheStats creation time)
        ICache cache = getCache(CacheType.INDICES_REQUEST_CACHE);
        AggregatedStats stats = new AggregatedStats(List.of(CacheService.INDICES_DIMENSION_NAME, CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_NAME));

        for (final IndexService indexService : indicesService) {
            String indexName = getIndexName(indexService);
            for (final IndexShard indexShard : indexService) {
                String indexShardName = getShardName(indexShard);
                CacheStatsDimension shardDimension = new CacheStatsDimension(SHARDS_DIMENSION_NAME, indexShardName);
                for (String tier : API_SUPPORTED_TIERS) {
                    CacheStatsDimension tierDimension = new CacheStatsDimension(TIER_DIMENSION_NAME, tier);
                    stats.put(List.of(indexName, indexShardName, tier), cache.stats().getStatsByDimensions(List.of(shardDimension, tierDimension)));
                }
            }
        }
        return stats;
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

    CacheStatsResponse getTotalRequestStats() {
        ICache cache = getCache(CacheType.INDICES_REQUEST_CACHE);
        return cache.stats().getTotalStats();
    }

    public NodeCacheStats stats() {
        AggregatedStats requestStats = getRequestCacheStats();
        CacheStatsResponse totalRequestStats = getTotalRequestStats(); // We always return the total stats as well, to spare the NodeCacheStats from having to sum it up
        return new NodeCacheStats(requestStats, totalRequestStats);
    }

    /**
     * Holds potentially nested maps from the aggregated dimension values to the cache stats responses. The values are ordered by insertion order.
     * For example, if we aggregate by indices and tiers, the outer map's keys are index names, and the inner ones are tier names.
     * If dimensionNames is empty, it holds a single value representing a total stats object.
     */
    public static class AggregatedStats implements Writeable {
        private final LinkedHashMap<String, Object> outerMap; // Object will be either another map or a CacheStatsResponse object for the innermost map

        // Ordered list of dimension names, for example "tier", "indices", "shards"
        // Not directly used to access values, but useful for consumers to know what dimension the values are associated with
        private final List<String> dimensionNames;

        private int size; // Total number of entries

        private static final String SERIALIZED_KEY_DELIMITER = ",";

        public AggregatedStats(List<String> dimensionNames) {
            this.dimensionNames = dimensionNames;
            this.outerMap = new LinkedHashMap<>();
            this.size = 0;
        }

        public AggregatedStats(StreamInput in) throws IOException {
            this.dimensionNames = Arrays.asList(in.readStringArray());
            this.outerMap = new LinkedHashMap<>();
            if (dimensionNames.isEmpty()) {
                // Read only the single CacheStatsResponse
                boolean valuePresent = in.readBoolean();
                if (valuePresent) {
                    put(List.of(), new CacheStatsResponse(in));
                }
                return;
            }
            String[] flattenedKeys = in.readStringArray();
            CacheStatsResponse[] responses = in.readArray(CacheStatsResponse::new, CacheStatsResponse[]::new);

            assert flattenedKeys.length == responses.length;
            for (int i = 0; i < flattenedKeys.length; i++) {
                List<String> dimensionValues = Arrays.asList(flattenedKeys[i].split(SERIALIZED_KEY_DELIMITER));
                put(dimensionValues, responses[i]);
            }
        }

        public List<String> getDimensionNames() {
            return dimensionNames;
        }

        // Put a new response in the maps.
        public void put(List<String> dimensionValues, CacheStatsResponse response) {
            putOrAdd(dimensionValues, response, false);
        }

        // Add a new response to the existing response in the maps.
        public void addTo(List<String> dimensionValues, CacheStatsResponse response) {
            putOrAdd(dimensionValues, response, true);
        }

        public void putOrAdd(List<String> dimensionValues, CacheStatsResponse response, boolean doAdd) {
            assert dimensionValues.size() == dimensionNames.size();
            LinkedHashMap<String, Object> currentMap = outerMap;
            if (dimensionNames.isEmpty()) {
                if (doAdd) {
                    CacheStatsResponse previousValue = (CacheStatsResponse) currentMap.get("");
                    assert previousValue != null : "Cannot add to null value for " + dimensionValues;
                    currentMap.put("", previousValue.add(response));
                } else {
                    CacheStatsResponse previousValue = (CacheStatsResponse) currentMap.put("", response);
                    assert previousValue == null : "Cannot overwrite existing value for " + dimensionValues;
                    size++;
                }
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
                    if (doAdd) {
                        // Add this value to the existing value, ensuring an existing value is present
                        CacheStatsResponse previousValue = (CacheStatsResponse) currentMap.get(dimensionValue);
                        assert previousValue != null : "Cannot add to null value for " + dimensionValues;
                        currentMap.put(dimensionValue, previousValue.add(response));
                    } else {
                        // Put the value in the innermost map, ensuring we don't overwrite an existing value
                        CacheStatsResponse previousValue = (CacheStatsResponse) currentMap.put(dimensionValue, response);
                        assert previousValue == null : "Cannot overwrite existing value for " + dimensionValues;
                        size++;
                    }
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(dimensionNames.toArray(new String[0]));
            // Write a flattened map, from a single concatenated key to a CacheStatsResponse object
            // Insertion order must be preserved, so instead of using StreamOutput.writeMap, we write an array of key Strings and an array of CacheStatsResponse objects.
            // Do all values associated with the first inserted key in the outermost map, in the order of the keys in the second-outermost map,
            // then all values for the second key in the outermost map, ...

            if (dimensionNames.isEmpty()) {
                // Only write the single CacheStatsResponse if it's present, don't write any keys
                CacheStatsResponse value = getResponse(List.of());
                boolean valuePresent = value != null;
                out.writeBoolean(valuePresent);
                if (valuePresent) {
                    value.writeTo(out);
                }
                return;
            }
            // Otherwise, write the keys in the maps along with the values
            Tuple<List<List<String>>, List<CacheStatsResponse>> keyValuePairs = getAllPairs();
            String[] flattenedKeys = getFlattenedKeys(keyValuePairs.v1());

            out.writeStringArray(flattenedKeys);
            out.writeArray((o, v) -> v.writeTo(o), keyValuePairs.v2().toArray(new CacheStatsResponse[0]));
        }

        private String[] getFlattenedKeys(List<List<String>> keyPathList) {
            String[] flattenedKeys = new String[size];
            for (int i = 0; i < size; i++) {
                flattenedKeys[i] = String.join(SERIALIZED_KEY_DELIMITER, keyPathList.get(i));
            }
            return flattenedKeys;
        }


        // Return a list of all key lists and values in order, such that if we re-added them to a new object in this order, we would have an equal AggregatedStats object.
        private Tuple<List<List<String>>, List<CacheStatsResponse>> getAllPairs() {
            if (dimensionNames.isEmpty()) {
                if (size == 1) {
                    return new Tuple<>(List.of(List.of("")), List.of(getResponse(List.of())));
                } else {
                    return new Tuple<>(List.of(List.of()), List.of());
                }
            }
            List<List<String>> keyPathList = new ArrayList<>();
            List<CacheStatsResponse> valueList = new ArrayList<>();

            getAllPairsHelper(outerMap, 0, List.of(), keyPathList, valueList);
            assert keyPathList.size() == size;
            assert valueList.size() == size;
            return new Tuple<>(keyPathList, valueList);
        }

        // Add the keys and leaf nodes which descend from currentMap to flattenedKeys and responses, preserving insertion order
        private void getAllPairsHelper(LinkedHashMap<String, Object> currentMap, int currentDepth, List<String> pathToCurrentMap, List<List<String>> keyPathList, List<CacheStatsResponse> responses) {
            if (currentDepth == dimensionNames.size() - 1) {
                // This map contains leaf nodes; add them to the list
                for (String key : getInnerMapKeySet(pathToCurrentMap)) {
                    List<String> valuePath = new ArrayList<>(pathToCurrentMap);
                    valuePath.add(key);
                    CacheStatsResponse value = getResponse(valuePath);

                    keyPathList.add(valuePath);
                    responses.add(value);
                }
                return;
            }
            // This map doesn't contain leaf nodes; recursively call this on the maps it contains
            for (String key : getInnerMapKeySet(pathToCurrentMap)) {
                List<String> nextMapPath = new ArrayList<>(pathToCurrentMap);
                nextMapPath.add(key);
                LinkedHashMap<String, Object> nextMap = (LinkedHashMap<String, Object>) currentMap.get(key);
                getAllPairsHelper(nextMap, currentDepth+1, nextMapPath, keyPathList, responses);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } if (o.getClass() != AggregatedStats.class) {
                return false;
            }
            AggregatedStats other = (AggregatedStats) o;
            return dimensionNames.equals(other.dimensionNames) && getAllPairs().equals(other.getAllPairs());
        }

        @Override
        public int hashCode() {
            Tuple<List<List<String>>, List<CacheStatsResponse>> keyValuePairs = getAllPairs();
            String[] flattenedKeys = getFlattenedKeys(keyValuePairs.v1());
            return Objects.hash(Arrays.hashCode(flattenedKeys), Arrays.hashCode(keyValuePairs.v2().toArray(new CacheStatsResponse[0])));
        }
    }
}
