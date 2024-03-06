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
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * Holds potentially nested maps from the aggregated dimension values to the cache stats responses. The values are ordered by insertion order.
 * For example, if we aggregate by indices and tiers, the outer map's keys are index names, and the inner ones are tier names.
 * If dimensionNames is empty, it holds a single value representing a total stats object.
 */
public class AggregatedStats implements Writeable {
    // TODO: This class is separate from MultiDimensionCacheStats because it's also used in the future stats
    //  rework PR that exposes new APIs through the CacheService.
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
                previousValue.add(response);
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
                    previousValue.add(response);
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

    // Take in an ordered list of dimension values and return the resulting response.
    // If any of them is null, add up over all values for this dimension.
    public CacheStatsResponse getSummedResponse(List<String> dimensionValues) {
        assert dimensionValues.size() == dimensionNames.size() : "Must include values (or null value) for all dimensions you want to sum";
        CacheStatsResponse response = new CacheStatsResponse();
        getSummedResponseHelper(dimensionValues, new ArrayList<>(), outerMap, response);
        return response;
    }

    // Increment response by all the values which descend from the current root (specified by list).
    private void getSummedResponseHelper(List<String> dimensionValues, List<String> currentRootPath, LinkedHashMap<String, Object> currentMap, CacheStatsResponse response) {
        /*if (currentRootPath.size() == dimensionValues.size()) {
            // we're at a leaf node
            try {
                //response.add(getResponse(currentRootPath));
                response.add((CacheStatsResponse) currentMap.get(currentRootPath.get(currentRootPath.size()-1)));
            } catch (AssertionError ignored) {
                // getResponse throws an assertion error if this path does not exist; this can happen if we specify a
                // value for the last dimension but null values for dimensions before it. Do nothing in this case.
            }
            return;
        }*/
        int currentLevel = currentRootPath.size();
        String currentValue = dimensionValues.get(currentLevel);
        if (currentValue == null) {
            // Add to the response from all dimension values for this dimension
            List<String> keySet;
            try {
                keySet = getInnerMapKeySet(currentRootPath);
            } catch (AssertionError e) {
                // Thrown if there is no such path; return in this case
                return;
            }
            for (String nextValue : keySet) {
                List<String> newRootPath = new ArrayList<>(currentRootPath);
                newRootPath.add(nextValue);

                if (currentLevel == dimensionValues.size()-1) {
                    try {
                        CacheStatsResponse statsResponse = (CacheStatsResponse) currentMap.get(nextValue);
                        response.add(statsResponse);
                    } catch (AssertionError e) {
                        // getResponse throws an assertion error if this path does not exist; this can happen if we specify a
                        // value for the last dimension but null values for dimensions before it. Do nothing in this case.
                    }

                } else {
                    LinkedHashMap<String, Object> newMap = (LinkedHashMap<String, Object>) currentMap.get(nextValue);
                    getSummedResponseHelper(dimensionValues, newRootPath, newMap, response);
                }
            }
        } else {
            List<String> newRootPath = new ArrayList<>(currentRootPath);
            newRootPath.add(currentValue);
            if (currentLevel == dimensionValues.size() - 1) {
                try {
                    CacheStatsResponse statsResponse = (CacheStatsResponse) currentMap.get(currentValue);
                    response.add(statsResponse);
                } catch (AssertionError e) {
                    // getResponse throws an assertion error if this path does not exist; this can happen if we specify a
                    // value for the last dimension but null values for dimensions before it. Do nothing in this case.
                }
                return;
            } else {
                LinkedHashMap<String, Object> newMap = (LinkedHashMap<String, Object>) currentMap.get(currentValue);
                if (newMap != null) {
                    getSummedResponseHelper(dimensionValues, newRootPath, newMap, response);
                }
            }
        }
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
    Tuple<List<List<String>>, List<CacheStatsResponse>> getAllPairs() {
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
