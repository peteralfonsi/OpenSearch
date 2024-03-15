/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A CacheStats object supporting aggregation over multiple different dimensions.
 * Stores a fixed snapshot of a cache's stats; does not allow changes.
 */
public class MultiDimensionCacheStats implements CacheStats {
    // A snapshot of a StatsHolder containing stats maintained by the cache.
    // Pkg-private for testing.
    final Map<StatsHolder.Key, CacheStatsResponse.Snapshot> snapshot;
    final List<String> dimensionNames;

    public static String CLASS_NAME = "multidimension";

    public MultiDimensionCacheStats(Map<StatsHolder.Key, CacheStatsResponse.Snapshot> snapshot, List<String> dimensionNames) {
        this.snapshot = snapshot;
        this.dimensionNames = dimensionNames;
    }

    /**
     * Should not be used with StreamOutputs produced using writeToWithClassName.
     */
    public MultiDimensionCacheStats(StreamInput in) throws IOException {
        this.dimensionNames = List.of(in.readStringArray());
        Map<StatsHolder.Key, CacheStatsResponse.Snapshot> readMap = in.readMap(
            i -> new StatsHolder.Key(List.of(i.readArray(StreamInput::readString, String[]::new))),
            CacheStatsResponse.Snapshot::new
        );
        this.snapshot = new ConcurrentHashMap<StatsHolder.Key, CacheStatsResponse.Snapshot>(readMap);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        out.writeMap(
            snapshot,
            (o, key) -> o.writeArray((o1, dimValue) -> o1.writeString((String) dimValue), key.dimensionValues.toArray()),
            (o, snapshot) -> snapshot.writeTo(o)
        );
    }

    @Override
    public CacheStatsResponse.Snapshot getTotalStats() {
        CacheStatsResponse response = new CacheStatsResponse();
        // To avoid making many Snapshot objects for the incremental sums, add to a mutable CacheStatsResponse and finally convert to
        // Snapshot
        for (Map.Entry<StatsHolder.Key, CacheStatsResponse.Snapshot> entry : snapshot.entrySet()) {
            response.add(entry.getValue());
        }
        return response.snapshot();
    }

    @Override
    public long getTotalHits() {
        return getTotalStats().getHits();
    }

    @Override
    public long getTotalMisses() {
        return getTotalStats().getMisses();
    }

    @Override
    public long getTotalEvictions() {
        return getTotalStats().getEvictions();
    }

    @Override
    public long getTotalSizeInBytes() {
        return getTotalStats().getSizeInBytes();
    }

    @Override
    public long getTotalEntries() {
        return getTotalStats().getEntries();
    }

    @Override
    public String getClassName() {
        return CLASS_NAME;
    }

    @Override
    public void writeToWithClassName(StreamOutput out) throws IOException {
        out.writeString(getClassName());
        writeTo(out);
    }

    /**
     * Return a TreeMap containing stats values aggregated by the levels passed in. Results are ordered so that
     * values are grouped by their dimension values.
     * @param levels The levels to aggregate by
     * @return The resulting stats
     */
    public TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> aggregateByLevels(List<String> levels) {
        if (levels.size() == 0) {
            throw new IllegalArgumentException("Levels cannot have size 0");
        }
        int[] levelIndices = getLevelIndices(levels);
        TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> result = new TreeMap<>(new KeyComparator());

        for (Map.Entry<StatsHolder.Key, CacheStatsResponse.Snapshot> entry : snapshot.entrySet()) {
            List<String> levelValues = new ArrayList<>(); // The values for the dimensions we're aggregating over for this key
            for (int levelIndex : levelIndices) {
                levelValues.add(entry.getKey().dimensionValues.get(levelIndex));
            }
            // The new key for the aggregated stats contains only the dimensions specified in levels
            StatsHolder.Key levelsKey = new StatsHolder.Key(levelValues);
            CacheStatsResponse.Snapshot originalResponse = entry.getValue();
            if (result.containsKey(levelsKey)) {
                result.put(levelsKey, result.get(levelsKey).add(originalResponse));
            } else {
                result.put(levelsKey, originalResponse);
            }
        }
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Always show total stats, regardless of levels
        getTotalStats().toXContent(builder, params);

        List<String> levels = getLevels(params);
        if (levels == null) {
            // display total stats only
            return builder;
        }

        int[] indices = getLevelIndices(levels); // Checks whether levels are valid; throws IllegalArgumentException if not
        toXContentForLevels(builder, params, levels);
        return builder;
    }

    XContentBuilder toXContentForLevels(XContentBuilder builder, Params params, List<String> levels) throws IOException {
        TreeMap<StatsHolder.Key, CacheStatsResponse.Snapshot> agg = aggregateByLevels(levels);
        StatsHolder.Key lastKey = null;

        int depth = 0;

        for (Map.Entry<StatsHolder.Key, CacheStatsResponse.Snapshot> entry : agg.entrySet()) {
            StatsHolder.Key key = entry.getKey();
            if (lastKey == null) {
                // Open an object for all relevant dimension values
                for (int i = 0; i < key.dimensionValues.size(); i++) {
                    builder.startObject(levels.get(i));
                    System.out.println("New object = " + levels.get(i));
                    builder.startObject(key.dimensionValues.get(i));
                    System.out.println("New object = " + key.dimensionValues.get(i));
                    depth += 2;
                }
            }

            else {
                int innermostCommonDimIndex = getInnermostCommonDimensionIndex(key, lastKey);
                // End objects for all dimension values this key doesn't have in common with the last key
                int numObjectsToClose = (key.dimensionValues.size() - innermostCommonDimIndex - 2) * 2;
                for (int i = 0; i < numObjectsToClose; i++) {
                    builder.endObject();
                    System.out.println("Ended object");
                    depth--;
                }
                // Start new nested objects for all dimension values that this key doesn't have in common with the last key
                for (int i = innermostCommonDimIndex + 1; i < key.dimensionValues.size(); i++) {
                    if (i >= innermostCommonDimIndex + 2) {
                        // If we're opening an object for a dimension value that's different in the new key,
                        // also open a new object with the name of that dimension
                        builder.startObject(levels.get(i));
                        System.out.println("New object = " + levels.get(i));
                        depth++;
                    }
                    builder.startObject(key.dimensionValues.get(i));
                    System.out.println("New object = " + key.dimensionValues.get(i));
                    depth++;
                }
            }

            // Finally, write the value
            entry.getValue().toXContent(builder, params);
            builder.endObject(); // End the object that contains only the snapshot for this key
            System.out.println("Ended object");
            depth--;
            System.out.println("depth = " + depth);
            lastKey = key;
        }
        // There will always be 2 * levels.size() - 1 objects to close at the end
        for (int i = 0; i < levels.size() * 2 - 1; i++) {
            builder.endObject();
            System.out.println("Ended object");
            depth--;
        }
        System.out.println("Final depth = " + depth);

        return builder;

    }

    private int getInnermostCommonDimensionIndex(StatsHolder.Key key, StatsHolder.Key lastKey) {
        assert key.dimensionValues.size() == lastKey.dimensionValues.size();
        for (int i = key.dimensionValues.size() - 1; i >= 0; i--){
            if (key.dimensionValues.get(i).equals(lastKey.dimensionValues.get(i))) {
                return i;
            }
        }
        return -1; // No common dimension values
    }

    private List<String> getLevels(Params params) {
        String levels = params.param("level");
        if (levels == null) {
            return null;
        }
        return List.of(levels.split(","));
    }


    // First compare outermost dimension, then second outermost, etc.
    // Pkg-private for testing
    static class KeyComparator implements Comparator<StatsHolder.Key> {
        @Override
        public int compare(StatsHolder.Key k1, StatsHolder.Key k2) {
            assert k1.dimensionValues.size() == k2.dimensionValues.size();
            for (int i = 0; i < k1.dimensionValues.size(); i++) {
                int compareValue = k1.dimensionValues.get(i).compareTo(k2.dimensionValues.get(i));
                if (compareValue != 0) {
                    return compareValue;
                }
            }
            return 0;
        }
    }

    private int[] getLevelIndices(List<String> levels) {
        // Levels must all be present in dimensionNames and also be in matching order
        // Return a list of indices in dimensionNames corresponding to each level
        int[] result = new int[levels.size()];
        int levelsIndex = 0;

        for (int namesIndex = 0; namesIndex < dimensionNames.size(); namesIndex++) {
            if (dimensionNames.get(namesIndex).equals(levels.get(levelsIndex))) {
                result[levelsIndex] = namesIndex;
                levelsIndex++;
            }
            if (levelsIndex >= levels.size()) {
                break;
            }
        }
        if (levelsIndex != levels.size()) {
            throw new IllegalArgumentException("Invalid levels: " + levels);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || o.getClass() != MultiDimensionCacheStats.class) {
            return false;
        }
        MultiDimensionCacheStats other = (MultiDimensionCacheStats) o;
        return this.snapshot.equals(other.snapshot) && this.dimensionNames.equals(other.dimensionNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.snapshot, this.dimensionNames);
    }

}
