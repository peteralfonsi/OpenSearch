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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A CacheStats object supporting aggregation over multiple different dimensions.
 * Stores a fixed snapshot of a cache's stats; does not allow changes.
 *
 * @opensearch.experimental
 */
public class MultiDimensionCacheStats implements CacheStats {
    // A snapshot of a StatsHolder containing stats maintained by the cache.
    // Pkg-private for testing.
    final Map<StatsHolder.Key, CounterSnapshot> snapshot;
    final List<String> dimensionNames;

    // The name of the cache type producing these stats. Returned in API response.
    final String storeName;
    public static String STORE_NAME_FIELD = "store_name";

    public static String CLASS_NAME = "multidimension";

    public MultiDimensionCacheStats(Map<StatsHolder.Key, CounterSnapshot> snapshot, List<String> dimensionNames, String storeName) {
        this.snapshot = snapshot;
        this.dimensionNames = dimensionNames;
        this.storeName = storeName;
    }

    /**
     * Should not be used with StreamOutputs produced using writeToWithClassName.
     */
    public MultiDimensionCacheStats(StreamInput in) throws IOException {
        this.dimensionNames = List.of(in.readStringArray());
        this.snapshot = in.readMap(
            i -> new StatsHolder.Key(List.of(i.readArray(CacheStatsDimension::new, CacheStatsDimension[]::new))),
            CounterSnapshot::new
        );
        this.storeName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        out.writeMap(
            snapshot,
            (o, key) -> o.writeArray((o1, dim) -> ((CacheStatsDimension) dim).writeTo(o1), key.dimensions.toArray()),
            (o, snapshot) -> snapshot.writeTo(o)
        );
        out.writeString(storeName);
    }

    @Override
    public CounterSnapshot getTotalStats() {
        CacheStatsCounter counter = new CacheStatsCounter();
        // To avoid making many Snapshot objects for the incremental sums, add to a mutable CacheStatsCounter and finally convert to
        // Snapshot
        for (CounterSnapshot snapshotValue : snapshot.values()) {
            counter.add(snapshotValue);
        }
        return counter.snapshot();
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

    static class DimensionNode {
        private final String dimensionValue;
        // Storing dimensionValue is useful for producing XContent
        final TreeMap<String, DimensionNode> children; // Map from dimensionValue to the DimensionNode for that dimension value
        private CounterSnapshot snapshot;

        DimensionNode(String dimensionValue) {
            this.dimensionValue = dimensionValue;
            this.children = new TreeMap<>();
            this.snapshot = null;
            // Only leaf nodes have non-null snapshots. Might make it be sum-of-children in future.
        }

        /**
         * Increments the snapshot in this node.
         */
        void addSnapshot(CounterSnapshot newSnapshot) {
            if (snapshot == null) {
                snapshot = newSnapshot;
            } else {
                snapshot = CounterSnapshot.addSnapshots(snapshot, newSnapshot);
            }
        }

        /**
         * Returns the node found by following these dimension values down from the current node.
         * If such a node does not exist, creates it.
         */
        DimensionNode getNode(List<String> dimensionValues) {
            DimensionNode current = this;
            for (String dimensionValue : dimensionValues) {
                current.children.putIfAbsent(dimensionValue, new DimensionNode(dimensionValue));
                current = current.children.get(dimensionValue);
            }
            return current;
        }

        CounterSnapshot getSnapshot() {
            return snapshot;
        }
    }

    /**
     * Returns a tree containing the stats aggregated by the levels passed in. The root node is a dummy node,
     * whose name and value are null.
     */
    DimensionNode aggregateByLevels(List<String> levels) {
        int[] levelPositions = getLevelsInSortedOrder(levels); // Check validity of levels and get their indices in dimensionNames

        DimensionNode root = new DimensionNode(null);
        for (Map.Entry<StatsHolder.Key, CounterSnapshot> entry : snapshot.entrySet()) {
            List<String> levelValues = new ArrayList<>(); // This key's relevant dimension values, which match the levels
            List<CacheStatsDimension> keyDimensions = entry.getKey().dimensions;
            for (int levelPosition : levelPositions) {
                levelValues.add(keyDimensions.get(levelPosition).dimensionValue);
            }
            DimensionNode leafNode = root.getNode(levelValues);
            leafNode.addSnapshot(entry.getValue());
        }
        return root;
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

        int[] positions = getLevelsInSortedOrder(levels); // Checks whether levels are valid; throws IllegalArgumentException if not
        toXContentForLevels(builder, params, levels);
        // Also add the store name for the cache that produced the stats
        builder.field(STORE_NAME_FIELD, storeName);
        return builder;
    }

    XContentBuilder toXContentForLevels(XContentBuilder builder, Params params, List<String> levels) throws IOException {
        DimensionNode aggregated = aggregateByLevels(levels);
        // Depth -1 corresponds to the dummy root node, which has no dimension value and only has children
        toXContentForLevelsHelper(-1, aggregated, levels, builder, params);
        return builder;

    }

    private void toXContentForLevelsHelper(int depth, DimensionNode current, List<String> levels, XContentBuilder builder, Params params)
        throws IOException {
        if (depth >= 0) {
            builder.startObject(current.dimensionValue);
        }

        if (depth == levels.size() - 1) {
            // This is a leaf node
            current.getSnapshot().toXContent(builder, params);
        } else {
            builder.startObject(levels.get(depth + 1));
            for (DimensionNode nextNode : current.children.values()) {
                toXContentForLevelsHelper(depth + 1, nextNode, levels, builder, params);
            }
            builder.endObject();
        }

        if (depth >= 0) {
            builder.endObject();
        }
    }

    private List<String> getLevels(Params params) {
        String levels = params.param("level");
        if (levels == null) {
            return null;
        }
        return List.of(levels.split(","));
    }

    private int[] getLevelsInSortedOrder(List<String> levels) {
        // Levels must all be present in dimensionNames and also be in matching order, or they are invalid
        // Return an array of each level's position within the list dimensionNames
        if (levels.isEmpty()) {
            throw new IllegalArgumentException("Levels cannot have size 0");
        }
        int[] result = new int[levels.size()];
        for (int i = 0; i < levels.size(); i++) {
            String level = levels.get(i);
            int levelIndex = dimensionNames.indexOf(level);
            if (levelIndex != -1) {
                result[i] = levelIndex;
            } else {
                throw new IllegalArgumentException("Unrecognized level: " + level);
            }
            if (i > 0 && result[i] < result[i - 1]) {
                // If the levels passed in are out of order, they are invalid
                throw new IllegalArgumentException("Invalid ordering for levels: " + levels);
            }
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
