/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.metrics.CounterMetric;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static org.opensearch.common.cache.stats.MultiDimensionCacheStats.MDCSDimensionNode;

/**
 * A class caches use to internally keep track of their stats across multiple dimensions.
 * Not intended to be exposed outside the cache; for this, use statsHolder.getCacheStats() to create an immutable
 * copy of the current state of the stats.
 *
 * @opensearch.experimental
 */
public class StatsHolder {

    // The list of permitted dimensions. Should be ordered from "outermost" to "innermost", as you would like to
    // aggregate them in an API response.
    private final List<String> dimensionNames;

    // A tree structure based on dimension values, which stores stats values in its leaf nodes.
    private final StatsHolderDimensionNode statsRoot;

    public StatsHolder(List<String> dimensionNames) {
        this.dimensionNames = dimensionNames;
        this.statsRoot = new StatsHolderDimensionNode(null); // The root node has no dimension value associated with it, only children
        statsRoot.createChildrenMap();
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    // For all these increment functions, the dimensions list comes from the key, and contains all dimensions present in dimensionNames.
    // The order has to match the order given in dimensionNames.
    public void incrementHits(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.hits.inc(amount), 1, true);
    }

    public void incrementMisses(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.misses.inc(amount), 1, true);
    }

    public void incrementEvictions(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.evictions.inc(amount), 1, true);
    }

    public void incrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.sizeInBytes.inc(amount), amountBytes, true);
    }

    // For decrements, we should not create nodes if they are absent. This protects us from erroneously decrementing values for keys
    // which have been entirely deleted, for example in an async removal listener.
    public void decrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.sizeInBytes.dec(amount), amountBytes, false);
    }

    public void incrementEntries(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.entries.inc(amount), 1, true);
    }

    public void decrementEntries(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.entries.dec(amount), 1, false);
    }

    // A helper function which traverses the whole stats tree and runs some function taking in the node and path at each node.
    static void traverseStatsTreeHelper(
        StatsHolderDimensionNode currentNode,
        List<String> pathToCurrentNode,
        BiConsumer<StatsHolderDimensionNode, List<String>> function
    ) {
        function.accept(currentNode, pathToCurrentNode);
        if (currentNode.hasChildren()) {
            // not a leaf node
            for (StatsHolderDimensionNode child : currentNode.children.values()) {
                List<String> pathToChild = new ArrayList<>(pathToCurrentNode);
                pathToChild.add(child.getDimensionValue());
                traverseStatsTreeHelper(child, pathToChild, function);
            }
        }
    }

    /**
     * Reset number of entries and memory size when all keys leave the cache, but don't reset hit/miss/eviction numbers.
     * This is in line with the behavior of the existing API when caches are cleared.
     */
    public void reset() {
        traverseStatsTreeHelper(statsRoot, new ArrayList<>(), (node, path) -> {
            CacheStatsCounter counter = node.getStats();
            if (counter != null) {
                counter.sizeInBytes.dec(counter.getSizeInBytes());
                counter.entries.dec(counter.getEntries());
            }
        });
    }

    public long count() {
        // Include this here so caches don't have to create an entire CacheStats object to run count().
        final CounterMetric count = new CounterMetric();
        traverseStatsTreeHelper(statsRoot, new ArrayList<>(), (node, path) -> {
            if (node.children.isEmpty()) {
                // Only increment on leaf nodes to avoid double-counting, as non-leaf nodes contain stats too
                count.inc(node.getStats().getEntries());
            }
        });
        return count.count();
    }

    /**
     * Use the incrementer function to increment/decrement a value in the stats for a set of dimensions.
     * If createNewNodesIfAbsent is true, and there is no stats for this set of dimensions, create one.
     */
    private void internalIncrement(
        List<String> dimensionValues,
        BiConsumer<CacheStatsCounter, Long> incrementer,
        long amount,
        boolean createNewNodesIfAbsent
    ) {
        assert dimensionValues.size() == dimensionNames.size();
        List<StatsHolderDimensionNode> ancestors = getNodeAndAncestors(dimensionValues, createNewNodesIfAbsent);
        for (StatsHolderDimensionNode ancestorNode : ancestors) {
            incrementer.accept(ancestorNode.getStats(), amount);
        }
    }

    /**
     * Produce an immutable CacheStats representation of these stats.
     */
    public CacheStats getCacheStats() {
        MDCSDimensionNode snapshot = new MDCSDimensionNode(null);
        snapshot.createChildrenMap();
        traverseStatsTreeHelper(statsRoot, new ArrayList<>(), (node, path) -> {
            if (path.size() > 0) {
                CacheStatsCounterSnapshot nodeSnapshot = node.getStats().snapshot();
                String dimensionValue = path.get(path.size() - 1);
                MDCSDimensionNode newNode = new MDCSDimensionNode(dimensionValue);
                if (node.getChildren() != null) {
                    newNode.createChildrenMap();
                }
                newNode.setStats(nodeSnapshot);
                // Get the parent of this node in the new tree
                DimensionNode parentNode = getNode(path.subList(0, path.size() - 1), snapshot);
                ((Map<String, MDCSDimensionNode>) parentNode.getChildren()).put(dimensionValue, newNode);
            }
        });
        snapshot.setStats(statsRoot.getStats().snapshot());
        return new MultiDimensionCacheStats(snapshot, dimensionNames);
    }

    /**
     * Remove the stats for the nodes containing these dimension values in their path.
     * The list of dimension values must have a value for every dimension in the stats holder.
     */
    public void removeDimensions(List<String> dimensionValues) {
        assert dimensionValues.size() == dimensionNames.size();
        List<StatsHolderDimensionNode> ancestors = getNodeAndAncestors(dimensionValues, false);
        // Get the parent of the leaf node to remove
        StatsHolderDimensionNode parentNode = ancestors.get(ancestors.size() - 2);
        StatsHolderDimensionNode removedNode = ancestors.get(ancestors.size() - 1);
        CacheStatsCounter statsToDecrement = removedNode.getStats();
        if (parentNode != null) {
            parentNode.children.remove(removedNode.getDimensionValue());
        }

        // Now for all nodes that were ancestors of the removed node, decrement their stats, and check if they now have no children. If so,
        // remove them.
        for (int i = dimensionValues.size() - 1; i >= 1; i--) {
            StatsHolderDimensionNode currentNode = ancestors.get(i);
            parentNode = ancestors.get(i - 1);
            currentNode.getStats().subtract(statsToDecrement);
            if (currentNode.children.isEmpty()) {
                parentNode.children.remove(currentNode.getDimensionValue());
            }
        }
        // Finally, decrement stats for the root node.
        statsRoot.getStats().subtract(statsToDecrement);
    }

    static class StatsHolderDimensionNode extends DimensionNode {
        ConcurrentHashMap<String, StatsHolderDimensionNode> children; // Map from dimensionValue to the DimensionNode for that dimension
                                                                      // value
        private CacheStatsCounter stats; // The stats for this node. If a leaf node, corresponds to the stats for this combination of
                                         // dimensions; if not,
        // contains the sum of its children's stats.

        StatsHolderDimensionNode(String dimensionValue) {
            super(dimensionValue);
            this.children = null; // Lazy load this as needed
            this.stats = new CacheStatsCounter();
        }

        @Override
        protected void createChildrenMap() {
            children = new ConcurrentHashMap<>();
        }

        @Override
        protected Map<String, StatsHolderDimensionNode> getChildren() {
            return children;
        }

        public CacheStatsCounter getStats() {
            return stats;
        }

        public void setStats(CacheStatsCounter stats) {
            this.stats = stats;
        }
    }

    List<StatsHolderDimensionNode> getNodeAndAncestors(List<String> dimensionValues, boolean createNodesIfAbsent) {
        List<StatsHolderDimensionNode> result = new ArrayList<>();
        result.add(statsRoot);
        StatsHolderDimensionNode current = statsRoot;
        for (String dimensionValue : dimensionValues) {
            if (current.children == null) {
                current.createChildrenMap();
            }
            // If we are creating new nodes, put one in the map. Otherwise, the mapping function returns null to leave the map unchanged
            current = current.children.computeIfAbsent(
                dimensionValue,
                (key) -> createNodesIfAbsent ? new StatsHolderDimensionNode(dimensionValue) : null
            );
            if (current == null) {
                return new ArrayList<>(); // Return an empty list if the complete path doesn't exist
            }
            result.add(current);
        }
        return result;
    }

    /**
     * Returns the node found by following these dimension values down from the root node.
     * Returns null if no such node exists.
     */
    static DimensionNode getNode(List<String> dimensionValues, DimensionNode root) {
        DimensionNode current = root;
        for (String dimensionValue : dimensionValues) {
            current = current.getChildren().get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    // pkg-private for testing
    StatsHolderDimensionNode getStatsRoot() {
        return statsRoot;
    }
}
