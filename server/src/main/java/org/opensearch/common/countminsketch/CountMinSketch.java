/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.countminsketch;

public class CountMinSketch {
    private final int depth;
    private final int width;
    private final int[][] table;
    private final int decayPeriod;
    private int incrementCounter; // Set to -1 for it to never decay

    int decayCount;

    public CountMinSketch(int depth, int width, int decayPeriod) {
        this.depth = depth;
        this.width = width;
        this.table = new int[depth][width];
        this.decayPeriod = decayPeriod;
        this.incrementCounter = 0;
        this.decayCount = 0;
    }

    private int caffeineHash(int x) {
        // From spread() in https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/FrequencySketch.java
        x ^= x >>> 17;
        x *= 0xed5ad4bb;
        x ^= x >>> 11;
        x *= 0xac4c1b51;
        x ^= x >>> 15;
        return x;
    }

    private int getColumn(int row, int x) {
        /*
        If we have one well-behaved hash function h(x), it should be reasonable to produce a
        hash function h_r(x) for each row r by just taking the default hash function and adding r as an offset:
        h_r(x) = h(x + r)
        */
        // TODO: Empirically x * row seems to perform MUCH better than x + row
        return Math.abs(caffeineHash(x * (row + 1))) % width;
    }

    public void increment(int x) {
        for (int row = 0; row < depth; row++) {
            // TODO: Check overflow
            table[row][getColumn(row, x)] += 1;
        }
        incrementCounter++;
        if (incrementCounter >= decayPeriod && decayPeriod >= 0) {
            decay();
        }
    }

    public int estimate(int x) {
        int lowestValue = Integer.MAX_VALUE;
        for (int row = 0; row < depth; row++) {
            int newValue = table[row][getColumn(row, x)];
            if (newValue < lowestValue) {
                lowestValue = newValue;
            }
        }
        return lowestValue;
    }

    public synchronized void decay() {
        // Check decay counter again, this protects against double-decaying from multiple threads
        // TODO: I think this + synchronized block means we don't need a lock to prevent estimating/incrementing during decay?
        if (incrementCounter < decayPeriod) {
            return;
        }
        for (int row = 0; row < depth; row++) {
            for (int col = 0; col < width; col++) {
                table[row][col] >>>= 1;
            }
        }
        incrementCounter = 0;
        decayCount++;
    }

}
