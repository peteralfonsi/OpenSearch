/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.countminsketch;

import org.opensearch.common.util.concurrent.ReleasableLock;

public class CountMinSketch {
    private final int depth;
    private final int width;
    private final int[][] table;
    private final int decayPeriod;
    private int decayCounter; // Set to -1 for it to never decay
    private ReleasableLock decayLock;

    public CountMinSketch(int depth, int width, int decayPeriod) {
        this.depth = depth;
        this.width = width;
        this.table = new int[depth][width];
        this.decayPeriod = decayPeriod;
        this.decayCounter = 0;
    }

    private int hash(int x) {
        // TODO - Integer.hashCode() just returns its own value lol
        // This doesn't have to be too good, and should be relatively cheap
        // Taken from https://stackoverflow.com/questions/6082915/a-good-hash-function-to-use-in-interviews-for-integer-numbers-strings
        x ^= (x << 13);
        x ^= (x >>> 17);
        x ^= (x << 5);
        return x;
    }

    private int getColumn(int row, int x) {
        /*
        If we have one well-behaved hash function h(x), it should be reasonable to produce a
        hash function h_r(x) for each row r by just taking the default hash function and adding r as an offset:
        h_r(x) = h(x + r)
        */
        // TODO: It sort of seems like row * x performs better than row + x for errors. Overall the errors are much worse than anticipated. I may not like this hash fn.
        return Math.abs(hash(row * x)) % width;
    }

    public void increment(int x) {
        for (int row = 0; row < depth; row++) {
            // TODO: Check overflow
            table[row][getColumn(row, x)] += 1;
        }
        decayCounter++;
        if (decayCounter >= decayPeriod && decayPeriod >= 0) {
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
        if (decayCounter < decayPeriod) {
            return;
        }
        for (int row = 0; row < depth; row++) {
            for (int col = 0; col < width; col++) {
                table[row][col] /= 2;
            }
        }
        decayCounter = 0;
    }

}
