/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tdigest.TDigestDouble;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;

/**
 * TODO: For testing purposes only, this now uses Apache Datasketches impl
 *
 * @opensearch.internal
 */
public class TDigestState {
    private final TDigestDouble sketch;

    public TDigestState(double compression) {
        /*super(compression);
        this.compression = compression;*/
        this.sketch = new TDigestDouble((short) compression); // TODO: compression appears to be same as k
    }

    private TDigestState(TDigestDouble in) {
        this.sketch = in;
    }

    // TODO: Will have to replicate the following methods from AVLTreeDigest:
    //   centroidCount(), weighted add(), maybe compress() (nope), size(), maybe cdf(), quantile(), compression(), maybe centroids()

    public double compression() {
        return sketch.getK();
    }

    public long size() {
        return sketch.getTotalWeight();
    }

    public double quantile(double q) {
        return sketch.getQuantile(q);
    }

    public double cdf(double x) {
        // TODO - not sure if this is right
        return sketch.getRank(x);
    }

    public Collection<Centroid> centroids() {
        // TODO: Not available. This is only used in InternalMedianAbsoluteDeviation, so that one would have to use tdigest impl unless theres a diff way.
        return List.of();
    }

    public void add(double x, int w) {
        for (int i = 0; i < w; i++) {
            add(x);
        }
    }
    public void add(double x) {
        this.sketch.update(x);
    }

    public void add(TDigestState otherState) {

    }


    public static void write(TDigestState state, StreamOutput out) throws IOException {
        /*out.writeDouble(state.compression);
        out.writeVInt(state.centroidCount());
        for (Centroid centroid : state.centroids()) {
            out.writeDouble(centroid.mean());
            out.writeVLong(centroid.count());
        }*/
        byte[] serialized = state.sketch.toByteArray();
        out.writeByteArray(serialized);
    }

    public static TDigestState read(StreamInput in) throws IOException {
        /*double compression = in.readDouble();
        TDigestState state = new TDigestState(compression);
        int n = in.readVInt();
        for (int i = 0; i < n; i++) {
            state.add(in.readDouble(), in.readVInt());
        }
        return state;*/
        byte[] bytes = in.readByteArray();
        return new TDigestState(TDigestDouble.heapify(Memory.wrap(bytes)));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj instanceof TDigestState == false) {
            return false;
        }
        TDigestState that = (TDigestState) obj;
        if (this.compression() != that.compression()) {
            return false;
        }
        // TODO: Under same ordering constraints as original MergingDigest we can probably compare equality by serialized state
        return Arrays.equals(this.sketch.toByteArray(), that.sketch.toByteArray());
        /*Iterator<? extends Centroid> thisCentroids = centroids().iterator();
        Iterator<? extends Centroid> thatCentroids = that.centroids().iterator();
        while (thisCentroids.hasNext()) {
            if (thatCentroids.hasNext() == false) {
                return false;
            }
            Centroid thisNext = thisCentroids.next();
            Centroid thatNext = thatCentroids.next();
            if (thisNext.mean() != thatNext.mean() || thisNext.count() != thatNext.count()) {
                return false;
            }
        }
        return thatCentroids.hasNext() == false;*/
    }

    @Override
    public int hashCode() {
        int h = getClass().hashCode();
        h = 31 * h + Double.hashCode(compression());
        h = 31 * h + Double.hashCode(cdf(0.2)); // TODO: Lol idk, we dont have access to the centroids so
        return h;
        /*int h = getClass().hashCode();
        h = 31 * h + Double.hashCode(compression);
        for (Centroid centroid : centroids()) {
            h = 31 * h + Double.hashCode(centroid.mean());
            h = 31 * h + centroid.count();
        }
        return h;*/
    }
}
