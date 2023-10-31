/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.time;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;

public class AdHocSerializerBenchmark {

    @Setup
    public void setup() {

    }
    @Benchmark
    public String testDefaultJavaBytesReferenceSerializer() {
        // make BytesReference implement Serializable, use that instead of janky implementation, and time


    }
}
