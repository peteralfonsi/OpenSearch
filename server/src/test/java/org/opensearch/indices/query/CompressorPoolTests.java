/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.query;

import org.opensearch.test.OpenSearchTestCase;

public class CompressorPoolTests extends OpenSearchTestCase {
    public void testCompression() throws Exception {
        CompressorPool pool = new CompressorPool(10);
        byte[] orig = new byte[]{1, 2, 3, 4, 5, 6, 4, 5, 6, 7, 4, 5, 6, 7, 4, 5, 6};
        byte[] compressed = pool.compress(orig).get();
        byte[] decompressed = pool.decompress(compressed);
        assertArrayEquals(orig, decompressed);
        pool.shutdown();
    }

    // TODO
    public void testMultithreadedCompression() throws Exception {

    }
}
