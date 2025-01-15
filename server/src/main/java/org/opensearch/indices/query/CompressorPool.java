/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.query;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.compress.LZ4;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CompressorPool {
    private final ExecutorService executor;
    private final ThreadLocal<LZ4Compressor> threadLocalCompressor;

    public CompressorPool(int poolSize) {
        this.executor = Executors.newFixedThreadPool(poolSize);
        this.threadLocalCompressor = ThreadLocal.withInitial(LZ4Compressor::new);
    }

    public Future<byte[]> compress(byte[] data) {
        return executor.submit(() -> {
            LZ4Compressor compressor = threadLocalCompressor.get();
            return compressor.compress(data);
        });
    }

    public void shutdown() {
        executor.shutdown();
    }

    public byte[] decompress(byte[] compressed) throws IOException {
        // Decompression is threadsafe already (?)
        ByteArrayDataInput in = new ByteArrayDataInput(compressed);
        int originalLength = in.readInt();
        byte[] result = new byte[originalLength];
        LZ4.decompress(in, originalLength, result, 0);
        return result;
    }

    static class LZ4Compressor {
        private final static int scratchOverhead = 256;

        private final LZ4.FastCompressionHashTable ht;

        LZ4Compressor() {
            ht = new LZ4.FastCompressionHashTable();
        }

        public byte[] compress(byte[] bytes) throws IOException {
            byte[] result = new byte[bytes.length + 4 + scratchOverhead]; // We seem to need overhead? At least for short byte[]
            ByteArrayDataOutput out = new ByteArrayDataOutput(result);
            // Write the original length into out; we need it at decompression time
            out.writeInt(bytes.length);
            LZ4.compress(bytes, 0, bytes.length, out, ht);
            int finalLength = out.getPosition();
            byte[] trimmed = new byte[finalLength];
            System.arraycopy(result, 0, trimmed, 0, finalLength);
            return trimmed;
        }
    }
}


