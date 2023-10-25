/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.opensearch.common.Randomness;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.IndexService;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import org.ehcache.impl.serialization.ByteArraySerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;


// This class checks time taken for different serializer options for use in an ehcache disk tier.
// It's not intended to be merged into open source
// Some modifications to related classes also made for ease of testing
public class SerializerPerformanceTests extends OpenSearchSingleNodeTestCase {

    private final int REPS = 5000000; // how many total times to de/serialize
    private final int BATCH_SIZE = 50000; // how many keys to generate at once and time
    private final int NUM_BATCHES = REPS / BATCH_SIZE;
    private final int KEY_VALUE_LENGTH = 1000;
    private final String SERIALIZATION_FP = "/Users/petealft/Desktop/serialized.ser";
    public SerializerPerformanceTests() {
        super();
    }

    private class InfraHolder {
        IndexShard indexShard;
        IndicesRequestCache irc;
        EhcacheDiskCachingTier tier;
        IndicesService.IndexShardCacheEntity entity;
        private InfraHolder(IndexShard indexShard, IndicesRequestCache irc, EhcacheDiskCachingTier tier, IndicesService.IndexShardCacheEntity entity) {
            this.indexShard = indexShard;
            this.irc = irc;
            this.tier = tier;
            this.entity = entity;
        }
    }

    private InfraHolder getInfra() throws Exception {
        // this doesn't work in the constructor bc node is null for some reason - shouldn't super() init the nodes?
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = createIndex("test");
        final IndexShard indexShard = indexService.getShardOrNull(0);
        assertNotNull(indexShard);
        IndicesRequestCache irc = new IndicesRequestCache(Settings.EMPTY, indicesService);
        EhcacheDiskCachingTier tier = new EhcacheDiskCachingTier(1000000, 0, irc, "");
        IndicesService.IndexShardCacheEntity entity = indicesService.new IndexShardCacheEntity(indexShard);
        return new InfraHolder(indexShard, irc, tier, entity);
    }

    private EhcacheKey[] getEKeys(Random random, InfraHolder infra) throws Exception {
        EhcacheKey[] eKeys = new EhcacheKey[BATCH_SIZE];
        for (int i = 0; i < BATCH_SIZE; i++) {
            eKeys[i] = new EhcacheKey(getRandomIRCKey(KEY_VALUE_LENGTH, random, infra.irc, infra.tier, infra.entity));
        }
        return eKeys;
    }

    private byte[][] getEKeyBytes(Random random, InfraHolder infra) throws Exception {
        int byteLen = new EhcacheKey(getRandomIRCKey(KEY_VALUE_LENGTH, random, infra.irc, infra.tier, infra.entity)).getBytes().length;
        byte[][] eKeyBytes = new byte[BATCH_SIZE][byteLen];
        for (int i = 0; i < BATCH_SIZE; i++) {
            eKeyBytes[i] = new EhcacheKey(getRandomIRCKey(KEY_VALUE_LENGTH, random, infra.irc, infra.tier, infra.entity)).getBytes();
        }
        return eKeyBytes;
    }

    // splitting these into their own fns, it got clunky and i think all the IO stuff hanging around hurts performance

    /*private void defaultEhcacheKeySerializer(FileWriter fw, Random random, InfraHolder infra) throws Exception {
        long[] serBatchTimes = new long[NUM_BATCHES]; // batch serialization times in nano
        long[] deserBatchTimes = new long[NUM_BATCHES]; // batch deserialization times in nano
        double fileSizeMB = 0.0; // assuming its p constant run to run
        String serializerName = "DefaultEhcacheKeySerializer";

        for (int j = 0; j < NUM_BATCHES; j++) {
            System.gc();
            EhcacheKey[] eKeys = getEKeys(random, infra);
            FileOutputStream outFile = new FileOutputStream(SERIALIZATION_FP);
            ObjectOutputStream out = new ObjectOutputStream(outFile);

            System.gc();
            long now = System.nanoTime();
            for (int i = 0; i < BATCH_SIZE; i++) {
                out.writeObject(eKeys[i]);
            }
            serBatchTimes[j] = System.nanoTime() - now;
            System.out.println("Batch ser time = " + printMillis(serBatchTimes[j]));
            out.close();
            outFile.close();

            File sizeChecker = new File(SERIALIZATION_FP);
            fileSizeMB = (double) Files.size(Paths.get(SERIALIZATION_FP)) / (1048576);
            System.gc();

            FileInputStream inFile = new FileInputStream(SERIALIZATION_FP);
            ObjectInputStream in = new ObjectInputStream(inFile);

            now = System.nanoTime();
            for (int i = 0; i < BATCH_SIZE; i++) {
                EhcacheKey eKey = (EhcacheKey) in.readObject();
            }
            deserBatchTimes[j] = System.nanoTime() - now;
            System.out.println("Batch deser time = " + printMillis(deserBatchTimes[j]));

            in.close();
            inFile.close();
            System.gc();
        }
        for (int j = 0; j < NUM_BATCHES; j++) {
            toCSVLine(fw, new String[]{
                serializerName,
                Integer.toString(BATCH_SIZE),
                Double.toString((double) serBatchTimes[j] / 1000000),
                Double.toString((double) deserBatchTimes[j] / 1000000),
                Integer.toString(KEY_VALUE_LENGTH),
                Double.toString(fileSizeMB)
            });
        }
        fw.close();
    }*/


    private void defaultSerializerHelper(String type, FileWriter fw, Random random, InfraHolder infra) throws Exception {
        long[] serBatchTimes = new long[NUM_BATCHES]; // batch serialization times in nano
        long[] deserBatchTimes = new long[NUM_BATCHES]; // batch deserialization times in nano
        ByteArraySerializer ehcacheBytesSer = new ByteArraySerializer();
        double fileSizeMB = 0.0; // assuming its p constant run to run
        Kryo kryo = new Kryo();
        kryo.register(EhcacheKey.class);
        kryo.register(byte[].class);
        String serializerName = "";
        int byteLen = new EhcacheKey(getRandomIRCKey(KEY_VALUE_LENGTH, random, infra.irc, infra.tier, infra.entity)).getBytes().length;
        switch (type) {
            case "EhcacheKey":
                serializerName = "DefaultEhcacheKeySerializer";
                break;
            case "IRCKey":
                serializerName = "DefaultIRCKeySerializer";
                break;
            case "EhcacheBytes":
                serializerName = "BundledEhcacheBytesSerializer";
                break;
            case "Kryo":
                serializerName = "EhcacheKeyKryo";
                break;
            case "KryoBytes":
                serializerName = "EhcacheKeyBytesKryo";
                break;
            /*case "FST":
                serializerName = "EhcacheKeyFST";
                break;*/

        }
        for (int j = 0; j < NUM_BATCHES; j++) {
            System.gc();
            EhcacheKey[] eKeys = null;
            byte[][] eKeyBytes = null;

            switch (type) {
                case "EhcacheKey":
                case "Kryo":
                //case "FST":
                    eKeys = getEKeys(random, infra);
                    break;
                case "EhcacheBytes":
                case "KryoBytes":
                    eKeyBytes = getEKeyBytes(random, infra);
                    break;
            }

            // https://www.geeksforgeeks.org/serialization-in-java/
            FileOutputStream outFile = new FileOutputStream(SERIALIZATION_FP);
            FileChannel fc = outFile.getChannel();
            ObjectOutputStream out = new ObjectOutputStream(outFile);
            Output kryoOutput = new Output(outFile);
            //FSTObjectOutput fstOut = new FSTObjectOutput(outFile);

            System.gc();
            long now = System.nanoTime();
            switch (type) {
                case "EhcacheKey":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        out.writeObject(eKeys[i]);
                    }
                    break;
                case "EhcacheBytes":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        fc.write(ehcacheBytesSer.serialize(eKeyBytes[i]));
                    }
                    break;
                case "Kryo":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        kryo.writeObject(kryoOutput, eKeys[i]);
                    }
                    break;
                case "KryoBytes":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        kryo.writeObject(kryoOutput, eKeyBytes[i]);
                    }
                    break;
                /*case "FST":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        fstOut.writeObject(eKeys[i], EhcacheKey.class);
                    }
                    break;*/
            }
            serBatchTimes[j] = System.nanoTime() - now;
            System.out.println("Batch ser time = " + printMillis(serBatchTimes[j]));


            kryoOutput.close();
            out.close();
            fc.close();
            //fstOut.close();
            outFile.close();

            File sizeChecker = new File(SERIALIZATION_FP);
            fileSizeMB = (double) Files.size(Paths.get(SERIALIZATION_FP)) / (1048576);

            System.gc();

            FileInputStream inFile = new FileInputStream(SERIALIZATION_FP);
            FileChannel inFc = inFile.getChannel();
            ObjectInputStream in = new ObjectInputStream(inFile);
            Input kryoInput = new Input(inFile);
            //FSTObjectInput fstIn = new FSTObjectInput(inFile);

            now = System.nanoTime();
            switch (type) {
                case "EhcacheKey":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        EhcacheKey eKey = (EhcacheKey) in.readObject();
                    }
                    break;
                case "EhcacheBytes":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        byte[] eKeyBytesIndiv = ehcacheBytesSer.read(ByteBuffer.wrap(inFile.readNBytes(byteLen)));
                    }
                    break;
                case "Kryo":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        EhcacheKey eKey = kryo.readObject(kryoInput, EhcacheKey.class);
                    }
                    break;
                case "KryoBytes":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        byte[] eKeyBytesIndiv = kryo.readObject(kryoInput, byte[].class);
                    }
                    break;
                /*case "FST":
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        EhcacheKey eKey = (EhcacheKey) fstIn.readObject(EhcacheKey.class);
                    }
                    break;*/
            }

            deserBatchTimes[j] = System.nanoTime() - now;
            System.out.println("Batch deser time = " + printMillis(deserBatchTimes[j]));

            kryoInput.close();
            in.close();
            //fstIn.close();
            inFile.close();
            System.gc();
        }
        for (int j = 0; j < NUM_BATCHES; j++) {
            toCSVLine(fw, new String[]{
                serializerName,
                Integer.toString(BATCH_SIZE),
                Double.toString((double) serBatchTimes[j] / 1000000),
                Double.toString((double) deserBatchTimes[j] / 1000000),
                Integer.toString(KEY_VALUE_LENGTH),
                Double.toString(fileSizeMB)
            });
        }
    }

    public void testEhcacheKeySerializers() throws Exception {
        // the fns that fully serialize have to have diff logic from IRC key -> ehcache key
        Random random = Randomness.get();
        InfraHolder infra = getInfra();
        FileWriter fw = doCsvHeader();

        //defaultSerializerHelper("IRCKey", fw, random, infra); // have to make a Bunch of stuff serializable to enable this, but just to check...
        //defaultSerializerHelper("FST", fw, random, infra);
        defaultSerializerHelper("Kryo", fw, random, infra);
        defaultSerializerHelper("KryoBytes", fw, random, infra);
        defaultSerializerHelper("EhcacheBytes", fw, random, infra);
        defaultSerializerHelper("EhcacheKey", fw, random, infra);
        testTimeKeyToEhcacheKey(fw, random, infra);
        //defaultEhcacheKeySerializer(fw, random, infra);
        fw.close();
    }
    public void testTimeKeyToEhcacheKey(FileWriter fw, Random random, InfraHolder infra) throws Exception {

        long[] serBatchTimes = new long[NUM_BATCHES]; // batch serialization times in nano
        long[] deserBatchTimes = new long[NUM_BATCHES]; // batch deserialization times in nano
        for (int j = 0; j < NUM_BATCHES; j++) {
            System.gc();
            IndicesRequestCache.Key[] keys = new IndicesRequestCache.Key[BATCH_SIZE];
            EhcacheKey[] serKeys = new EhcacheKey[BATCH_SIZE];
            for (int i = 0; i < BATCH_SIZE; i++) {
                keys[i] = getRandomIRCKey(KEY_VALUE_LENGTH, random, infra.irc, infra.tier, infra.entity);
            }
            // We run serialization twice -- we need the serialized values stored somewhere to test deserialization,
            // but we don't want to include array add time in our time took

            // first run - testing time took
            System.gc();
            long now = System.nanoTime();
            for (int i = 0; i < BATCH_SIZE; i++) {
                EhcacheKey eKey = new EhcacheKey(keys[i]);
            }
            serBatchTimes[j] = System.nanoTime() - now;
            System.out.println("Batch ser time = " + printMillis(serBatchTimes[j]));

            // run it again, storing them this time
            System.gc();
            now = System.nanoTime();
            for (int i = 0; i < BATCH_SIZE; i++) {
                serKeys[i] = new EhcacheKey(keys[i]);
            }
            System.out.println("Addtl time from array adds = " + printMillis(((System.nanoTime() - now) - serBatchTimes[j])));

            System.gc();
            now = System.nanoTime();
            for (int i = 0; i < BATCH_SIZE; i++) {
                infra.tier.convertEhcacheKeyToOriginal(serKeys[i]);
            }
            deserBatchTimes[j] = System.nanoTime() - now;
            System.out.println("Batch deser time = " + printMillis(deserBatchTimes[j]));
            System.gc();
        }
        for (int j = 0; j < NUM_BATCHES; j++) {
            // doing this inside the main loop strangely seems to affect the took time a lot
            toCSVLine(fw, new String[]{
                "KeyToEhcacheKey",
                Integer.toString(BATCH_SIZE),
                Double.toString((double) serBatchTimes[j] / 1000000),
                Double.toString((double) deserBatchTimes[j] / 1000000),
                Integer.toString(KEY_VALUE_LENGTH)
            });
        }
        fw.close();
    }

    private FileWriter doCsvHeader() throws Exception {
        String[] csvHeader = new String[] {
            "Serializer",
            "Batch size",
            "Serialization time (ms)",
            "Deserialization time (ms)",
            "Key value length",
            "Compressed file size (MB)"
        };
        File csvFile = new File("/Users/petealft/Desktop/serializer_performance.csv");
        csvFile.createNewFile();
        FileWriter fw = new FileWriter(csvFile, true);
        if (csvFile.length() == 0) {
            toCSVLine(fw, csvHeader);
        }
        return fw;
    }

    private static void toCSVLine(FileWriter fw, String[] lineArr) throws IOException {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < lineArr.length; i++) {
            line.append(lineArr[i]);
            if (i < lineArr.length-1) {
                line.append(",");
            } else {
                line.append("\n");
            }
        }
        fw.write(line.toString());
    }

    private String printMillis(long nanos) {
        return "" + (double) nanos / 1000000 + " (ms)";
    }

    private IndicesRequestCache.Key getRandomIRCKey(int valueLength, Random random, IndicesRequestCache irc, EhcacheDiskCachingTier tier, IndicesService.IndexShardCacheEntity entity) {
        byte[] value = new byte[valueLength];
        //random.nextBytes(value);
        for (int i = 0; i < valueLength; i++) {
            value[i] = (byte) (random.nextInt(126 - 32) + 32);
        }
        BytesReference keyValue = new BytesArray(value);
        return irc.new Key(entity, keyValue, UUID.randomUUID().toString()); // same UUID source as used in real key
    }
}

