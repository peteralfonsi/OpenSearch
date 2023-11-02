/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.opensearch.common.Randomness;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.common.bytes.PagedBytesReference;
import org.opensearch.core.common.util.ByteArray;
import org.opensearch.test.OpenSearchTestCase;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class BytesReferenceSerializerTests extends OpenSearchTestCase {
    public void testClassUnchanged() throws Exception {
        // check that, for each supported type of BytesReference, the class remains unchanged after deserialization

        BytesReferenceSerializer ser = new BytesReferenceSerializer();
        Random rand = Randomness.get();
        byte[] testValue = new byte[1000];
        rand.nextBytes(testValue);

        /*BytesArray.class,
            CompositeBytesReference.class,
            PagedBytesReference.class,
            ReleasableBytesReference.class*/

        BytesReference bytesArray = new BytesArray(testValue);
        ByteBuffer serialized = ser.serialize(bytesArray);
        assertTrue(ser.equals(bytesArray, serialized));
        BytesReference deserialized = ser.deserialize(serialized);
        assertEquals(bytesArray.getClass(), deserialized.getClass());
        assertEquals(bytesArray, deserialized); // AbstractBytesReference checks equality using iterator, so should be ok

        // this actually returns a BytesArray when .of() is given only one argument
        BytesReference composite = CompositeBytesReference.of(new BytesArray(testValue));
        serialized = ser.serialize(composite);
        assertTrue(ser.equals(composite, serialized));
        deserialized = ser.deserialize(serialized);
        assertEquals(composite.getClass(), deserialized.getClass());
        assertEquals(composite, deserialized);

        // this one is actually a CompositeBytesReference
        BytesReference doubleComposite = CompositeBytesReference.of(new BytesArray(testValue), new BytesArray(testValue));
        assertEquals(CompositeBytesReference.class, doubleComposite.getClass());
        serialized = ser.serialize(doubleComposite);
        assertTrue(ser.equals(doubleComposite, serialized));
        deserialized = ser.deserialize(serialized);
        assertEquals(doubleComposite.getClass(), deserialized.getClass());
        assertEquals(doubleComposite, deserialized);

        ByteArray arr = BigArrays.NON_RECYCLING_INSTANCE.newByteArray(testValue.length);
        arr.set(0L, testValue, 0, testValue.length);
        assertFalse(arr.hasArray());
        BytesReference paged = BytesReference.fromByteArray(arr, testValue.length);
        assertEquals(PagedBytesReference.class, paged.getClass());
    }

    // not actually belonging in this test, just for debug...

    public void testEhcacheOptionOneImpl() throws Exception {
        EhcacheSolutionOptionOne.Builder<BytesReference, BytesReference> builder = new EhcacheSolutionOptionOne.Builder<>();
        builder.setMaximumWeightInBytes(10000000)
            .setKeyType(BytesReference.class)
            .setValueType(BytesReference.class)
            .setKeySerializer(new BytesReferenceSerializer())
            .setValueSerializer(new BytesReferenceSerializer())
            .setExpireAfterAccess(new TimeValue(1, TimeUnit.DAYS))
            .setStoragePath("/tmp/OptionOne")
            .setThreadPoolAlias("ehcacheTest")
            .setSettings(Settings.builder().build())
            .setIsEventListenerModeSync(true);
        EhcacheSolutionOptionOne<BytesReference, BytesReference> optionOneTier = builder.build();
        int iterations = 10;
        int keySize = 1000;

        BytesReference[] keys = new BytesReference[iterations];
        BytesReference[] values = new BytesReference[iterations];
        byte[] keyContent = new byte[keySize];
        byte[] valueContent = new byte[keySize];
        Random rand = Randomness.get();
        for (int i = 0; i < iterations; i++) {
            rand.nextBytes(keyContent);
            rand.nextBytes(valueContent);
            keys[i] = new BytesArray(keyContent);
            values[i] = new BytesArray(valueContent);
            optionOneTier.put(keys[i], values[i]);
        }
        // add paged, test bytes references
        optionOneTier.close();

    }

    public void testEhcacheOptionTwoImpl() throws Exception {
        EhcacheSolutionOptionTwo.Builder<Integer, BytesReference> builder = new EhcacheSolutionOptionTwo.Builder<>();
        builder.setMaximumWeightInBytes(10000000)
            .setKeyType(Integer.class)
            .setValueType(BytesReference.class)
            .setValueSerializer(new BytesReferenceSerializerOptionTwo())
            .setExpireAfterAccess(new TimeValue(1, TimeUnit.DAYS))
            .setStoragePath("/tmp/OptionTwo")
            .setThreadPoolAlias("ehcacheTest")
            .setSettings(Settings.builder().build())
            .setIsEventListenerModeSync(true);
        EhcacheSolutionOptionTwo<Integer, BytesReference> optionTwoTier = builder.build();
        int iterations = 1;
        int keySize = 1000;

        //BytesReference[] keys = new BytesReference[iterations];
        BytesReference[] values = new BytesReference[iterations];
        //byte[] keyContent = new byte[keySize];
        byte[] valueContent = new byte[keySize];
        Random rand = Randomness.get();
        for (int i = 0; i < iterations; i++) {
            //rand.nextBytes(keyContent);
            rand.nextBytes(valueContent);
            //keys[i] = new BytesArray(keyContent);
            values[i] = new BytesArray(valueContent);
            optionTwoTier.put(i, values[i]);
        }
        // add paged, test bytes references
        BytesReference res = optionTwoTier.get(0);
        assertNotNull(res);
        optionTwoTier.close();
    }

    public org.ehcache.Cache<byte[], byte[]> getSimpleCache() {
        String threadPoolAlias = "ehcacheTest";
        String DISK_CACHE_ALIAS = "";
        PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
            .with(CacheManagerBuilder.persistence(new File("/tmp/OptionTwo")))
            .using(PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder()
                .defaultPool("ehcachePool" + "Default", 1, 3) // Default pool used for other tasks like
                // event listeners
                .pool(threadPoolAlias, 2,
                    2).build())
            .build(true);
        CacheConfigurationBuilder<byte[], byte[]> cacheBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(
                byte[].class,
                byte[].class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().disk(1000000000, MemoryUnit.B))
            .withService(new OffHeapDiskStoreConfiguration(threadPoolAlias, 2,
                16));
        return cacheManager.createCache(DISK_CACHE_ALIAS, cacheBuilder);
    }
    public void testSimpleBytesArrEhcache() throws Exception {
        org.ehcache.Cache<byte[], byte[]> cache = getSimpleCache();
        byte[] key = new byte[]{3};
        byte[] value = new byte[]{45};
        cache.put(key, value);
        byte[] result = cache.get(key);
        //byte[] laterKey = new byte[]{3};
        //byte[] result = cache.get(laterKey);
    }

}
