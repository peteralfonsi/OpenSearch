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

package org.opensearch.index.fielddata;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.indices.fielddata.cache.IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class FieldDataLoadingIT extends OpenSearchIntegTestCase {
    // To shorten runtimes, set cluster setting INDICES_CACHE_CLEAN_INTERVAL_SETTING to a lower value.
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), "5s") // TODO: For testing, increasing this from 1ms (not seeing any flaking) to 5 sec
            .build();
    }

    public void testEagerGlobalOrdinalsFieldDataLoading() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("name")
                    .field("type", "text")
                    .field("fielddata", true)
                    .field("eager_global_ordinals", true)
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "name").get();
        client().admin().indices().prepareRefresh("test").get();

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), greaterThan(0L));

        // Ensure cache cleared before other tests in the suite begin
        client().admin().indices().clearCache(new ClearIndicesCacheRequest().fieldDataCache(true)).actionGet();
        assertBusy(() -> {
            ClusterStatsResponse clearedResponse = client().admin().cluster().prepareClusterStats().get();
            assertEquals(0, clearedResponse.getIndicesStats().getFieldData().getMemorySizeInBytes());
        });
    }

    public void testIndicesFieldDataCacheSizeSetting() throws Exception {
        // Put one value into the cache
        int maxEntries = 10;
        int numFields = 2 * maxEntries;
        String fieldPrefix = "field";
        createIndex("index", numFields, fieldPrefix);
        client().prepareSearch("index").setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + "0", SortOrder.ASC).get();
        long sizePerEntry = client().admin().cluster().prepareClusterStats().get().getIndicesStats().getFieldData().getMemorySizeInBytes();
        assertTrue(sizePerEntry > 0);

        // Set the max size setting so that it can fit maxEntries such entries
        long maxSize = maxEntries * sizePerEntry + 1;
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(INDICES_FIELDDATA_CACHE_SIZE_KEY.getKey(), maxSize + "b"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Add >N values to the cache and assert the size is less than the limit
        for (int i = 1; i < numFields; i++) {
            client().prepareSearch("index").setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + i, SortOrder.ASC).get();
        }
        long cacheSize = client().admin().cluster().prepareClusterStats().get().getIndicesStats().getFieldData().getMemorySizeInBytes();
        assertTrue(cacheSize <= maxSize && cacheSize > sizePerEntry);

        // Set the max size setting to a smaller value and assert the new size is less than that (waiting for refresh)
        long newMaxSize = 2 * sizePerEntry + 1;
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(INDICES_FIELDDATA_CACHE_SIZE_KEY.getKey(), newMaxSize + "b"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        assertBusy(() -> {
            long newCacheSize = client().admin()
                .cluster()
                .prepareClusterStats()
                .get()
                .getIndicesStats()
                .getFieldData()
                .getMemorySizeInBytes();
            assertTrue(newCacheSize <= newMaxSize);
        });
    }

    private void createIndex(String index, int numFieldsPerIndex, String fieldPrefix) throws Exception {
        assert numFieldsPerIndex >= 1;
        XContentBuilder req = jsonBuilder().startObject().startObject("properties");
        for (int j = 0; j < numFieldsPerIndex; j++) {
            req.startObject(fieldPrefix + j).field("type", "text").field("fielddata", true).endObject();
        }
        req.endObject().endObject();
        assertAcked(prepareCreate(index).setMapping(req));
        Map<String, String> source = new HashMap<>();
        for (int j = 0; j < numFieldsPerIndex; j++) {
            source.put(fieldPrefix + j, "value");
        }
        client().prepareIndex(index).setId("1").setSource(source).get();
        client().admin().indices().prepareRefresh(index).get();

        // Put something into the cache and clear it, waiting for stats to return to 0.
        // Index creation temporarily opens + closes a test index using IndicesService.withTempIndexService()
        // that has the same name as the real index,
        // and this ensures the clear resulting from that close has completed before we go to the actual test.
        client().prepareSearch(index).setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + "0", SortOrder.ASC).get();
        client().admin().indices().clearCache(new ClearIndicesCacheRequest().fieldDataCache(true)).actionGet();
        assertBusy(() -> {
            ClusterStatsResponse clearedResponse = client().admin().cluster().prepareClusterStats().get();
            assertEquals(0, clearedResponse.getIndicesStats().getFieldData().getMemorySizeInBytes());
        });
    }

    public void testFieldDataCacheClearConcurrentIndices() throws Exception {
        // Check concurrently clearing multiple indices from the FD cache correctly removes all expected keys.
        int numIndices = 10;
        String indexPrefix = "test";
        createAndSearchIndices(numIndices, 1, indexPrefix, "field");
        // TODO: Should be 1 entry per field per index in cache, but cannot check this directly until we add the items count stat in a
        // future PR

        // Concurrently clear multiple indices from FD cache
        Thread[] threads = new Thread[numIndices];
        Phaser phaser = new Phaser(numIndices + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numIndices);

        for (int i = 0; i < numIndices; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                try {
                    ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest().fieldDataCache(true)
                        .indices(indexPrefix + finalI);
                    client().admin().indices().clearCache(clearCacheRequest).actionGet();
                    phaser.arriveAndAwaitAdvance();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        // Cache size should be 0
        assertBusy(() -> {
            ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
            assertEquals(0, response.getIndicesStats().getFieldData().getMemorySizeInBytes());
        });
    }

    public void testFieldDataCacheClearConcurrentFields() throws Exception {
        // Check concurrently clearing multiple indices + fields from the FD cache correctly removes all expected keys.
        int numIndices = 10;
        int numFieldsPerIndex = 5;
        String indexPrefix = "test";
        String fieldPrefix = "field";
        createAndSearchIndices(numIndices, numFieldsPerIndex, indexPrefix, fieldPrefix);

        // Concurrently clear multiple indices+fields from FD cache
        Thread[] threads = new Thread[numIndices * numFieldsPerIndex];
        Phaser phaser = new Phaser(numIndices * numFieldsPerIndex + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numIndices * numFieldsPerIndex);

        for (int i = 0; i < numIndices; i++) {
            int finalI = i;
            for (int j = 0; j < numFieldsPerIndex; j++) {
                int finalJ = j;
                threads[i * numFieldsPerIndex + j] = new Thread(() -> {
                    try {
                        ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest().fieldDataCache(true)
                            .indices(indexPrefix + finalI)
                            .fields(fieldPrefix + finalJ);
                        client().admin().indices().clearCache(clearCacheRequest).actionGet();
                        phaser.arriveAndAwaitAdvance();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    countDownLatch.countDown();
                });
                threads[i * numFieldsPerIndex + j].start();
            }
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        // Cache size should be 0
        assertBusy(() -> {
            ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
            assertEquals(0, response.getIndicesStats().getFieldData().getMemorySizeInBytes());
        });
    }

    public void testFieldDataCacheClearOnCloseIndices() throws Exception {
        int numIndices = 10;
        int numFieldsPerIndex = 5;
        String indexPrefix = "test";
        String fieldPrefix = "field";
        createAndSearchIndices(numIndices, numFieldsPerIndex, indexPrefix, fieldPrefix);

        // Close all indices
        for (int i = 0; i < numIndices; i++) {
            client().admin().indices().prepareClose(indexPrefix + i).get();
        }

        // Cache size should be 0
        assertBusy(() -> {
            ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
            assertEquals(0, response.getIndicesStats().getFieldData().getMemorySizeInBytes());
        });
    }

    public void testFieldDataCacheConcurrentCloseAndClear() throws Exception {
        int numIndices = 50;
        int numFieldsPerIndex = 5;
        String indexPrefix = "test";
        String fieldPrefix = "field";
        createAndSearchIndices(numIndices, numFieldsPerIndex, indexPrefix, fieldPrefix);

        // Concurrently close and clear the same indices
        Thread[] threads = new Thread[numIndices * 2];
        Phaser phaser = new Phaser(numIndices * 2 + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numIndices * 2);

        for (int i = 0; i < numIndices; i++) {
            int finalI = i;
            threads[i * 2] = new Thread(() -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    client().admin().indices().prepareClose(indexPrefix + finalI).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i * 2 + 1] = new Thread(() -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest().fieldDataCache(true)
                        .indices(indexPrefix + finalI);
                    client().admin().indices().clearCache(clearCacheRequest).actionGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i * 2].start();
            threads[i * 2 + 1].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        // Cache size should be 0 and not negative
        assertBusy(() -> {
            ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
            long cacheSize = response.getIndicesStats().getFieldData().getMemorySizeInBytes();
            assertEquals(0, cacheSize);
        });
    }

    public void testFieldDataStatsConcurrentForceMerge() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(2);
        internalCluster().startDataOnlyNodes(3);
        int numIndices = 30;
        int numFieldsPerIndex = 5;
        String indexPrefix = "test";
        String fieldPrefix = "field";

        // Create indices with multiple segments
        for (int i = 0; i < numIndices; i++) {
            String index = indexPrefix + i;
            XContentBuilder req = jsonBuilder().startObject().startObject("properties");
            for (int j = 0; j < numFieldsPerIndex; j++) {
                req.startObject(fieldPrefix + j).field("type", "text").field("fielddata", true).endObject();
            }
            req.endObject().endObject();
            assertAcked(prepareCreate(index).setMapping(req).setSettings(Settings.builder().put("refresh_interval", -1).put("number_of_replicas", 1)));

            // Index multiple docs without refresh to create multiple segments
            Map<String, String> source = new HashMap<>();
            for (int j = 0; j < numFieldsPerIndex; j++) {
                source.put(fieldPrefix + j, "value");
            }
            for (int doc = 0; doc < 70; doc++) {
                client().prepareIndex(index).setId(String.valueOf(doc)).setSource(source).get();
                client().admin().indices().prepareFlush(index).get();
            }
            client().admin().indices().prepareRefresh(index).get();

            // Verify multiple segments exist
            long segmentCount = client().admin().indices().prepareStats(index).get().getIndex(index).getTotal().getSegments().getCount();
            assertTrue("Index " + index + " has only " + segmentCount + " segment(s)", segmentCount > 1);

            // Search to populate field data cache
            for (int j = 0; j < numFieldsPerIndex; j++) {
                client().prepareSearch(index).setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + j, SortOrder.ASC).get();
            }
        }
        ensureGreen();

        // Concurrently force merge all indices and check stats
        Thread[] mergeThreads = new Thread[numIndices];
        Thread[] clearThreads = new Thread[numIndices / 2];
        Phaser phaser = new Phaser(numIndices + numIndices / 2 + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numIndices + numIndices / 2);
        AtomicBoolean stopChecking = new AtomicBoolean(false);

        Thread statsChecker = new Thread(() -> {
            while (!stopChecking.get()) {
                try {
                    ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
                    assertNotNull(response.getIndicesStats());
                    assertNotNull(response.getIndicesStats().getFieldData());
                    long cacheSize = response.getIndicesStats().getFieldData().getMemorySizeInBytes();
                    assertTrue("Cache size is negative: " + cacheSize, cacheSize >= 0);
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        statsChecker.start();

        for (int i = 0; i < numIndices; i++) {
            int finalI = i;
            mergeThreads[i] = new Thread(() -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    client().admin().indices().prepareForceMerge(indexPrefix + finalI).setMaxNumSegments(1).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            mergeThreads[i].start();
        }

        // Clear cache on 50% of indices concurrently
        for (int i = 0; i < numIndices / 2; i++) {
            int finalI = i;
            clearThreads[i] = new Thread(() -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest().fieldDataCache(true)
                        .indices(indexPrefix + finalI);
                    client().admin().indices().clearCache(clearCacheRequest).actionGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            clearThreads[i].start();
        }

        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        stopChecking.set(true);
        statsChecker.join();

        // Final check
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertNotNull(response.getIndicesStats());
        assertNotNull(response.getIndicesStats().getFieldData());
        long cacheSize = response.getIndicesStats().getFieldData().getMemorySizeInBytes();
        assertTrue(cacheSize >= 0);
    }

    public void testFieldDataCacheClearOnShardRelocation() throws Exception {
        String node_1 = internalCluster().startNode(Settings.builder().build());
        Client client = client(node_1);

        assertEquals(1, cluster().size());
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("1").execute().actionGet();
        assertFalse(healthResponse.isTimedOut());

        int numIndices = 20;
        String indexPrefix = "test";
        String fieldPrefix = "field";
        int numFields = 5;

        // Create indices and populate field data cache
        for (int i = 0; i < numIndices; i++) {
            String index = indexPrefix + i;
            XContentBuilder req = jsonBuilder().startObject().startObject("properties");
            for (int j = 0; j < numFields; j++) {
                req.startObject(fieldPrefix + j).field("type", "text").field("fielddata", true).endObject();
            }
            req.endObject().endObject();
            assertAcked(
                prepareCreate(index).setMapping(req)
                    .setSettings(
                        Settings.builder()
                            .put("number_of_shards", 1)
                            .put("number_of_replicas", 0)
                            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                    )
            );

            Map<String, String> source = new HashMap<>();
            for (int j = 0; j < numFields; j++) {
                source.put(fieldPrefix + j, "value");
            }
            client().prepareIndex(index).setId("1").setSource(source).get();
            client().admin().indices().prepareRefresh(index).get();
        }

        ensureGreen();

        // Populate field data cache after all indices are ready
        for (int i = 0; i < numIndices; i++) {
            String index = indexPrefix + i;
            for (int j = 0; j < numFields; j++) {
                client().prepareSearch(index).setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + j, SortOrder.ASC).get();
            }
        }

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertTrue(response.getIndicesStats().getFieldData().getMemorySizeInBytes() > 0);

        Settings newSettings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE.name())
            .build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(newSettings).execute().actionGet();

        String node_2 = internalCluster().startDataOnlyNode(Settings.builder().build());
        assertEquals(2, cluster().size());
        healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertFalse(healthResponse.isTimedOut());

        // Move shards and randomly clear cache concurrently
        Thread[] moveThreads = new Thread[numIndices];
        Thread[] clearThreads = new Thread[numIndices / 2];
        Phaser phaser = new Phaser(numIndices + numIndices / 2 + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numIndices + numIndices / 2);

        // Move all shards to node_2 first, sequentially
        for (int i = 0; i < numIndices; i++) {
            String index = indexPrefix + i;
            MoveAllocationCommand cmd = new MoveAllocationCommand(index, 0, node_1, node_2);
            internalCluster().client().admin().cluster().prepareReroute().add(cmd).get();
        }
        client().admin().cluster().prepareHealth().setWaitForNoRelocatingShards(true).setWaitForNoInitializingShards(true).get();

        // Move all shards back to node_1 concurrently with cache clears
        for (int i = 0; i < numIndices; i++) {
            int finalI = i;
            String index = indexPrefix + i;
            moveThreads[i] = new Thread(() -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    MoveAllocationCommand cmd = new MoveAllocationCommand(index, 0, node_2, node_1);
                    internalCluster().client().admin().cluster().prepareReroute().add(cmd).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            moveThreads[i].start();
        }

        // Clear cache on 50% of indices concurrently with moves back
        for (int i = 0; i < numIndices / 2; i++) {
            int finalI = i;
            clearThreads[i] = new Thread(() -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest().fieldDataCache(true)
                        .indices(indexPrefix + finalI);
                    client().admin().indices().clearCache(clearCacheRequest).actionGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            clearThreads[i].start();
        }

        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .get();
        assertFalse(clusterHealth.isTimedOut());

        // Close all indices and verify cache is cleared
        for (int i = 0; i < numIndices; i++) {
            client().admin().indices().prepareClose(indexPrefix + i).get();
        }

        assertBusy(() -> {
            ClusterStatsResponse clearedResponse = client().admin().cluster().prepareClusterStats().get();
            long cacheSize = clearedResponse.getIndicesStats().getFieldData().getMemorySizeInBytes();
            assertTrue("Cache size is negative: " + cacheSize, cacheSize >= 0);
            assertEquals(0, cacheSize);
        });
    }

    private void createAndSearchIndices(int numIndices, int numFieldsPerIndex, String indexPrefix, String fieldPrefix) throws Exception {
        for (int i = 0; i < numIndices; i++) {
            String index = indexPrefix + i;
            createIndex(index, numFieldsPerIndex, fieldPrefix);
        }
        // Separate loop to ensure createIndex() handles any cache wipe from opening+closing the temporary test index
        for (int i = 0; i < numIndices; i++) {
            // Search on each index to fill the cache
            String index = indexPrefix + i;
            for (int j = 0; j < numFieldsPerIndex; j++) {
                client().prepareSearch(index).setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + j, SortOrder.ASC).get();
            }
        }
        ensureGreen();
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertTrue(response.getIndicesStats().getFieldData().getMemorySizeInBytes() > 0L);
    }
}
