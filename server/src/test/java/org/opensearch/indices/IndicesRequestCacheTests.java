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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.indices;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.OriginalIndicesTests;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.UUIDs;
import org.opensearch.common.cache.tier.DiskCachingTier;
import org.opensearch.common.cache.tier.EhCacheDiskCachingTier;
import org.opensearch.common.cache.tier.TieredCacheService;
import org.opensearch.common.cache.tier.TieredCacheSpilloverStrategyService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.AbstractBytesReference;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.index.IndexService;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesRequestCache.CacheEntity;
import org.opensearch.indices.IndicesRequestCache.Key;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class IndicesRequestCacheTests extends OpenSearchSingleNodeTestCase {
    public IndexShard getIndexShardCache() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardId shardId = mock(ShardId.class);
        when(indexShard.shardId()).thenReturn(shardId);
        return indexShard;
    }

    public void testBasicOperationsCache() throws Exception {
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.EMPTY,
            getInstanceFromNode(IndicesService.class),
            dummyClusterSettings
        );
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        IndexShard indexShard = getIndexShardCache();

        // initial cache
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // Closing the cache doesn't modify an already returned CacheEntity
        if (randomBoolean()) {
            reader.close();
        } else {
            entity.setIsOpen(false);
            cache.clear(entity);
        }
        cache.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testCacheDifferentReaders() throws Exception {
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, getInstanceFromNode(IndicesService.class), dummyClusterSettings);
        IndexShard indexShard = getIndexShardCache();
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        // initial cache
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        final int cacheSize = requestCacheStats.stats().getMemorySize().bytesAsInt();
        assertEquals(1, cache.numRegisteredCloseListeners());

        // cache the second
        TestEntity secondEntity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(entity, loader, secondReader, termBytes);
        assertEquals("bar", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(2, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > cacheSize + value.length());
        assertEquals(2, cache.numRegisteredCloseListeners());

        secondEntity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        assertEquals("bar", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        entity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(2, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        // Closing the cache doesn't change returned entities
        reader.close();
        cache.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertEquals(cacheSize, requestCacheStats.stats().getMemorySize().bytesAsInt());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // release
        if (randomBoolean()) {
            secondReader.close();
        } else {
            entity.setIsOpen(false);
            cache.clear(secondEntity);
        }
        cache.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(secondReader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testEviction() throws Exception {
        final ByteSizeValue size;
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        {
            IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, getInstanceFromNode(IndicesService.class), dummyClusterSettings);
            IndexShard indexShard = getIndexShardCache();
            ShardRequestCache requestCacheStats = new ShardRequestCache();
            Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

            writer.addDocument(newDoc(0, "foo"));
            DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
            TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
            BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
            TestEntity entity = new TestEntity(requestCacheStats, indexShard);
            Loader loader = new Loader(reader, 0);

            writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
            DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
            TestEntity secondEntity = new TestEntity(requestCacheStats, indexShard);
            Loader secondLoader = new Loader(secondReader, 0);

            BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
            assertEquals("foo", value1.streamInput().readString());
            BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
            assertEquals("bar", value2.streamInput().readString());
            size = requestCacheStats.stats().getMemorySize();
            IOUtils.close(reader, secondReader, writer, dir, cache);
        }
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.builder().put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), size.getBytes() + 1 + "b").build(),
            getInstanceFromNode(IndicesService.class),
            dummyClusterSettings
        );
        IndexShard indexShard = getIndexShardCache();
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TestEntity secondEntity = new TestEntity(requestCacheStats, indexShard);
        Loader secondLoader = new Loader(secondReader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TestEntity thirdEntity = new TestEntity(requestCacheStats, indexShard);
        Loader thirdLoader = new Loader(thirdReader, 0);

        BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", requestCacheStats.stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirdEntity, thirdLoader, thirdReader, termBytes);
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(2, requestCacheStats.stats().getEntries());
        assertEquals(1, requestCacheStats.stats().getEvictions());
        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);
    }

    public void testClearAllEntityIdentity() throws Exception {
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, getInstanceFromNode(IndicesService.class), dummyClusterSettings);
        IndexShard indexShard = getIndexShardCache();
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TestEntity secondEntity = new TestEntity(requestCacheStats, indexShard);
        Loader secondLoader = new Loader(secondReader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        IndexShard differentIdentity = getIndexShardCache();
        TestEntity thirdEntity = new TestEntity(requestCacheStats, differentIdentity);
        Loader thirdLoader = new Loader(thirdReader, 0);

        BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", requestCacheStats.stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirdEntity, thirdLoader, thirdReader, termBytes);
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(3, cache.count());
        final long hitCount = requestCacheStats.stats().getHitCount();
        // clear all for the indexShard Identity even though it isn't still open
        cache.clear(randomFrom(entity, secondEntity));
        cache.cleanCache();
        assertEquals(1, cache.count());
        // third has not been validated since it's a different identity
        value3 = cache.getOrCompute(thirdEntity, thirdLoader, thirdReader, termBytes);
        assertEquals(hitCount + 1, requestCacheStats.stats().getHitCount());
        assertEquals("baz", value3.streamInput().readString());

        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);

    }

    public Iterable<Field> newDoc(int id, String value) {
        return Arrays.asList(
                newField("id", Integer.toString(id), StringField.TYPE_STORED),
                newField("value", value, StringField.TYPE_STORED)
        );
    }

    private static class Loader implements CheckedSupplier<BytesReference, IOException> {

        private final DirectoryReader reader;
        private final int id;
        public boolean loadedFromCache = true;

        Loader(DirectoryReader reader, int id) {
            super();
            this.reader = reader;
            this.id = id;
        }

        @Override
        public BytesReference get() {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs topDocs = searcher.search(new TermQuery(new Term("id", Integer.toString(id))), 1);
                assertEquals(1, topDocs.totalHits.value);
                Document document = reader.storedFields().document(topDocs.scoreDocs[0].doc);
                out.writeString(document.get("value"));
                loadedFromCache = false;
                return out.bytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void testInvalidate() throws Exception {
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.EMPTY,
            getInstanceFromNode(IndicesService.class),
            dummyClusterSettings
        );
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        IndexShard indexShard = getIndexShardCache();

        // initial cache
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // load again after invalidate
        entity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(reader, 0);
        cache.invalidate(entity, reader, termBytes);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // release
        if (randomBoolean()) {
            reader.close();
        } else {
            entity.setIsOpen(false); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testEqualsKey() throws IOException {
        IndexShard trueBoolean = getIndexShardCache();
        IndexShard falseBoolean = getIndexShardCache();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndicesRequestCache indicesRequestCache = indicesService.indicesRequestCache;
        Directory dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, config);
        ShardId shardId = new ShardId("foo", "bar", 1);
        IndexReader reader1 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey1 = ((OpenSearchDirectoryReader) reader1).getDelegatingCacheHelper().getDelegatingCacheKey().getId();
        writer.addDocument(new Document());
        IndexReader reader2 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey2 = ((OpenSearchDirectoryReader) reader2).getDelegatingCacheHelper().getDelegatingCacheKey().getId();
        IOUtils.close(reader1, reader2, writer, dir);
        IndicesRequestCache.Key key1 = indicesRequestCache.new Key(new TestEntity(null, trueBoolean), new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key2 = indicesRequestCache.new Key(new TestEntity(null, trueBoolean), new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key3 = indicesRequestCache.new Key(new TestEntity(null, falseBoolean), new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key4 = indicesRequestCache.new Key(new TestEntity(null, trueBoolean), new TestBytesReference(1), rKey2);
        IndicesRequestCache.Key key5 = indicesRequestCache.new Key(new TestEntity(null, trueBoolean), new TestBytesReference(2), rKey2);
        String s = "Some other random object";
        assertEquals(key1, key1);
        assertEquals(key1, key2);
        assertNotEquals(key1, null);
        assertNotEquals(key1, s);
        assertNotEquals(key1, key3);
        assertNotEquals(key1, key4);
        assertNotEquals(key1, key5);
    }

    public void testSerializationDeserializationOfCacheKey() throws Exception {
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        ShardRequestCache shardRequestCache = new ShardRequestCache();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndicesRequestCache indicesRequestCache = indicesService.indicesRequestCache;
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShard(0);
        IndicesService.IndexShardCacheEntity shardCacheEntity = indicesService.new IndexShardCacheEntity(indexShard);
        String readerCacheKeyId = UUID.randomUUID().toString();
        IndicesRequestCache.Key key1 = indicesRequestCache.new Key(shardCacheEntity, termBytes, readerCacheKeyId);
        BytesReference bytesReference = null;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            key1.writeTo(out);
            bytesReference = out.bytes();
        }
        StreamInput in = bytesReference.streamInput();

        IndicesRequestCache.Key key2 = indicesRequestCache.new Key(in);

        assertEquals(readerCacheKeyId, key2.readerCacheKeyId);
        assertEquals(shardCacheEntity.getCacheIdentity(), key2.entity.getCacheIdentity());
        assertEquals(termBytes, key2.value);

    }

    private static BytesReference getQSRBytesReference(long tookTimeNanos) throws IOException {
        // unfortunately no good way to separate this out from DiskTierTookTimePolicyTests.getQSR() :(
        ShardId shardId = new ShardId("index", "uuid", randomInt());
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(
            OriginalIndicesTests.randomOriginalIndices(),
            searchRequest,
            shardId,
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            randomNonNegativeLong(),
            null,
            new String[0]
        );
        ShardSearchContextId id = new ShardSearchContextId(UUIDs.base64UUID(), randomLong());
        QuerySearchResult result = new QuerySearchResult(
            id,
            new SearchShardTarget("node", shardId, null, OriginalIndices.NONE),
            shardSearchRequest
        );
        TopDocs topDocs = new TopDocs(new TotalHits(randomLongBetween(0, Long.MAX_VALUE), TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
        result.topDocs(new TopDocsAndMaxScore(topDocs, randomBoolean() ? Float.NaN : randomFloat()), new DocValueFormat[0]);

        result.setTookTimeNanos(tookTimeNanos);

        BytesStreamOutput out = new BytesStreamOutput();
        // it appears to need a boolean and then a ShardSearchContextId written to the stream before the QSR in order to deserialize?
        out.writeBoolean(false);
        id.writeTo(out);
        result.writeToNoId(out);
        return out.bytes();
    }

    private class TestBytesReference extends AbstractBytesReference {

        int dummyValue;

        TestBytesReference(int dummyValue) {
            this.dummyValue = dummyValue;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TestBytesReference && this.dummyValue == ((TestBytesReference) other).dummyValue;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + dummyValue;
            return result;
        }

        @Override
        public byte get(int index) {
            return 0;
        }

        @Override
        public int length() {
            return 0;
        }

        @Override
        public BytesReference slice(int from, int length) {
            return null;
        }

        @Override
        public BytesRef toBytesRef() {
            return null;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }

    private class TestEntity extends AbstractIndexShardCacheEntity {
        private final IndexShard standInForIndexShard;
        private final ShardRequestCache shardRequestCache;
        private boolean isOpen = true;

        private TestEntity(ShardRequestCache shardRequestCache, IndexShard standInForIndexShard) {
            this.standInForIndexShard = standInForIndexShard;
            this.shardRequestCache = shardRequestCache;
        }

        @Override
        protected ShardRequestCache stats() {
            return shardRequestCache;
        }

        @Override
        public boolean isOpen() {
            return this.isOpen;
        }

        public void setIsOpen(boolean isOpen) {
            this.isOpen = isOpen;
        }

        @Override
        public Object getCacheIdentity() {
            return standInForIndexShard;
        }

        @Override
        public long ramBytesUsed() {
            return 42;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }
    }

    public static class CleanDiskCacheTests {
        private IndicesRequestCache indicesRequestCache;
        private TieredCacheService<Key, BytesReference> tieredCacheService;
        private DiskCachingTier<Key, BytesReference> diskCachingTier;
        private IndicesService indicesService;

        @Before
        public void setup() {
            tieredCacheService = mock(TieredCacheService.class);
            diskCachingTier = mock(DiskCachingTier.class);
            indicesService = mock(IndicesService.class);
            indicesRequestCache = new IndicesRequestCache(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                indicesService,
                tieredCacheService
            );
        }

        @Test
        public void shouldNotCleanDiskCacheWhenEmpty() {
            final int DISK_CACHE_COUNT = 0;
            final double CLEANUP_THRESHOLD = 50.0;

            when(tieredCacheService.getDiskCachingTier()).thenReturn(
                    (Optional<DiskCachingTier<Key, BytesReference>>) Optional.of(diskCachingTier)
            );
            when(diskCachingTier.count()).thenReturn(DISK_CACHE_COUNT);

            indicesRequestCache.cleanDiskCache(CLEANUP_THRESHOLD);

            verify(diskCachingTier, never()).keys();
        }

        @Test
        public void shouldNotCleanDiskCacheWhenCleanupKeysPercentageIsBelowThreshold() {
            final int DISK_CACHE_COUNT = 1;
            final double CLEANUP_THRESHOLD = 49.0;

            when(tieredCacheService.getDiskCachingTier()).thenReturn(
                    (Optional<DiskCachingTier<Key, BytesReference>>) Optional.of(diskCachingTier)
            );
            when(diskCachingTier.count()).thenReturn(DISK_CACHE_COUNT);

            indicesRequestCache.cleanDiskCache(CLEANUP_THRESHOLD);

            verify(diskCachingTier, never()).keys();
        }

        @Test
        public void cleanDiskCacheWhenCleanupKeysPercentageIsGreaterThanOrEqualToThreshold() {
            final int DISK_CACHE_COUNT = 100;
            final double CLEANUP_THRESHOLD = 50.0;
            final int STALE_KEYS_IN_DISK_COUNT = 51;

            // Mock dependencies
            IndicesService mockIndicesService = mock(IndicesService.class);
            TieredCacheService<Key, BytesReference> mockTieredCacheService = mock(TieredCacheService.class);
            DiskCachingTier<Key, BytesReference> mockDiskCachingTier = mock(DiskCachingTier.class);
            Iterator<Key> mockIterator = mock(Iterator.class);
            Iterable<Key> mockIterable = () -> mockIterator;

            IndicesRequestCache cache = new IndicesRequestCache(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                mockIndicesService,
                mockTieredCacheService
            );

            // Set up mocks
            when(mockTieredCacheService.getDiskCachingTier()).thenReturn(Optional.of(mockDiskCachingTier));
            when(mockDiskCachingTier.count()).thenReturn(DISK_CACHE_COUNT);
            when(mockDiskCachingTier.keys()).thenReturn(mockIterable);
            when(mockIterator.hasNext()).thenReturn(true, true, false);

            // Create mock Keys and return them when next() is called
            CacheEntity mockEntity = mock(CacheEntity.class);
            Key firstMockKey = cache.createKeyForTesting(mockEntity, "readerCacheKeyId1");
            Key secondMockKey = cache.createKeyForTesting(mockEntity, "readerCacheKeyId2");
            when(mockEntity.getCacheIdentity()).thenReturn(new Object());
            when(mockIterator.next()).thenReturn(firstMockKey, secondMockKey);

            cache.addCleanupKeyForTesting(mockEntity, "readerCacheKeyId");
            cache.setStaleKeysInDiskCountForTesting(STALE_KEYS_IN_DISK_COUNT);
            cache.cleanDiskCache(CLEANUP_THRESHOLD);

            // Verify interactions
            verify(mockDiskCachingTier).keys();
            verify(mockIterator, times(2)).next();
        }

        @Test
        public void cleanDiskCacheAndCallInvalidateOfDiskTier() {
            final int DISK_CACHE_COUNT = 100;
            final double CLEANUP_THRESHOLD = 50.0;
            final int STALE_KEYS_IN_DISK_COUNT = 51;

            // Mock dependencies
            IndicesService mockIndicesService = mock(IndicesService.class);
            TieredCacheService<Key, BytesReference> mockTieredCacheService = mock(TieredCacheService.class);
            DiskCachingTier<Key, BytesReference> mockDiskCachingTier = mock(EhCacheDiskCachingTier.class);
            Iterator<Key> mockIterator = mock(Iterator.class);
            Iterable<Key> mockIterable = () -> mockIterator;

            IndicesRequestCache cache = new IndicesRequestCache(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                mockIndicesService,
                mockTieredCacheService
            );

            // Set up mocks
            when(mockTieredCacheService.getDiskCachingTier()).thenReturn(Optional.of(mockDiskCachingTier));
            when(mockDiskCachingTier.count()).thenReturn(DISK_CACHE_COUNT);
            when(mockDiskCachingTier.keys()).thenReturn(mockIterable);
            when(mockIterator.hasNext()).thenReturn(true, true, false);

            // Create mock Keys and return them when next() is called
            CacheEntity mockEntity = mock(CacheEntity.class);
            Key firstMockKey = cache.createKeyForTesting(mockEntity, "readerCacheKeyId1");
            Key secondMockKey = cache.createKeyForTesting(mockEntity, "readerCacheKeyId2");
            when(mockEntity.getCacheIdentity()).thenReturn(new Object());
            when(mockIterator.next()).thenReturn(firstMockKey, secondMockKey);

            cache.addCleanupKeyForTesting(mockEntity, "readerCacheKeyId");
            cache.setStaleKeysInDiskCountForTesting(STALE_KEYS_IN_DISK_COUNT);
            cache.cleanDiskCache(CLEANUP_THRESHOLD);

            // Verify interactions
            verify(mockDiskCachingTier).keys();
            verify(mockDiskCachingTier, times(1)).invalidate(any(IndicesRequestCache.Key.class));
            verify(mockIterator, times(2)).next();
        }

        @Test
        public void diskCleanupKeysPercentageWhenDiskCacheIsEmpty() {
            when(tieredCacheService.getDiskCachingTier()).thenReturn(Optional.of(diskCachingTier));
            when(diskCachingTier.count()).thenReturn(0);

            double result = indicesRequestCache.diskCleanupKeysPercentage();

            assertEquals(0, result, 0);
        }

        @Test
        public void diskCleanupKeysPercentageWhenKeysToCleanIsEmpty() {
            IndicesService mockIndicesService = mock(IndicesService.class);
            TieredCacheService<Key, BytesReference> mockTieredCacheService = mock(TieredCacheSpilloverStrategyService.class);
            DiskCachingTier<Key, BytesReference> mockDiskCachingTier = mock(DiskCachingTier.class);
            when(mockTieredCacheService.getDiskCachingTier()).thenReturn(Optional.of(mockDiskCachingTier));
            when(mockDiskCachingTier.count()).thenReturn(100);
            IndicesRequestCache cache = new IndicesRequestCache(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                mockIndicesService,
                mockTieredCacheService
            );

            double result = cache.diskCleanupKeysPercentage();

            assertEquals(0, result, 0.001);
        }

        @Test
        public void diskCleanupKeysPercentageWhenDiskCacheAndKeysToCleanAreNotEmpty() {
            IndicesService mockIndicesService = mock(IndicesService.class);
            TieredCacheService<Key, BytesReference> mockTieredCacheService = mock(TieredCacheSpilloverStrategyService.class);
            DiskCachingTier<Key, BytesReference> mockDiskCachingTier = mock(DiskCachingTier.class);
            when(mockTieredCacheService.getDiskCachingTier()).thenReturn(Optional.of(mockDiskCachingTier));
            when(mockDiskCachingTier.count()).thenReturn(100);

            IndicesRequestCache cache = new IndicesRequestCache(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                mockIndicesService,
                mockTieredCacheService
            );

            IndicesRequestCache.CacheEntity mockEntity = Mockito.mock(IndicesRequestCache.CacheEntity.class);
            when(mockEntity.getCacheIdentity()).thenReturn(new Object());
            cache.addCleanupKeyForTesting(mockEntity, "readerCacheKeyId");
            cache.setStaleKeysInDiskCountForTesting(1);

            double result = cache.diskCleanupKeysPercentage();
            assertEquals(1.0, result, 0);
        }
    }
}
