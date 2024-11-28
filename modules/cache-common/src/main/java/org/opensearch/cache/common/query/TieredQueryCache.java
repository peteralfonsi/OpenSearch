/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;
import org.opensearch.cache.common.tier.TieredSpilloverCache;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.policy.CachedQueryResult;
import org.opensearch.common.cache.serializer.BytesReferenceSerializer;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.cache.service.CacheService;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.indices.IRCKeyWriteableSerializer;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.indices.OpenSearchQueryCache;
import org.opensearch.search.aggregations.bucket.composite.CompositeKey;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.function.ToLongBiFunction;


// TODO: This is a proof of concept only! This is not an elegant or clean (or even really functional) implementation.

/*
    General outline:
    Have a class mostly duplicating Lucene LRUQueryCache that overrides its onQueryCache, onQueryEviction, onDocIdSetCache, onDocIdSetEviction, onHit, onMiss
    methods. This acts as the heap tier, allowing us to a) capture stats and b) funnel evicted items into the disk tier.

    We can't directly reuse LRUQC as we need to wrap incoming weights in our own weight implementation, not LRUQC.CachingWrapperWeight.
    The existing one explicitly calls out to LRUQC's get() when attempting to retrieve from cache. But we need it to call out to the tiered get()
    otherwise it won't check the disk cache.

    Ideally LRUQC.get() would be protected but it's private. So we have to duplicate it.

    Items evicted from the heap tier must be checked for serializability. If allowed, they are serialized and enter an ehcache disk tier.
    (This is handled (in the future) by a policy attached to the TSC).

    The actual "cache" part in LRUQC has two steps: Map<IndexReader.CacheKey, LeafCache> is the outer one.
    I think CacheKey is 1:1 with leaves in an index.
    Then LeafCache contains a Map<Query, CacheAndCount> which has the final info we use to construct the doc id set.
    So an important question... do we shove whole LeafCache objects into ehcache, or can a LeafCache have some items in memory, and some in ehcache?

    I think the second way makes more sense for two reasons:
    1) queries are evicted one at a time, not as whole LeafCache objects, and these would logically go into the disk one at a time
    2) not all queries can be serialized, so a LeafCache could have some in there that can enter disk, and some that can't.

    Looks like LRUQC controls the eviction logic, and just tells LeafCache to remove() something when needed.

    ... What if we made each instance of LeafCache be a TSC, instead of basically just an in-memory map?
    This doesn't quite make sense, but maybe they could all use a shared TSC. And the key could be some composite of IndexReader.CacheKey and Query?
    Would this be compatible with the logic where disk tier rejects something if it's not serializable?
    If we did this, we could dispense with LeafCache entirely, and just have one level of cache.
    Unless serializing IndexReader.CacheKey is difficult. Then we keep two levels, and assign each LeafCache a
    UUID on creation, which then becomes the first part of the composite key. But they'd still use a shared TSC.
    In this case, the outer map always lives in memory. But the final stuff, the Map<Query, CacheAndCount>, is a TSC.

    We can listen for the TSC's evictions (??) to handle stats (????)
    (The TSC stats are nice because they track by tier. But, we are missing some info there,
    bc if the leafcache doesn't exist, the query never reaches the TSC at all, and it doesn't know about the miss.)
    Might have to enable TSC stats and combine them yuck! But for now dont worry too much about that.
    Besides, every stat except misses should be reportable from the TSC.

    This should all move into the cache-common module.

    I think for the PoC only keys that are serializable can even live in the heap tier. Maybe can tweak this later with changes to TSC?
    Maybe even just adding a new disk tier policy to TSC would suffice. I think it would.


    Assumption: There's no "staleness" to handle in any way beyond however LRUQC decides to evict/remove items.

    Is the deal with the singletons *purely* about LRU optimization,
    or is it actually the case there's lots of duplicated queries in different leaves? If the latter, we may want to use it.
    Check on this Monday. I dont really understand why - as the comments say - there would be multiple copies.
     */
public class TieredQueryCache implements QueryCache, OpenSearchQueryCache {

    private final ICache<CompositeKey, CacheAndCount> innerCache; // This should typically be a TieredSpilloverCache but theoretically can be anything - for testing purposes
    private final Map<IndexReader.CacheKey, LeafCache> outerCache;

    //private final LongAdder hitCount;
    private final LongAdder outerCacheMissCount; // TODO: this should prob be some map from shard (or w/e TSC dimension is) to LongAdder

    private AtomicInteger nextLeafCacheId; // Increment this and feed to each LeafCache on creation as unique id.

    private final CompositeKeySerializer keySerializer;
    private final long maxRamBytesUsed;
    private final float skipCacheFactor; // Idk what this is - looks simple tho
    private final Predicate<LeafReaderContext> leavesToCache;

    private final RemovalListener<ICacheKey<CompositeKey>, CacheAndCount> removalListener;

    // Is there any need for locks? The underlying TSC is threadsafe. I think the need for locks in original was due to LeafCache impl.


    public TieredQueryCache(long heapSizeBytes,
                            long diskSizeBytes,
                            long maxRamBytesUsed,
                            Predicate<LeafReaderContext> leavesToCache,
                            float skipCacheFactor,
                            CacheService cacheService,
                            Settings settings,
                            ClusterService clusterService,
                            NodeEnvironment nodeEnvironment
                            ) {
        this.maxRamBytesUsed = maxRamBytesUsed;
        this.leavesToCache = leavesToCache;
        if (skipCacheFactor >= 1 == false) { // NaN >= 1 evaluates false
            throw new IllegalArgumentException(
                "skipCacheFactor must be no less than 1, get " + skipCacheFactor);
        }
        this.skipCacheFactor = skipCacheFactor;

        this.keySerializer = new CompositeKeySerializer(new QuerySerializer());
        this.outerCacheMissCount = new LongAdder();

        this.outerCache = new ConcurrentHashMap<>(); // Does the concurrent-ness have a perf impact? Not sure.

        ToLongBiFunction<ICacheKey<CompositeKey>, CacheAndCount> weigher = (k, v) -> k.ramBytesUsed(k.key.ramBytesUsed()) + v.ramBytesUsed();
        this.removalListener = new TSCRemovalListener();
        this.innerCache = cacheService.createCache(
            new CacheConfig.Builder<CompositeKey, CacheAndCount>().setSettings(settings)
                .setWeigher(weigher)
                .setValueType(CacheAndCount.class)
                .setKeyType(CompositeKey.class)
                .setRemovalListener(removalListener)
                //.setDimensionNames(List.of(INDEX_DIMENSION_NAME, SHARD_ID_DIMENSION_NAME)) // TODO
                /*.setCachedResultParser((bytesReference) -> {
                    try {
                        return CachedQueryResult.getPolicyValues(bytesReference);
                    } catch (IOException e) {
                        // Set took time to -1, which will always be rejected by the policy.
                        return new CachedQueryResult.PolicyValues(-1);
                    }
                })*/ // TODO - i forgor that this is unfortunately hardcoded to always return CachedQueryResult.
                // But for the proof of concept we can hack around that by just adding what we need? Or do we actually need anything?
                .setKeySerializer(keySerializer)
                .setValueSerializer(new CacheAndCountSerializer())
                .setClusterSettings(clusterService.getClusterSettings())
                .setStoragePath(nodeEnvironment.nodePaths()[0].path.toString() + "/query_cache")
                .build(),
            CacheType.QUERY_CACHE
        );
        // In theory it may be useful to test this with non-TSC internal caches for regression purposes.
        // For example, if we just do an OpenSearchOnHeapCache of the same size, how much worse does it perform?


    }

    protected CacheAndCount get(Query query, IndexReader.CacheHelper cacheHelper) {
        // TODO: Mostly same as LRUQC.get(), but skipping the singleton map stuff - dont think this is necessary here? But could be wrong
        assert query instanceof BoostQuery == false;
        assert query instanceof ConstantScoreQuery == false;

        final IndexReader.CacheKey readerKey = cacheHelper.getKey();
        final LeafCache leafCache = outerCache.get(readerKey);

        if (leafCache == null) {
            onMiss(readerKey, query);
            return null;
        }
        // Singleton stuff would go here if I decide it's needed
        final CacheAndCount cached = leafCache.get(query);
        if (cached == null) {
            onMiss(readerKey, query);
        } else {
            onHit(readerKey, query); // TODO: Currently does nothing.
        }
        return cached;
    }

    // TODO: Duplicated these from LRUQC for now. Skipping other callbacks as TSC can mostly handle stats.
    protected void onHit(Object readerCoreKey, Query query) {
        //hitCount.add(1);
        // TODO: dont think this is needed as TSC can track hits.
    }
    protected void onMiss(Object readerCoreKey, Query query) {
        assert query != null;
        outerCacheMissCount.add(1);
        // TODO: this only tracks the misses due to nonexistent leaf cache, as TSC can track others.
    }

    // TODO: Dont forget - the LRUQC has some logic about what even should enter the cache. Make sure to duplicate this.

    @Override
    public QueryCacheStats getStats(ShardId shard) {
        return null;
    }

    @Override
    public void clearIndex(String index) {

    }

    @Override
    public void onClose(ShardId shardId) {

    }

    @Override
    public void close() throws IOException {
        innerCache.close();
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        while (weight instanceof TieredCachingWrapperWeight) {
            weight = ((TieredCachingWrapperWeight) weight).in;
        }

        return new TieredCachingWrapperWeight(weight, policy);
        // TODO: This is just the LRUQC wrapper. Do we want to do a similar wrapper as IQC for stats?
    }

    private void putIfAbsent(Query query, CacheAndCount cached, IndexReader.CacheHelper cacheHelper) {
        // TODO: This is a lot simpler than the LRUQC one - for now I'm not fully handling stats, and I dont have to deal with the singleton LRU stuff
        final IndexReader.CacheKey key = cacheHelper.getKey();
        LeafCache leafCache = outerCache.get(key);
        if (leafCache == null) {
            int nextId = nextLeafCacheId.incrementAndGet();
            leafCache = new LeafCache(nextId, innerCache);
            final LeafCache previous = outerCache.put(key, leafCache);
            assert previous == null;
        }
        leafCache.putIfAbsent(query, cached);
        // We also dont handle eviction; the TSC does.
    }

    /**
     * Duplicated from LRUQC. I think this is a bad and confusing name, but let's keep it for consistency.
     * Default cache implementation: uses {@link RoaringDocIdSet} for sets that have a density &lt; 1%
     * and a {@link BitDocIdSet} over a {@link FixedBitSet} otherwise.
     */
    protected CacheAndCount cacheImpl(BulkScorer scorer, int maxDoc) throws IOException {
        if (scorer.cost() * 100 >= maxDoc) {
            // FixedBitSet is faster for dense sets and will enable the random-access
            // optimization in ConjunctionDISI
            return cacheIntoBitSet(scorer, maxDoc);
        } else {
            return cacheIntoRoaringDocIdSet(scorer, maxDoc);
        }
    }

    private static CacheAndCount cacheIntoBitSet(BulkScorer scorer, int maxDoc) throws IOException {
        final FixedBitSet bitSet = new FixedBitSet(maxDoc);
        int[] count = new int[1];
        scorer.score(
            new LeafCollector() {

                @Override
                public void setScorer(Scorable scorer) throws IOException {}

                @Override
                public void collect(int doc) throws IOException {
                    count[0]++;
                    bitSet.set(doc);
                }
            },
            null);
        return new CacheAndCount(new BitDocIdSet(bitSet, count[0]), count[0]);
    }

    private static CacheAndCount cacheIntoRoaringDocIdSet(BulkScorer scorer, int maxDoc)
        throws IOException {
        RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
        scorer.score(
            new LeafCollector() {

                @Override
                public void setScorer(Scorable scorer) throws IOException {}

                @Override
                public void collect(int doc) throws IOException {
                    builder.add(doc);
                }
            },
            null);
        RoaringDocIdSet cache = builder.build();
        return new CacheAndCount(cache, cache.cardinality());
    }

    Predicate<CompositeKey> createTSCPolicy(CompositeKeySerializer serializer) {
        return serializer::isAllowed;
    }

    private class TieredCachingWrapperWeight extends ConstantScoreWeight {
        // TODO - based on LRUQC CachingWrapperWeight, but it uses *this* class's get() to get the actual value from cache when needed
        private final Weight in;
        private final QueryCachingPolicy policy;
        // we use an AtomicBoolean because Weight.scorer may be called from multiple
        // threads when IndexSearcher is created with threads
        private final AtomicBoolean used;

        TieredCachingWrapperWeight(Weight in, QueryCachingPolicy policy) {
            super(in.getQuery(), 1f);
            this.in = in;
            this.policy = policy;
            used = new AtomicBoolean(false);
        }

        // TODO: all other logic, which can be mostly duplicated from LRUQC.

        // This logic needs us to implement following in TQC:
        // maxRamBytesUsed, cacheImpl, leavesToCache, skipCacheFactor, putIfAbsent, and possibly readLock tho that might not be needed.

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            return in.matches(context, doc);
        }

        private boolean cacheEntryHasReasonableWorstCaseSize(int maxDoc) {
            // The worst-case (dense) is a bit set which needs one bit per document
            final long worstCaseRamUsage = maxDoc / 8;
            final long totalRamAvailable = maxRamBytesUsed;
            // Imagine the worst-case that a cache entry is large than the size of
            // the cache: not only will this entry be trashed immediately but it
            // will also evict all current entries from the cache. For this reason
            // we only cache on an IndexReader if we have available room for
            // 5 different filters on this reader to avoid excessive trashing
            return worstCaseRamUsage * 5 < totalRamAvailable;
        }

        private CacheAndCount cache(LeafReaderContext context) throws IOException {
            final BulkScorer scorer = in.bulkScorer(context);
            if (scorer == null) {
                return CacheAndCount.EMPTY;
            } else {
                return cacheImpl(scorer, context.reader().maxDoc());
            }
        }

        /** Check whether this segment is eligible for caching, regardless of the query. */
        private boolean shouldCache(LeafReaderContext context) throws IOException {
            return cacheEntryHasReasonableWorstCaseSize(
                ReaderUtil.getTopLevelContext(context).reader().maxDoc())
                && leavesToCache.test(context);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (in.isCacheable(context) == false) {
                // this segment is not suitable for caching
                return in.scorerSupplier(context);
            }

            // Short-circuit: Check whether this segment is eligible for caching
            // before we take a lock because of #get
            if (shouldCache(context) == false) {
                return in.scorerSupplier(context);
            }

            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                // this reader has no cache helper
                return in.scorerSupplier(context);
            }

            // If the lock is already busy, prefer using the uncached version than waiting
            /*if (readLock.tryLock() == false) {
                return in.scorerSupplier(context);
            }*/

            CacheAndCount cached = get(in.getQuery(), cacheHelper);
            /*
            CacheAndCount cached;
            try {
                cached = get(in.getQuery(), cacheHelper);
            } finally {
                readLock.unlock();
            }*/

            if (cached == null) {
                if (policy.shouldCache(in.getQuery())) {
                    final ScorerSupplier supplier = in.scorerSupplier(context);
                    if (supplier == null) {
                        putIfAbsent(in.getQuery(), CacheAndCount.EMPTY, cacheHelper);
                        return null;
                    }

                    final long cost = supplier.cost();
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            // skip cache operation which would slow query down too much
                            if (cost / skipCacheFactor > leadCost) {
                                return supplier.get(leadCost);
                            }

                            Scorer scorer = supplier.get(Long.MAX_VALUE);
                            CacheAndCount cached =
                                cacheImpl(new DefaultBulkScorer(scorer), context.reader().maxDoc());
                            putIfAbsent(in.getQuery(), cached, cacheHelper);
                            DocIdSetIterator disi = cached.iterator();
                            if (disi == null) {
                                // docIdSet.iterator() is allowed to return null when empty but we want a non-null
                                // iterator here
                                disi = DocIdSetIterator.empty();
                            }

                            return new ConstantScoreScorer(
                                TieredCachingWrapperWeight.this, 0f, ScoreMode.COMPLETE_NO_SCORES, disi);
                        }

                        @Override
                        public long cost() {
                            return cost;
                        }
                    };
                } else {
                    return in.scorerSupplier(context);
                }
            }

            assert cached != null;
            if (cached == CacheAndCount.EMPTY) {
                return null;
            }
            final DocIdSetIterator disi = cached.iterator();
            if (disi == null) {
                return null;
            }

            return new ScorerSupplier() {
                @Override
                public Scorer get(long LeadCost) throws IOException {
                    return new ConstantScoreScorer(
                        TieredCachingWrapperWeight.this, 0f, ScoreMode.COMPLETE_NO_SCORES, disi);
                }

                @Override
                public long cost() {
                    return disi.cost();
                }
            };
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            ScorerSupplier scorerSupplier = scorerSupplier(context);
            if (scorerSupplier == null) {
                return null;
            }
            return scorerSupplier.get(Long.MAX_VALUE);
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            // Our cache won't have an accurate count if there are deletions
            if (context.reader().hasDeletions()) {
                return in.count(context);
            }

            // Otherwise check if the count is in the cache
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (in.isCacheable(context) == false) {
                // this segment is not suitable for caching
                return in.count(context);
            }

            // Short-circuit: Check whether this segment is eligible for caching
            // before we take a lock because of #get
            if (shouldCache(context) == false) {
                return in.count(context);
            }

            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                // this reader has no cacheHelper
                return in.count(context);
            }

            // If the lock is already busy, prefer using the uncached version than waiting
            /*if (readLock.tryLock() == false) {
                return in.count(context);
            }*/

            CacheAndCount cached = get(in.getQuery(), cacheHelper);
            /*CacheAndCount cached;
            try {
                cached = get(in.getQuery(), cacheHelper);
            } finally {
                readLock.unlock();
            }*/
            if (cached != null) {
                // cached
                return cached.count();
            }
            // Not cached, check if the wrapped weight can count quickly then use that
            return in.count(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return in.isCacheable(ctx);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (in.isCacheable(context) == false) {
                // this segment is not suitable for caching
                return in.bulkScorer(context);
            }

            // Short-circuit: Check whether this segment is eligible for caching
            // before we take a lock because of #get
            if (shouldCache(context) == false) {
                return in.bulkScorer(context);
            }

            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                // this reader has no cacheHelper
                return in.bulkScorer(context);
            }

            // If the lock is already busy, prefer using the uncached version than waiting
            /*if (readLock.tryLock() == false) {
                return in.bulkScorer(context);
            }*/

            CacheAndCount cached = get(in.getQuery(), cacheHelper);
            /*CacheAndCount cached;
            try {
                cached = get(in.getQuery(), cacheHelper);
            } finally {
                readLock.unlock();
            }*/

            if (cached == null) {
                if (policy.shouldCache(in.getQuery())) {
                    cached = cache(context);
                    putIfAbsent(in.getQuery(), cached, cacheHelper);
                } else {
                    return in.bulkScorer(context);
                }
            }

            assert cached != null;
            if (cached == CacheAndCount.EMPTY) {
                return null;
            }
            final DocIdSetIterator disi = cached.iterator();
            if (disi == null) {
                return null;
            }

            return new DefaultBulkScorer(
                new ConstantScoreScorer(this, 0f, ScoreMode.COMPLETE_NO_SCORES, disi));
        }
        }

    class CompositeKey implements Accountable {
        final int leafCacheid;
        final Query query;

        CompositeKey(int leafCacheid, Query query) {
            this.leafCacheid = leafCacheid;
            this.query = query;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompositeKey key = (CompositeKey) o;
            if (leafCacheid != key.leafCacheid) return false;
            if (!query.equals(key.query)) return false;  // TODO: Is it a valid assumption that queries correctly implement equals()?
            return true;
        }

        @Override
        public long ramBytesUsed() {
            return 0; // TODO: It looks like in LRUQC, LeafCache only counts DocIdSet towards its total usage.
        }
    }

    class LeafCache {
        // TODO - Basically a wrapper on top of the TSC, but it applies its unique ID to create the composite key
        final int id;
        final ICache<CompositeKey, CacheAndCount> actualCache;

        LeafCache(int id, ICache<CompositeKey, CacheAndCount> actualCache) {
            this.id = id;
            this.actualCache = actualCache;
        }

        CacheAndCount get(Query query) {
            return actualCache.get(getFinalKey(query)); // TODO: should this be computeIfAbsent?
        }

        void putIfAbsent(Query query, CacheAndCount cached) {
            actualCache.put(getFinalKey(query), cached);
        }

        ICacheKey<CompositeKey> getFinalKey(Query query) {
            CompositeKey key = new CompositeKey(id, query);
            return new ICacheKey<>(key, null); // TODO: dimensions?? Will the TSC even track stats - dont think so for now?
            // I think the TSC dimension should ultimately be from the shard. TBD if feasible.
        }
    }

    // Duplicated from LRUQC with no changes
    protected static class CacheAndCount implements Accountable {
        protected static final CacheAndCount EMPTY = new CacheAndCount(DocIdSet.EMPTY, 0);

        private static final long BASE_RAM_BYTES_USED =
            RamUsageEstimator.shallowSizeOfInstance(CacheAndCount.class);
        private final DocIdSet cache;
        private final int count;

        public CacheAndCount(DocIdSet cache, int count) {
            this.cache = cache;
            this.count = count;
        }

        public DocIdSetIterator iterator() throws IOException {
            return cache.iterator();
        }

        public int count() {
            return count;
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + cache.ramBytesUsed();
        }
    }

    class TSCRemovalListener implements RemovalListener<ICacheKey<CompositeKey>, CacheAndCount> {
        @Override
        public void onRemoval(RemovalNotification<ICacheKey<CompositeKey>, CacheAndCount> notification) {
            // TODO
        }
    }

    // Various serializers below

    class CompositeKeySerializer implements Serializer<CompositeKey, byte[]> {
        final QuerySerializer serializer;

        CompositeKeySerializer(QuerySerializer serializer) {
            this.serializer = serializer;
        }

        // TODO below
        @Override
        public byte[] serialize(CompositeKey object) {
            return new byte[0];
        }

        @Override
        public CompositeKey deserialize(byte[] bytes) {
            return null;
        }

        @Override
        public boolean equals(CompositeKey object, byte[] bytes) {
            return false;
        }

        public boolean isAllowed(CompositeKey key) {
            // TODO - report yes for serializable, no for not serializable. Feed this into a policy into the TSC to control disk tier access.
            return serializer.isAllowed(key.query);
        }
    }

    class QuerySerializer implements Serializer<Query, byte[]> {
        // TODO - will have to basically do one query type at a time. Not TermQuery ever lol.

        @Override
        public byte[] serialize(Query object) {
            return new byte[0];
        }

        @Override
        public Query deserialize(byte[] bytes) {
            return null;
        }

        @Override
        public boolean equals(Query object, byte[] bytes) {
            return false;
        }

        public boolean isAllowed(Query query) {
            // TODO - report yes for serializable, no for not serializable. Feed this into a policy into the TSC to control disk tier access.
            return true;
        }
    }

    class CacheAndCountSerializer implements Serializer<CacheAndCount, byte[]> {
        // TODO - this object looks p serializable, but not 100% sure
        // Theres like ... 8 different impls of DocIdSet. But I think we only use 2 possible ones in the cache. Hopefully can just do that.
        // BitDocIdSet and RoaringDocIdSet
        // Could possibly abuse the bits() interface DocIdSet exposes for this.
        // Nope RoaringDocIdSet doesn't implement this. Maybe the iterator then.

        @Override
        public byte[] serialize(CacheAndCount object) {
            return null;
        }

        @Override
        public CacheAndCount deserialize(byte[] bytes) {
            return null;
        }

        @Override
        public boolean equals(CacheAndCount object, byte[] bytes) {
            return false;
        }
    }
}

