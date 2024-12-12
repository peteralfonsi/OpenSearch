/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import org.opensearch.cache.store.CaffeineHeapCache;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.cache.CaffeineHeapCacheSettings.CACHE_TYPE_MAP;

/**
 * Caffeine based cache plugin.
 */
public class CaffeineCachePlugin extends Plugin implements CachePlugin {

    private static final String CAFFEINE_CACHE_PLUGIN = "CaffeinePlugin";

    /**
     * Default constructor to avoid javadoc related failures.
     */
    public CaffeineCachePlugin() {}

    @Override
    public Map<String, ICache.Factory> getCacheFactoryMap() {
        return Map.of(CaffeineHeapCache.CaffeineHeapCacheFactory.NAME, new CaffeineHeapCache.CaffeineHeapCacheFactory());
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        for (Map.Entry<CacheType, Map<String, Setting<?>>> entry : CACHE_TYPE_MAP.entrySet()) {
            for (Map.Entry<String, Setting<?>> entry1 : entry.getValue().entrySet()) {
                settingList.add(entry1.getValue());
            }
        }
        return settingList;
    }

    @Override
    public String getName() {
        return CAFFEINE_CACHE_PLUGIN;
    }
}
