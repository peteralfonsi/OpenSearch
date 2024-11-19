/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import org.opensearch.cache.store.disk.EhcacheDiskCache;
import org.opensearch.cache.store.disk.EhcacheTieredCache;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.settings.Setting;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.settings.Setting.Property.NodeScope;

/**
 * Settings related to EhcacheTieredCache.
 */
public class EhcacheTieredCacheSettings {
    // TODO: Since I can't override a static suffix for the settings, for now it's just gonna use ehcache_disk settings where appropriate.
    // This can be fixed later on if needed.

    /**
     * Default cache size in bytes ie 1gb.
     */
    public static final long DEFAULT_HEAP_CACHE_SIZE_IN_BYTES = 10_485_760L; // 10 MB

    /**
     * Heap cache max size setting.
     */
    public static final Setting.AffixSetting<Long> HEAP_CACHE_MAX_SIZE_IN_BYTES_SETTING = Setting.suffixKeySetting(
        EhcacheTieredCache.EhcacheTieredCacheFactory.EHCACHE_TIERED_CACHE_NAME + ".heap_max_size_in_bytes",
        (key) -> Setting.longSetting(key, DEFAULT_HEAP_CACHE_SIZE_IN_BYTES, NodeScope)
    );

    /**
     * Key for max size.
     */
    public static final String HEAP_CACHE_MAX_SIZE_IN_BYTES_KEY = "heap_max_size_in_bytes";

    private static final Map<String, Setting.AffixSetting<?>> KEY_SETTING_MAP = Map.of(
        HEAP_CACHE_MAX_SIZE_IN_BYTES_KEY,
        HEAP_CACHE_MAX_SIZE_IN_BYTES_SETTING
    );

    /**
     * Map to store desired settings for a cache type.
     */
    public static final Map<CacheType, Map<String, Setting<?>>> CACHE_TYPE_MAP = getCacheTypeMap();

    /**
     * Used to form concrete setting for cache types and return desired map
     * @return map of cacheType and associated settings.
     * // TODO: Duplicated from other settings, but currently not extending those...
     */
    protected static Map<CacheType, Map<String, Setting<?>>> getCacheTypeMap() {
        Map<CacheType, Map<String, Setting<?>>> cacheTypeMap = new HashMap<>();
        for (CacheType cacheType : CacheType.values()) {
            Map<String, Setting<?>> settingMap = new HashMap<>();
            for (Map.Entry<String, Setting.AffixSetting<?>> entry : KEY_SETTING_MAP.entrySet()) {
                settingMap.put(entry.getKey(), entry.getValue().getConcreteSettingForNamespace(cacheType.getSettingPrefix()));
            }
            cacheTypeMap.put(cacheType, settingMap);
        }
        return cacheTypeMap;
    }

    /**
     * Fetches setting list for a combination of cache type and store name.
     * @param cacheType cache type
     * @return settings
     * // tODO: cant override this bc static. So not extending other settings for now.
     */
    public static final Map<String, Setting<?>> getSettingListForCacheType(CacheType cacheType) {
        Map<String, Setting<?>> cacheTypeSettings = CACHE_TYPE_MAP.get(cacheType);
        if (cacheTypeSettings == null) {
            throw new IllegalArgumentException(
                "No settings exist for cache store name: "
                    + EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME
                    + "associated with "
                    + "cache type: "
                    + cacheType
            );
        }
        return cacheTypeSettings;
    }

    /**
     * Default constructor. Added to fix javadocs.
     */
    public EhcacheTieredCacheSettings() {}

}
