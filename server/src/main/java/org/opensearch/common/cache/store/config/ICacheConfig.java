/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.config;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.settings.Settings;

/**
 * Common configurations related to store aware caches.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ICacheConfig<K, V> {

    //private StoreAwareCacheEventListener<K, V> eventListener;
    private RemovalListener<ICacheKey<K>, V> removalListener;

    private Settings settings;

    private Class<K> keyType;

    private Class<V> valueType;

    private ICacheConfig(Builder<K, V> builder) {
        this.keyType = builder.keyType;
        this.valueType = builder.valueType;
        this.settings = builder.settings;
        this.removalListener = builder.removalListener;
    }

    public RemovalListener<ICacheKey<K>, V> getRemovalListener() {
        return removalListener;
    }

    public Class<K> getKeyType() {
        return keyType;
    }

    public Class<V> getValueType() {
        return valueType;
    }

    public Settings getSettings() {
        return settings;
    }

    /**
     * Builder class to build Cache config related parameters.
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    public static class Builder<K, V> {

        private RemovalListener<ICacheKey<K>, V> removalListener;

        private Settings settings;

        private Class<K> keyType;

        private Class<V> valueType;

        public Builder() {}

        public Builder<K, V> setRemovalListener(RemovalListener<ICacheKey<K>, V> listener) {
            this.removalListener = listener;
            return this;
        }

        public Builder<K, V> setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder<K, V> setKeyType(Class<K> keyType) {
            this.keyType = keyType;
            return this;
        }

        public Builder<K, V> setValueType(Class<V> valueType) {
            this.valueType = valueType;
            return this;
        }

        public ICacheConfig<K, V> build() {
            return new ICacheConfig<>(this);
        }
    }
}
