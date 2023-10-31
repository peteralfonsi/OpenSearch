/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import java.io.EOFException;
import java.io.IOException;

/**
 * An interface for serializers, to be used in disk caching tier and elsewhere.
 * @param <T>
 */
public interface Serializer<T, U> {
    /**
     * Serializes an object.
     * @param object
     * @return The serialized representation of the object.
     */
    U serialize(T object) throws IOException;

    /**
     * Deserializes bytes into an object.
     * @param bytes
     * @return
     */
    T deserialize(U bytes) throws IOException;

    /**
     * Compares an object to a serialized representation of an object.
     * @param object
     * @param bytes
     * @return true if representing the same object, false if not
     */
    boolean equals(T object, U bytes) throws IOException;
}
