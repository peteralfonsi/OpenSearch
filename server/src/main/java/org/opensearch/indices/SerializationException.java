/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.OpenSearchException;

public class SerializationException extends OpenSearchException {
    public SerializationException(String message) {
        super(message);
    }
}
