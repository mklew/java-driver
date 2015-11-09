/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.nio.ByteBuffer;

import com.datastax.driver.core.exceptions.InvalidTypeException;

import static com.datastax.driver.core.CodecUtils.compose;

/**
 * A simple {@code RegularStatement} implementation built directly from a query
 * string.
 * <p>
 * Use {@link Session#newSimpleStatement(String)} to create a new instance of this
 * class.
 */
public class SimpleStatement extends RegularStatement {

    private final String query;

    private volatile ByteBuffer routingKey;
    private volatile String keyspace;
    
    protected SimpleStatement(String query, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        super(protocolVersion, codecRegistry);
        this.query = query;
    }

    protected SimpleStatement(String query, ProtocolVersion protocolVersion, CodecRegistry codecRegistry, Object... values) {
        super(protocolVersion, codecRegistry);
        this.query = query;
        bind(values);
    }

    @Override
    public String getQueryString() {
        return query;
    }

    /**
     * Returns the routing key for the query.
     * <p>
     * Unless the routing key has been explicitly set through
     * {@link #setRoutingKey}, this method will return {@code null} to
     * avoid having to parse the query string to retrieve the partition key.
     *
     * @return the routing key set through {@link #setRoutingKey} if such a key
     * was set, {@code null} otherwise.
     *
     * @see Statement#getRoutingKey
     */
    @Override
    public ByteBuffer getRoutingKey() {
        return routingKey;
    }

    /**
     * Sets the routing key for this query.
     * <p>
     * This method allows you to manually provide a routing key for this query. It
     * is thus optional since the routing key is only an hint for token aware
     * load balancing policy but is never mandatory.
     * <p>
     * If the partition key for the query is composite, use the
     * {@link #setRoutingKey(ByteBuffer...)} method instead to build the
     * routing key.
     *
     * @param routingKey the raw (binary) value to use as routing key.
     * @return this {@code SimpleStatement} object.
     *
     * @see Statement#getRoutingKey
     */
    public SimpleStatement setRoutingKey(ByteBuffer routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    /**
     * Returns the keyspace this query operates on.
     * <p>
     * Unless the keyspace has been explicitly set through {@link #setKeyspace},
     * this method will return {@code null} to avoid having to parse the query
     * string.
     *
     * @return the keyspace set through {@link #setKeyspace} if such keyspace was
     * set, {@code null} otherwise.
     *
     * @see Statement#getKeyspace
     */
    @Override
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Sets the keyspace this query operates on.
     * <p>
     * This method allows you to manually provide a keyspace for this query. It
     * is thus optional since the value returned by this method is only an hint
     * for token aware load balancing policy but is never mandatory.
     * <p>
     * Do note that if the query does not use a fully qualified keyspace, then
     * you do not need to set the keyspace through that method as the
     * currently logged in keyspace will be used.
     *
     * @param keyspace the name of the keyspace this query operates on.
     * @return this {@code SimpleStatement} object.
     *
     * @see Statement#getKeyspace
     */
    public SimpleStatement setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Sets the routing key for this query.
     * <p>
     * See {@link #setRoutingKey(ByteBuffer)} for more information. This
     * method is a variant for when the query partition key is composite and
     * thus the routing key must be built from multiple values.
     *
     * @param routingKeyComponents the raw (binary) values to compose to obtain
     * the routing key.
     * @return this {@code SimpleStatement} object.
     *
     * @see Statement#getRoutingKey
     */
    public SimpleStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
        this.routingKey = compose(routingKeyComponents);
        return this;
    }

}
