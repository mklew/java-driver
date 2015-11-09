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

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimpleStatementTest {

    Cluster mockCluster;

    @BeforeClass(groups = "unit")
    public void setup() {
        CodecRegistry codecRegistry = new CodecRegistry();
        mockCluster = mock(Cluster.class);
        Configuration configuration = mock(Configuration.class);
        when(mockCluster.getConfiguration()).thenReturn(configuration);
        ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(configuration.getCodecRegistry()).thenReturn(codecRegistry);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void should_fail_if_too_many_variables() {
        List<Object> args = Collections.nCopies(1 << 16, (Object)1);
        new SimpleStatement("mock query",
            mockCluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            mockCluster.getConfiguration().getCodecRegistry(),
            args.toArray());
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void should_throw_IAE_if_getObject_called_on_statement_without_values() {
        new SimpleStatement("doesn't matter",
            mockCluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            mockCluster.getConfiguration().getCodecRegistry()).getObject(0);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void should_throw_IAE_if_getObject_called_with_wrong_index() {
        new SimpleStatement("doesn't matter",
            mockCluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            mockCluster.getConfiguration().getCodecRegistry(),
            new Date()).getObject(1);
    }

    @Test(groups = "unit")
    public void should_return_object_at_ith_index() {
        Object expected = new Date();
        Object actual = new SimpleStatement("doesn't matter",
            mockCluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            mockCluster.getConfiguration().getCodecRegistry(),
            expected).getObject(0);
        assertThat(actual).isSameAs(expected);
    }
}
