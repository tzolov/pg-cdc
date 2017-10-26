/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.pg.cdc.source;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Integration Tests for JdbcSource. Uses hsqldb as a (real) embedded DB.
 *
 * @author Christian Tzolov
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
public abstract class PgCdcSourceIntegrationTests {

    @Autowired
    protected Source pgWalSource;

    @Autowired
    protected MessageCollector messageCollector;

    @TestPropertySource(properties = "pg.cdc.replicationSlot=mySlot")
    public static class DefaultBehaviorTests extends PgCdcSourceIntegrationTests {

        @MockBean
        private PGReplicationStream replicationStream;

        @MockBean
        private PGConnection pgConnection;

        @MockBean
        private ChainedLogicalStreamBuilder logicalStreamBuilder;

        @Test
        public void testReadPending() throws InterruptedException, SQLException {

            final String testData[] = {"Test Data 1", "Test Data 2"};

            when(replicationStream.readPending()).thenAnswer(new Answer<ByteBuffer>() {
                private int count = 0;

                @Override
                public ByteBuffer answer(InvocationOnMock invocationOnMock) throws Throwable {
                    ByteBuffer buffer = null;
                    if (count < testData.length) {
                        buffer = ByteBuffer.allocate(testData[count].length()).put(testData[count].getBytes());
                        count++;
                    }
                    return buffer;
                }
            });

            Message<?> received = messageCollector.forChannel(pgWalSource.output()).poll(2, TimeUnit.SECONDS);
            assertNotNull(received);
            assertThat(received.getPayload(), Matchers.instanceOf(String.class));
            assertEquals("Test Data 1", received.getPayload());

            received = messageCollector.forChannel(pgWalSource.output()).poll(2, TimeUnit.SECONDS);
            assertNotNull(received);
            assertThat(received.getPayload(), Matchers.instanceOf(String.class));
            assertEquals("Test Data 2", received.getPayload());

            received = messageCollector.forChannel(pgWalSource.output()).poll(2, TimeUnit.SECONDS);
            assertNull(received);

            verify(replicationStream, times(2)).setAppliedLSN(null);
            verify(replicationStream, times(2)).setFlushedLSN(null);
        }
    }

//    @TestPropertySource(properties = "pg.cdc.replicationSlot=demo_logical_slot")
//    public static class RealIntegrationTests extends PgCdcSourceIntegrationTests {
//
//        @Test
//        public void testReadPending() throws InterruptedException, SQLException {
//
//            Message<?> received = messageCollector.forChannel(pgWalSource.output()).poll(2, TimeUnit.SECONDS);
//            assertNotNull(received);
//            assertThat(received.getPayload(), Matchers.instanceOf(String.class));
//            assertEquals("{\"xid\":882,\"nextlsn\":\"0/1560858\",\"timestamp\":\"2017-10-03 11:22:43.773734+02\",\"change\":[{\"kind\":\"insert\",\"schema\":\"public\",\"table\":\"table_with_pk\",\"columnnames\":[\"a\",\"b\",\"c\"],\"columntypes\":[\"int4\",\"varchar\",\"timestamp\"],\"columnvalues\":[31,\"Backup and Restore\",\"2017-10-03 11:22:43.772821\"]},{\"kind\":\"insert\",\"schema\":\"public\",\"table\":\"table_with_pk\",\"columnnames\":[\"a\",\"b\",\"c\"],\"columntypes\":[\"int4\",\"varchar\",\"timestamp\"],\"columnvalues\":[32,\"Tuning\",\"2017-10-03 11:22:43.772821\"]},{\"kind\":\"insert\",\"schema\":\"public\",\"table\":\"table_with_pk\",\"columnnames\":[\"a\",\"b\",\"c\"],\"columntypes\":[\"int4\",\"varchar\",\"timestamp\"],\"columnvalues\":[33,\"Replication\",\"2017-10-03 11:22:43.772821\"]},{\"kind\":\"insert\",\"schema\":\"public\",\"table\":\"table_without_pk\",\"columnnames\":[\"a\",\"b\",\"c\"],\"columntypes\":[\"int4\",\"numeric\",\"text\"],\"columnvalues\":[11,2.34,\"Tapir\"]}]}", received.getPayload());
//
//            received = messageCollector.forChannel(pgWalSource.output()).poll(2, TimeUnit.SECONDS);
//            assertNull(received);
//        }
//    }

    @SpringBootApplication
    public static class PgWalSourceApplication {

    }
}