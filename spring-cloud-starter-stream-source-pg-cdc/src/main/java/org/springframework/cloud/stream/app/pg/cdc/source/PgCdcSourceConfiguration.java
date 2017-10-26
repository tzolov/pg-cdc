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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A module that reads data from an RDBMS using JDBC and creates a payload with the data.
 *
 * @author Christian Tzolov
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({PgCdcSourceProperties.class})
public class PgCdcSourceConfiguration {

    private static final Log LOG = LogFactory.getLog(PgCdcSourceConfiguration.class);

    @Autowired
    private PGReplicationStream replicationStream;

    @InboundChannelAdapter(value = Source.OUTPUT, poller = @Poller(fixedDelay = "0", maxMessagesPerPoll = "-1"))
    public String getReplicationMessage() {

        String walJson = null;

        try {
            ByteBuffer byteBuffer = replicationStream.readPending();

            if (byteBuffer != null) {
                int offset = byteBuffer.arrayOffset();
                byte[] source = byteBuffer.array();
                int length = source.length - offset;

                walJson = new String(source, offset, length);

                replicationStream.setAppliedLSN(replicationStream.getLastReceiveLSN());
                replicationStream.setFlushedLSN(replicationStream.getLastReceiveLSN());
            }
        } catch (SQLException e) {
            LOG.error("Error while streaming data", e);
        }

        return walJson;
    }

    @Bean
    public PGReplicationStream replicationStream(ChainedLogicalStreamBuilder builder) throws SQLException {
        return builder.start();
    }

    @Bean
    public ChainedLogicalStreamBuilder logicalStreamBuilder(PGConnection replicationConnection,
                                                            PgCdcSourceProperties properties) throws SQLException {

        if (properties.isRecreateReplicationSlot()) {
            try {
                replicationConnection.getReplicationAPI().dropReplicationSlot(properties.getReplicationSlot());
            } catch (Exception e) {
                LOG.warn("Unable to drop replication slot!", e);
            }
        }

        try {
            replicationConnection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(properties.getReplicationSlot())
                    .withOutputPlugin(properties.getOutputPlugin())
                    .make();
        } catch (PSQLException e) {
            LOG.warn("Unable to create replication slot!", e);
        }

        ChainedLogicalStreamBuilder builder = replicationConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(properties.getReplicationSlot())
                .withSlotOptions(properties.outputPluginOptionsAsProperties());

        // set Start position offset
        if (properties.getStartPosition() != null) {
            builder.withStartPosition(LogSequenceNumber.valueOf(properties.getStartPosition()));
        }

        return builder;
    }

    @Bean
    public PGConnection replicationConnection(PgCdcSourceProperties properties) throws SQLException {

        LOG.info("Output Plugin Options:" + properties.getOutputPluginOptions());

        Properties props = new Properties();

        PGProperty.USER.set(props, properties.getJdbcUser());
        PGProperty.PASSWORD.set(props, properties.getJdbcPassword());

        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        Connection con = DriverManager.getConnection(properties.getJdbcUrl(), props);
        PGConnection replicationConnection = con.unwrap(PGConnection.class);

        return replicationConnection;
    }

}