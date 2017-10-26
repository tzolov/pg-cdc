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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Holds configuration properties for the PG WAL Source module.
 *
 * @author Christian Tzolov
 */
@Validated
@ConfigurationProperties("pg.cdc")
public class PgCdcSourceProperties {


    public static final String OUTPUT_PLUGIN_WAL2JSON = "wal2json";

    /**
     * In the context of logical replication, a slot represents a stream of changes that can be replayed to a client
     * in the order they were made on the origin server. Each slot streams a sequence of changes from a single database.
     * A replication slot name is an identifier that is unique across all databases in a PostgreSQL cluster. Slots
     * persist independently of the connection using them and are crash-safe.
     */
    @NotNull
    private String replicationSlot;

    /**
     * The current position of each slot is persisted only at checkpoint, so in the case of a crash the slot may
     * return to an earlier LSN, which will then cause recent changes to be resent when the server restarts. Logical
     * decoding clients are responsible for avoiding ill effects from handling the same message more than once.
     * Clients may wish to record the last LSN they saw when decoding and skip over any repeated data or
     * (when using the replication protocol) request that decoding start from that LSN rather than letting the server
     * determine the start point. The Replication Progress Tracking feature is designed for this purpose, refer to
     * replication origins.
     */
    private String startPosition;

    /**
     * Drop and recreate any existing `replicationSlot`
     */
    private boolean recreateReplicationSlot;

    /**
     * JDBC URL to connect to the Postgres DB.
     */
    private String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";

    /**
     * JDBC user name
     */
    private String jdbcUser = "postgres";

    /**
     * JDBC password
     */
    private String jdbcPassword = "postgres";

    /**
     * Name of the output plugin configured in DB. The output plugin transform the data from the write-ahead log's
     * internal representation into the format the consumer of a replication slot desires.
     */
    @NotNull
    private String outputPlugin = OUTPUT_PLUGIN_WAL2JSON;

    /**
     * Output Plugin specific options. Defaults to the WAL2JSON plugin options:
     * include-xids:[true],pretty-print:[false],include-timestamp:[true],include-lsn:[true],include-schemas:[true],
     * include-types:[true], write-in-chunks:[false]
     */
    private Map<String, String> outputPluginOptions = new HashMap<>();

    private static final Map<String, String> DEFAULT_WAL2JSON_OUTPUT_PLUGIN_OPTIONS = new HashMap<String, String>() {{
        //Include the transaction ID to the change events
        put("include-xids", "true");
        //Pretty print the output JSON change events
        put("pretty-print", "false");
        //Include the transaction timestamp to the change events
        put("include-timestamp", "true");
        // Include the Last Serial Number
        put("include-lsn", "true");
        // Include the Schema name
        put("include-schemas", "true");
        // Include the column types
        put("include-types", "true");
        //
        put("write-in-chunks", "false");
    }};

    public String getReplicationSlot() {
        return replicationSlot;
    }

    public void setReplicationSlot(String replicationSlot) {
        this.replicationSlot = replicationSlot;
    }

    public String getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(String startPosition) {
        this.startPosition = startPosition;
    }

    public boolean isRecreateReplicationSlot() {
        return recreateReplicationSlot;
    }

    public void setRecreateReplicationSlot(boolean recreateReplicationSlot) {
        this.recreateReplicationSlot = recreateReplicationSlot;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getJdbcUser() {
        return jdbcUser;
    }

    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public String getOutputPlugin() {
        return outputPlugin;
    }

    public void setOutputPlugin(String outputPlugin) {
        this.outputPlugin = outputPlugin;
    }


    public Map<String, String> getOutputPluginOptions() {
        return outputPluginOptions;
    }

    public void setOutputPluginOptions(Map<String, String> outputPluginOptions) {
        this.outputPluginOptions = outputPluginOptions;
    }

    public Map<String, String> outputPluginOptionsWithDefaults() {
        if (OUTPUT_PLUGIN_WAL2JSON.equals(getOutputPlugin())) {
            HashMap<String, String> wal2jsonOptions = new HashMap<>(DEFAULT_WAL2JSON_OUTPUT_PLUGIN_OPTIONS);
            wal2jsonOptions.putAll(getOutputPluginOptions());
            return wal2jsonOptions;
        }
        return getOutputPluginOptions();
    }

    /**
     * Helper method to convert the output plugin options into {@link Properties} instance.
     *
     * @return Returns new {@link Properties} that contains the outputPluginOptions entries.
     */
    public Properties outputPluginOptionsAsProperties() {
        Properties outputPluginOptionsProperties = new Properties();
        outputPluginOptionsProperties.putAll(this.outputPluginOptionsWithDefaults());
        return outputPluginOptionsProperties;
    }

}