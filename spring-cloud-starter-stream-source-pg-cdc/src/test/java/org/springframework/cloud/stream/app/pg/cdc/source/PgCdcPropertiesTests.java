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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Christian Tzolov
 */
public class PgCdcPropertiesTests {

    private AnnotationConfigApplicationContext context;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        this.context = new AnnotationConfigApplicationContext();
    }

    @After
    public void tearDown() {
        this.context.close();
    }

    @Test
    public void replicationSlotName() {
        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.replicationSlot:myWalSlot");
        this.context.register(Conf.class);
        this.context.refresh();
        PgCdcSourceProperties properties = this.context.getBean(PgCdcSourceProperties.class);
        assertThat(properties.getReplicationSlot(), equalTo("myWalSlot"));
    }

    @Test
    public void startLsn() {
        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.replicationSlot:myWalSlot");
        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.startPosition:myStartLsn");
        this.context.register(Conf.class);
        this.context.refresh();
        PgCdcSourceProperties properties = this.context.getBean(PgCdcSourceProperties.class);
        assertThat(properties.getStartPosition(), equalTo("myStartLsn"));
    }

    @Test
    public void recreateReplicationSlot() {
        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.replicationSlot:myWalSlot");
        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.recreateReplicationSlot:true");
        this.context.register(Conf.class);
        this.context.refresh();
        PgCdcSourceProperties properties = this.context.getBean(PgCdcSourceProperties.class);
        assertThat(properties.isRecreateReplicationSlot(), equalTo(true));
    }

    @Test
    public void defaults() {
        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.replicationSlot:myWalSlot");
        this.context.register(Conf.class);
        this.context.refresh();
        PgCdcSourceProperties properties = this.context.getBean(PgCdcSourceProperties.class);
        assertThat(properties.getJdbcUrl(), equalTo("jdbc:postgresql://localhost:5432/postgres"));
        assertThat(properties.getJdbcUser(), equalTo("postgres"));
        assertThat(properties.getJdbcPassword(), equalTo("postgres"));
        assertThat(properties.getOutputPlugin(), equalTo("wal2json"));
        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-xids"), equalTo("true"));
        assertThat(properties.outputPluginOptionsAsProperties().getProperty("pretty-print"), equalTo("false"));
        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-timestamp"), equalTo("true"));
        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-lsn"), equalTo("true"));
        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-schemas"), equalTo("true"));
        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-types"), equalTo("true"));
        assertThat(properties.outputPluginOptionsAsProperties().getProperty("write-in-chunks"), equalTo("false"));
    }

    @Test
    public void outputPluginOptionsOverride() {
        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.replicationSlot:myWalSlot");
//        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.outputPluginOptions.include-xids:false");
        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.outputPluginOptions.include-timestamp=false");
//        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.outputPluginOptions.include-timestamp:true");
//        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.outputPluginOptions.include-lsn:false");
//        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.outputPluginOptions.include-schemas:false");
//        EnvironmentTestUtils.addEnvironment(this.context, "pg.cdc.outputPluginOptions.include-types:false");
        this.context.register(Conf.class);
        this.context.refresh();
        PgCdcSourceProperties properties = this.context.getBean(PgCdcSourceProperties.class);
//        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-xids"), equalTo("false"));
//        assertThat(properties.outputPluginOptionsAsProperties().getProperty("pretty-print"), equalTo("true"));
        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-timestamp"), equalTo("false"));
//        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-lsn"), equalTo("false"));
//        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-schemas"), equalTo("false"));
//        assertThat(properties.outputPluginOptionsAsProperties().getProperty("include-types"), equalTo("false"));
//        assertThat(properties.outputPluginOptionsAsProperties().getProperty("write-in-chunks"), equalTo("false"));
    }


    @Configuration
    @EnableConfigurationProperties(PgCdcSourceProperties.class)
    static class Conf {
    }
}