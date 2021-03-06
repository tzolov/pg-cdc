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

package org.springframework.cloud.stream.app.pg.cdc.keyvalue;

import org.postgresql.PGProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Christian Tzolov
 */
@Configuration
@EnableConfigurationProperties(KeyValueProperties.class)
public class KeyValueConfiguration {

    @Bean
    public PgMetaDataPrimaryKeyColumnIndices pgMetaData(KeyValueProperties properties) throws SQLException {

        Properties props = new Properties();

        PGProperty.USER.set(props, properties.getJdbcUser());
        PGProperty.PASSWORD.set(props, properties.getJdbcPassword());

        Connection con = DriverManager.getConnection(properties.getJdbcUrl(), props);

        DatabaseMetaData meta = con.getMetaData();

        return new PgMetaDataPrimaryKeyColumnIndices(meta);
    }

}
