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

package org.springframework.cloud.stream.app.pg.cdc.geode.sink;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.pdx.PdxInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.KeyValueConfiguration;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.KeyValueProperties;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.PrimaryKeyColumnIndices;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.Change;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.ChangeEvent;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import static org.apache.geode.cache.client.ClientRegionShortcut.PROXY;

/**
 * @author Christian Tzolov
 */
@EnableBinding(Sink.class)
@Import(KeyValueConfiguration.class)
@EnableConfigurationProperties({PgCdcGeodeSinkProperties.class, KeyValueProperties.class})
public class PgCdcGeodeSinkConfiguration {

    @Autowired
    private PgCdcGeodeSinkService pgGeodeService;

    @StreamListener(Sink.INPUT)
    public void sink(Change change) {

        for (ChangeEvent changeEvent : change.getChange()) {
            pgGeodeService.applyChangeEvent(changeEvent);
        }
    }

    @Bean
    public PgCdcGeodeSinkService pgGeodeService(ClientRegionFactory<String, PdxInstance> clientRegionFactory,
                                                PgCdcGeodeSinkProperties pgGeodeProperties,
                                                PdxInstanceKeyValueStoreAdapter keyValueStoreAdapter) {
        return new PgCdcGeodeSinkService(clientRegionFactory, pgGeodeProperties, keyValueStoreAdapter);
    }

    @Bean
    public PdxInstanceKeyValueStoreAdapter pdxInstanceKeyValueStoreAdapter(PrimaryKeyColumnIndices primaryKeyColumnIndices) {
        return new PdxInstanceKeyValueStoreAdapter(primaryKeyColumnIndices);
    }

    @Bean
    public ClientRegionFactory<String, PdxInstance> clientRegionFactory(PgCdcGeodeSinkProperties properties) {

        ClientCache clientCache = getClientCache(properties.getLocatorHost(),
                properties.getLocatorPort(), properties.getPdxPatterns(), properties.isPdxPersistent());

        ClientRegionFactory<String, PdxInstance> clientRegionFactory = clientCache.createClientRegionFactory(PROXY);

        return clientRegionFactory;
    }

    private static ClientCache getClientCache(
            String locatorHost, int locatorPort, String pdxPatterns, boolean pdxPersistent) {

        ClientCache clientCache = new ClientCacheFactory().addPoolLocator(locatorHost, locatorPort)
                .setPoolSubscriptionEnabled(true)
                .set("name", "RegionGemfireClient")
                .setPdxPersistent(pdxPersistent)
//                .setPdxSerializer(new ReflectionBasedAutoSerializer(pdxPatterns))
                .create();

        return clientCache;
    }
}
