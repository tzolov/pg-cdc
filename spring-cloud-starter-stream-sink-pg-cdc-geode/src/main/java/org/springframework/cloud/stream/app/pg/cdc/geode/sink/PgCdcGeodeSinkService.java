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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.pdx.PdxInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.KeyValueChangeEvent;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.ChangeEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Christian Tzolov
 */
public class PgCdcGeodeSinkService {

    private Map<String, Region<String, PdxInstance>> dataset2RegionCache;

    private ClientRegionFactory<String, PdxInstance> clientRegionFactory;

    private PgCdcGeodeSinkProperties pgGeodeProperties;

    private PdxInstanceKeyValueStoreAdapter keyValueStoreAdapter;

    @Autowired
    public PgCdcGeodeSinkService(ClientRegionFactory<String, PdxInstance> clientRegionFactory, PgCdcGeodeSinkProperties pgGeodeProperties, PdxInstanceKeyValueStoreAdapter keyValueStoreAdapter) {
        this.dataset2RegionCache = new HashMap<>();
        this.clientRegionFactory = clientRegionFactory;
        this.pgGeodeProperties = pgGeodeProperties;
        this.keyValueStoreAdapter = keyValueStoreAdapter;
    }

    public void applyChangeEvent(ChangeEvent changeEvent) {

        KeyValueChangeEvent operation = keyValueStoreAdapter.handle(changeEvent);

        Region<String, PdxInstance> region = findRegion(operation.getDataset());

        switch (operation.getKind()) {
            case delete:
                region.remove(operation.getKey());
                break;
            case insert:
            case update:
                region.put(operation.getKey(), (PdxInstance) operation.getValue());
                break;
            default:
                throw new RuntimeException("Unsupported change event type:" + changeEvent.getKind());
        }
    }

    private Region<String, PdxInstance> findRegion(String regionName) {

        regionName = (pgGeodeProperties.getTableToRegionName().containsKey(regionName)) ?
                pgGeodeProperties.getTableToRegionName().get(regionName) : regionName;

        Region<String, PdxInstance> region = dataset2RegionCache.get(regionName);

        // if the regions is not explicitly defined in dataset2RegionCache try to resolve it using
        // the EntityType-as-RegionName convention.
        if (region == null) {
            try {
                region = clientRegionFactory.create(regionName);
                if (region != null) {
                    dataset2RegionCache.put(regionName, region);
                }
            } catch (RegionExistsException e) {
                throw new IllegalArgumentException("No Region exists for: " + regionName, e);
            }
        }

        return region;
    }

}
