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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * @author Christian Tzolov
 */
@Validated
@ConfigurationProperties("pg.cdc.geode")
public class PgCdcGeodeSinkProperties {

    /**
     * Gemfire Locator host name
     */
    private String locatorHost = "localhost";

    /**
     * Gemfire Locator port
     */
    private int locatorPort = 10334;

    /**
     * PDX patterns
     */
    private String pdxPatterns;

    /**
     * Should the persist the pdx instances
     */
    private boolean pdxPersistent = true;

    /**
     * Maps the Schema+Table names into Region names.
     */
    @NotNull
    private Map<String, String> tableToRegionName;

    public String getLocatorHost() {
        return locatorHost;
    }

    public void setLocatorHost(String locatorHost) {
        this.locatorHost = locatorHost;
    }

    public int getLocatorPort() {
        return locatorPort;
    }

    public void setLocatorPort(int locatorPort) {
        this.locatorPort = locatorPort;
    }

    public String getPdxPatterns() {
        return pdxPatterns;
    }

    public void setPdxPatterns(String pdxPatterns) {
        this.pdxPatterns = pdxPatterns;
    }

    public boolean isPdxPersistent() {
        return pdxPersistent;
    }

    public void setPdxPersistent(boolean pdxPersistent) {
        this.pdxPersistent = pdxPersistent;
    }

    public Map<String, String> getTableToRegionName() {
        return tableToRegionName;
    }

    public void setTableToRegionName(Map<String, String> tableToRegionName) {
        this.tableToRegionName = tableToRegionName;
    }
}
