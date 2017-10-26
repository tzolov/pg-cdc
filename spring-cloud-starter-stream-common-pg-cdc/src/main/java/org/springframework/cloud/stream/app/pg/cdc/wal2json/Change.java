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

package org.springframework.cloud.stream.app.pg.cdc.wal2json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Christian Tzolov (christian.tzolov@gmail.com)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Change {

    private Integer xid;
    private String nextlsn;
    private String timestamp;
    private List<ChangeEvent> change;

    public Integer getXid() {
        return xid;
    }

    public void setXid(Integer xid) {
        this.xid = xid;
    }

    public String getNextlsn() {
        return nextlsn;
    }

    public void setNextlsn(String nextlsn) {
        this.nextlsn = nextlsn;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public List<ChangeEvent> getChange() {
        return change;
    }

    public void setChange(List<ChangeEvent> change) {
        this.change = change;
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        Change change = mapper.readValue(new File("src/test/resources/test_change.json"), Change.class);

        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(change));
    }

}
