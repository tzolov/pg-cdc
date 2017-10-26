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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.geode.pdx.JSONFormatter;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.KeyValueAdapter;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.PrimaryKeyColumnIndices;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.ChangeEvent;

/**
 * @author Christian Tzolov
 */
public class PdxInstanceKeyValueStoreAdapter extends KeyValueAdapter {

    private ObjectMapper mapper = new ObjectMapper();

    public PdxInstanceKeyValueStoreAdapter(PrimaryKeyColumnIndices primaryKeyColumnIndices) {
        super(primaryKeyColumnIndices);
    }

    @Override
    protected Object doGetValue(ChangeEvent changeEvent) {

        if (changeEvent.getKind() == ChangeEvent.Kind.delete) {
            return null;
        }

        try {
            String json = mapper.writeValueAsString(changeEvent.columnValuesAsMap());
            return JSONFormatter.fromJSON(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
