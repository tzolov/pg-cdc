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

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.InMemroyPrimaryKeyColumnIndices;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.KeyValueAdapter;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.KeyValueChangeEvent;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.ChangeEvent;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * @author Christian Tzolov (christian.tzolov@gmail.com)
 */
public class KeyValueAdapterTest {

    private ObjectMapper mapper = new ObjectMapper();
    private KeyValueAdapter adapter;

    @Before
    public void before() {
        adapter = new KeyValueAdapter(null);
    }

    @Test
    public void handleDelete() throws Exception {
        ChangeEvent deleteEvent = read("src/test/resources/test_delete.json");
        KeyValueChangeEvent kvChangeEvent = adapter.handle(deleteEvent);

        assertEquals("public_table_with_unique", kvChangeEvent.getDataset());
        assertEquals(ChangeEvent.Kind.delete, kvChangeEvent.getKind());
        assertEquals("1.23_false", kvChangeEvent.getKey());
        assertEquals("", kvChangeEvent.getValue());

    }

    @Test
    public void handleUpdate() throws Exception {
        ChangeEvent updateEvent = read("src/test/resources/test_update.json");
        KeyValueChangeEvent kvChangeEvent = adapter.handle(updateEvent);

        assertEquals("public_table_with_pk", kvChangeEvent.getDataset());
        assertEquals(ChangeEvent.Kind.update, kvChangeEvent.getKind());
        assertEquals("1_2_3", kvChangeEvent.getKey());
        assertEquals("{\"a\":1,\"b\":1,\"c\":2,\"d\":3,\"e\":3.54,\"f\":-876.563,\"g\":1.23,\"h\":\"teste     \",\"i\":\"testando\",\"j\":\"um texto longo\",\"k\":\"001110010101010\",\"l\":\"Sat Nov 02 17:30:52 2013\",\"m\":\"02-04-2013\",\"n\":true,\"o\":\"{ \\\"a\\\": 123 }\",\"p\":\"'Old' 'Parr'\"}", kvChangeEvent.getValue());

    }

    @Test
    public void handleInsert() throws Exception {
        final ChangeEvent insertEvent = read("src/test/resources/test_insert.json");
        adapter = new KeyValueAdapter(
                new InMemroyPrimaryKeyColumnIndices(
                        new HashMap<String, List<Integer>>() {{
                            put(adapter.doGetDatasetName(insertEvent), asList(2, 3));
                        }}, KeyValueAdapter.DELIMITER));

        KeyValueChangeEvent kvChangeEvent = adapter.handle(insertEvent);

        assertEquals("public_xpto", kvChangeEvent.getDataset());
        assertEquals(ChangeEvent.Kind.insert, kvChangeEvent.getKind());
        assertEquals("test1_null", kvChangeEvent.getKey());
        assertEquals("{\"a\":1,\"b\":true,\"c\":\"test1\",\"d\":\"\"}", kvChangeEvent.getValue());

    }

    @Test
    public void doGetDatasetName() throws Exception {
        ChangeEvent deleteEvent = read("src/test/resources/test_delete.json");
        assertEquals("public_table_with_unique", adapter.doGetDatasetName(deleteEvent));

        ChangeEvent updateEvent = read("src/test/resources/test_update.json");
        assertEquals("public_table_with_pk", adapter.doGetDatasetName(updateEvent));

        final ChangeEvent insertEvent = read("src/test/resources/test_insert.json");
        assertEquals("public_xpto", adapter.doGetDatasetName(insertEvent));
    }

    @Test
    public void doGetKey() throws Exception {
        ChangeEvent deleteEvent = read("src/test/resources/test_delete.json");
        assertEquals("1.23_false", adapter.doGetKey(deleteEvent));

        ChangeEvent updateEvent = read("src/test/resources/test_update.json");
        assertEquals("1_2_3", adapter.doGetKey(updateEvent));

        final ChangeEvent insertEvent = read("src/test/resources/test_insert.json");
        adapter = new KeyValueAdapter(
                new InMemroyPrimaryKeyColumnIndices(
                new HashMap<String, List<Integer>>() {{
                    put(adapter.doGetDatasetName(insertEvent), asList(2, 3));
                }}, KeyValueAdapter.DELIMITER));

        assertEquals("test1_null", adapter.doGetKey(insertEvent));
    }

    @Test
    public void doGetValue() throws Exception {
        ChangeEvent deleteEvent = read("src/test/resources/test_delete.json");
        assertEquals("", adapter.doGetValue(deleteEvent));

        ChangeEvent updateEvent = read("src/test/resources/test_update.json");
        assertEquals("{\"a\":1,\"b\":1,\"c\":2,\"d\":3,\"e\":3.54,\"f\":-876.563,\"g\":1.23," +
                "\"h\":\"teste     \",\"i\":\"testando\",\"j\":\"um texto longo\",\"k\":\"001110010101010\"," +
                "\"l\":\"Sat Nov 02 17:30:52 2013\",\"m\":\"02-04-2013\",\"n\":true,\"o\":\"{ \\\"a\\\": 123 }\"," +
                "\"p\":\"'Old' 'Parr'\"}", adapter.doGetValue(updateEvent));

        final ChangeEvent insertEvent = read("src/test/resources/test_insert.json");

        assertEquals("{\"a\":1,\"b\":true,\"c\":\"test1\",\"d\":\"\"}", adapter.doGetValue(
                insertEvent));
    }

    private ChangeEvent read(String path) throws IOException {
        return mapper.readValue(new File(path), ChangeEvent.class);
    }
}