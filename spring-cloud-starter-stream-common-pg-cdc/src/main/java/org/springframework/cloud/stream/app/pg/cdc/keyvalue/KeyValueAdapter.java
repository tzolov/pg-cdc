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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.Change;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.ChangeEvent;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Adapts the WAL2JSON events into format applicable for various Key/Value NoSQL data systems.
 * Adapter computes the <b>Key</b> and the <b>Value</b> and the target <b>Dataset Name</b> from the {@link ChangeEvent}.
 * The Key, Value and Dataset Name along with the {@link ChangeEvent.Kind} allows
 * to maintain the state of the target Key/Value store.
 * <p>
 * The Dataset Name is computed from the input vent Schema and Table names.
 * <p>
 * The Key is computed either from the OldKeys in the UPDATE and DELETE {@link ChangeEvent} or by looking up
 * a pre-configured column indexes in the case of INSERT events. To lookup table datasetToPrimaryKeyColumnIndexesMap
 * allows to configure different primary-key column indices for every target dataset.
 * <p>
 * The Value is computed only for the INSERT and DELETE event types. The the DELETE event value is an empty string.
 * Value encoded an flat JSON representation of the input raw values. The column names are used as field names and the
 * column values are encoded into JSON values.
 * <p>
 * Override the default {@link KeyValueAdapter#doGetDatasetName(ChangeEvent)}, {@link KeyValueAdapter#doGetKey(ChangeEvent)}
 * and {@link KeyValueAdapter#doGetValue(ChangeEvent)} methods for further customizations.
 *
 * @author Christian Tzolov (christian.tzolov@gmail.com)
 */
public class KeyValueAdapter {

    public static final String EMPTY_STRING = "";
    public static final String DELIMITER = "_";
    /**
     * For UPDATE or DELETE events, the wal2json encoder includes the primary keys inside the message under the OldKeys
     * section. For the INSERT events there wal2json doesn't provide information for the primary keys. This meta
     * information is provided from the datasetToPrimaryKeyColumnIndexesMap externally.
     */
    private PrimaryKeyColumnIndices primaryKeyColumnIndices;

    private ObjectMapper mapper = new ObjectMapper();

    public KeyValueAdapter(PrimaryKeyColumnIndices primaryKeyColumnIndices) {
        this.primaryKeyColumnIndices = primaryKeyColumnIndices;
    }

    public KeyValueChangeEvent handle(ChangeEvent changeEvent) {
        ChangeEvent.Kind eventType = changeEvent.getKind();
        String datasetName = doGetDatasetName(changeEvent);
        String key = doGetKey(changeEvent);
        Object value = doGetValue(changeEvent);

        return new KeyValueChangeEvent(eventType, datasetName, key, value);
    }

    /**
     * Computes the target Dataset name from the input Schema and Table names.
     *
     * @param changeEvent Event form which the target Dataset name is computed.
     * @return Returns the name of the target dataset altered by this event.
     */
    protected String doGetDatasetName(ChangeEvent changeEvent) {
        return changeEvent.getSchema() + DELIMITER + changeEvent.getTable();
    }

    /**
     * For UPDATE or DELETE events, the wal2json encoder includes the primary keys inside the message under the OldKeys
     * section. For the INSERT events there wal2json doesn't provide information for the primary keys. This meta
     * information is provided from the datasetToPrimaryKeyColumnIndexesMap externally.
     *
     * @param changeEvent change event to extract the key from.
     * @return For UPDATE and DELETE event types the key is computed from the old-keys' values inside the changeEvent.
     * For the INSERT event type a pre-configured key-index table (datasetToKeyIndexesMap) is used to lookup the column
     * indexes of the values that comprise the key.
     */
    protected String doGetKey(ChangeEvent changeEvent) {
        switch (changeEvent.getKind()) {
            case update:
            case delete:
                return computeUpdateDeleteKey(changeEvent);
            case insert:
                return computeInsertKey(changeEvent);
            default:
                throw new RuntimeException("Unknown Change Event Kind:" + changeEvent.getKind());
        }
    }

    private String computeUpdateDeleteKey(ChangeEvent changeEvent) {

        StringBuilder sb = new StringBuilder();
        Iterator<Object> keyValuesIterator = changeEvent.getOldkeys().getKeyvalues().iterator();
        while (keyValuesIterator.hasNext()) {
            sb.append(keyValuesIterator.next());
            if (keyValuesIterator.hasNext()) {
                sb.append(DELIMITER);
            }
        }
        return sb.toString();
    }

    private String computeInsertKey(ChangeEvent changeEvent) {
        StringBuilder sb = new StringBuilder();
        Iterator<Integer> columnIndexIterator = getPrimaryKeyColumnIndexes(
                changeEvent.getSchema(), changeEvent.getTable()).iterator();
        while (columnIndexIterator.hasNext()) {
            sb.append(changeEvent.getColumnvalues().get(columnIndexIterator.next()));
            if (columnIndexIterator.hasNext()) {
                sb.append(DELIMITER);
            }
        }
        return sb.toString();
    }

    /**
     * @return Returns the list of column indexes that define the primary key for the dataset.
     */
    private List<Integer> getPrimaryKeyColumnIndexes(String schemaName, String tableName) {
        List<Integer> indices = primaryKeyColumnIndices.getPrimaryKeyColumnIndices(null, schemaName, tableName);
        if (indices == null) {
            throw new RuntimeException("Unknown Region:" + schemaName + "_" + tableName);
        }
        return indices;
    }

    /**
     * Value is encoded as JSON message of the input raw values. The column names are used as field names and the
     * column values are encoded into JSON values. The value for DELETE event value is an empty string.
     *
     * @param changeEvent {@link ChangeEvent} for which the value will be computed.
     * @return Returns flat json message representing the input column names and values.
     */
    protected Object doGetValue(ChangeEvent changeEvent) {

        if (changeEvent.getKind() == ChangeEvent.Kind.delete) {
            return EMPTY_STRING;
        }

        try {
            return mapper.writeValueAsString(changeEvent.columnValuesAsMap());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert the input column name/values into JSON", e);
        }
    }

    public static void main(String[] args) throws IOException {

        Map<String, List<Integer>> indicesMap = new HashMap<String, List<Integer>>() {{
            put("public_table_with_pk", asList(0, 1));
            put("public_table_without_pk", asList(0));
            put("public_xpto", asList(0));
        }};

        KeyValueAdapter keyValueAdapter = new KeyValueAdapter(new InMemroyPrimaryKeyColumnIndices(indicesMap, DELIMITER));


        ObjectMapper mapper = new ObjectMapper();

        System.out.println(">>> src/test/resources/test_change.json");
        for (ChangeEvent event : mapper.readValue(new File("spring-cloud-starter-stream-common-pg-cdc/src/test/resources/test_change.json"), Change.class).getChange()) {
            keyValueAdapter.handle(event);
        }

        System.out.println(">>> src/test/resources/test_update.json");
        keyValueAdapter.handle(mapper.readValue(new File("spring-cloud-starter-stream-common-pg-cdc/src/test/resources/test_update.json"), ChangeEvent.class));

        System.out.println(">>> src/test/resources/test_insert.json");
        keyValueAdapter.handle(mapper.readValue(new File("spring-cloud-starter-stream-common-pg-cdc/src/test/resources/test_insert.json"), ChangeEvent.class));

        System.out.println(">>> src/test/resources/test_insert2.json");
        for (ChangeEvent event : mapper.readValue(new File("spring-cloud-starter-stream-common-pg-cdc/src/test/resources/test_insert2.json"), Change.class).getChange()) {
            keyValueAdapter.handle(event);
        }
    }
}
