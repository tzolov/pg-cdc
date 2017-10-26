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
import org.springframework.cloud.stream.app.pg.cdc.SqlUtils;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Christian Tzolov (christian.tzolov@gmail.com)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChangeEvent {

    public enum Kind {insert, update, delete}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class OldKeys {
        private List<String> keynames;
        private List<String> keytypes;
        private List<Object> keyvalues;

        public List<String> getKeynames() {
            return keynames;
        }

        public void setKeynames(List<String> keynames) {
            this.keynames = keynames;
        }

        public List<String> getKeytypes() {
            return keytypes;
        }

        public void setKeytypes(List<String> keytypes) {
            this.keytypes = keytypes;
        }

        public List<Object> getKeyvalues() {
            return keyvalues;
        }

        public void setKeyvalues(List<Object> keyvalues) {
            this.keyvalues = keyvalues;
        }
    }

    private Kind kind;
    private String schema;
    private String table;
    private List<String> columnnames;
    private List<String> columntypes;
    private List<Object> columnvalues;
    private OldKeys oldkeys;

    public Kind getKind() {
        return kind;
    }

    public void setKind(Kind kind) {
        this.kind = kind;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getColumnnames() {
        return columnnames;
    }

    public void setColumnnames(List<String> columnnames) {
        this.columnnames = columnnames;
    }

    public List<String> getColumntypes() {
        return columntypes;
    }

    public void setColumntypes(List<String> columntypes) {
        this.columntypes = columntypes;
    }

    public List<Object> getColumnvalues() {
        return columnvalues;
    }

    public void setColumnvalues(List<Object> columnvalues) {
        this.columnvalues = columnvalues;
    }

    public OldKeys getOldkeys() {
        return oldkeys;
    }

    public void setOldkeys(OldKeys oldkeys) {
        this.oldkeys = oldkeys;
    }

    @Override
    public String toString() {
        return "ChangeEvent{" +
                "kind='" + kind + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", columnnames=" + columnnames +
                ", columntypes=" + columntypes +
                ", columnvalues=" + columnvalues +
                '}';
    }

    public Map<String, Object> columnValuesAsMap() {

        Assert.isTrue(this.getKind() == Kind.update ||
                this.getKind() == Kind.insert, "Only Update and Insert events hold value");

        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < this.getColumnnames().size(); i++) {
            String fieldName = this.getColumnnames().get(i);
            String sqlType = this.getColumntypes().get(i);
            Object originalValue = this.getColumnvalues().get(i);
            Object fieldValue = SqlUtils.fromSqlValue(sqlType, "" + originalValue);
            map.put(fieldName, fieldValue);
        }

        return map;
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        print(mapper, "src/test/resources/test_insert.json");
        print(mapper, "src/test/resources/test_update.json");
        print(mapper, "src/test/resources/test_delete.json");
    }

    private static void print(ObjectMapper mapper, String path) throws IOException {
        System.out.println(SqlUtils.toPrettyJsonString(
                mapper.readValue(new File(path), ChangeEvent.class)));
    }
}
