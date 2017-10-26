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

import org.springframework.cloud.stream.app.pg.cdc.wal2json.ChangeEvent;

/**
 * @author Christian Tzolov
 */
public class KeyValueChangeEvent {

    private ChangeEvent.Kind kind;

    private String dataset;
    private String key;
    private Object value;

    public KeyValueChangeEvent() {
    }

    public KeyValueChangeEvent(ChangeEvent.Kind kind, String dataset, String key, Object value) {
        this.kind = kind;
        this.dataset = dataset;
        this.key = key;
        this.value = value;
    }

    public ChangeEvent.Kind getKind() {
        return kind;
    }

    public void setKind(ChangeEvent.Kind kind) {
        this.kind = kind;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
