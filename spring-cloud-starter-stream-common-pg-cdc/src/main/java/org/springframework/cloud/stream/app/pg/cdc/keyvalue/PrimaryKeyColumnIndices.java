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

import java.util.List;

/**
 * @author Christian Tzolov
 */
public interface PrimaryKeyColumnIndices {
    /**
     * Retrieves the column indices of the primary key columns in the Table. It uses the column names and the
     * primary column names to compute the indices.
     *
     * @param catalog a catalog name; must match the catalog name as it
     *                is stored in the database; "" retrieves those without a catalog;
     *                <code>null</code> means that the catalog name should not be used to narrow the search.
     * @param schema  a schema name pattern; must match the schema name as it is stored in the database; "" retrieves
     *                those without a schema; <code>null</code> means that the schema name should not be used to
     *                narrow the search.
     * @param table   a table name pattern; must match the table name as it is stored in the database.
     * @return Returns the column indices of the primary key columns.
     */
    List<Integer> getPrimaryKeyColumnIndices(String catalog, String schema, String table);
}
