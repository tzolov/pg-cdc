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

import org.postgresql.PGProperty;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Christian Tzolov
 */
public class PgMetaDataPrimaryKeyColumnIndices implements PrimaryKeyColumnIndices {


    public final static String COLUMN_NAME = "COLUMN_NAME";

    private DatabaseMetaData meta;

    public PgMetaDataPrimaryKeyColumnIndices(DatabaseMetaData meta) {
        this.meta = meta;
    }

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
    public List<Integer> getPrimaryKeyColumnIndices(String catalog, String schema, String table) {

        try {
            List<String> columnNames = new ArrayList<>();
            try (ResultSet columnMeta = meta.getColumns(catalog, schema, table, null)) {
                while (columnMeta.next()) {
                    columnNames.add(columnMeta.getString(COLUMN_NAME));
                }
            }

            List<Integer> indices = new ArrayList<>();
            try (ResultSet primaryKeys = meta.getPrimaryKeys(catalog, schema, table)) {
                while (primaryKeys.next()) {
                    String pk = primaryKeys.getString(COLUMN_NAME);
                    indices.add(columnNames.indexOf(pk));
                }
            }

            return indices;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to obtain the primary key indexes:", e);
        }
    }

    public static void main(String[] args) throws SQLException {

        Properties props = new Properties();

        PGProperty.USER.set(props, "postgres");
        PGProperty.PASSWORD.set(props, "postgres");

        Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", props);

        DatabaseMetaData meta = con.getMetaData();

        PgMetaDataPrimaryKeyColumnIndices pgMetaData = new PgMetaDataPrimaryKeyColumnIndices(meta);

        System.out.println(pgMetaData.getPrimaryKeyColumnIndices(null, "public", "table_with_pk"));
        System.out.println(pgMetaData.getPrimaryKeyColumnIndices(null, "public", "table_without_pk"));

//        try (ResultSet tables = meta.getTables(null,
//                null, "%", new String[]{"TABLE"})) {
//            while (tables.next()) {
//                String catalog = tables.getString("TABLE_CAT");
//                String schema = tables.getString("TABLE_SCHEM");
//                String tableName = tables.getString("TABLE_NAME");
//                System.out.println("Table: " + tableName + ": " + catalog + ":" + schema);
//                try (ResultSet primaryKeys = meta.getPrimaryKeys(catalog, schema, tableName)) {
//                    while (primaryKeys.next()) {
//                        System.out.println("Primary key: " + primaryKeys.getString("COLUMN_NAME"));
//                    }
//                }
//                // similar for exportedKeys
//            }
//        }
    }
}
