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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cloud.stream.app.pg.cdc.keyvalue.PgMetaDataPrimaryKeyColumnIndices;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

/**
 * @author Christian Tzolov
 */
@RunWith(MockitoJUnitRunner.class)
public class PgMetaDataTest {

    @Mock
    DatabaseMetaData databaseMetaData;

    @Mock
    ResultSet columnNamesResultSet;

    @Mock
    ResultSet primaryKeysResultSet;

    @Test
    public void primaryKeyColumnIndices() throws Exception {

        when(columnNamesResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(columnNamesResultSet.getString(Matchers.eq(PgMetaDataPrimaryKeyColumnIndices.COLUMN_NAME))).thenReturn("a").thenReturn("b").thenReturn("c");

        when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(primaryKeysResultSet.getString(eq(PgMetaDataPrimaryKeyColumnIndices.COLUMN_NAME))).thenReturn("a").thenReturn("c");

        when(databaseMetaData.getColumns(anyString(), eq("boza"), eq("koza"), anyString()))
                .thenReturn(columnNamesResultSet);

        when(databaseMetaData.getPrimaryKeys(anyString(), eq("boza"), eq("koza")))
                .thenReturn(primaryKeysResultSet);

        PgMetaDataPrimaryKeyColumnIndices pgMetaData = new PgMetaDataPrimaryKeyColumnIndices(databaseMetaData);

        List<Integer> indices = pgMetaData.getPrimaryKeyColumnIndices(null, "boza", "koza");

        Assert.assertArrayEquals(new Object[]{0, 2}, indices.toArray());
    }

    @Test
    public void primaryKeyColumnIndicesWithNoPrimaryKey() throws Exception {

        when(columnNamesResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(columnNamesResultSet.getString(eq(PgMetaDataPrimaryKeyColumnIndices.COLUMN_NAME))).thenReturn("a").thenReturn("b").thenReturn("c");

        when(primaryKeysResultSet.next()).thenReturn(false);

        when(databaseMetaData.getColumns(anyString(), eq("boza"), eq("koza"), anyString()))
                .thenReturn(columnNamesResultSet);
        when(databaseMetaData.getPrimaryKeys(anyString(), eq("boza"), eq("koza")))
                .thenReturn(primaryKeysResultSet);

        PgMetaDataPrimaryKeyColumnIndices pgMetaData = new PgMetaDataPrimaryKeyColumnIndices(databaseMetaData);

        List<Integer> indices = pgMetaData.getPrimaryKeyColumnIndices(null, "boza", "koza");

        Assert.assertArrayEquals(new Object[0], indices.toArray());
    }

}