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

package org.springframework.cloud.stream.app.pg.cdc.decoding.adapter.processor.parser;

import logicaldecoding.parser.PgLogicalDecodingBaseListener;
import logicaldecoding.parser.PgLogicalDecodingParser;
import org.antlr.v4.runtime.misc.NotNull;
import org.springframework.cloud.stream.app.pg.cdc.SqlUtils;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.Change;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.ChangeEvent;

import java.util.ArrayList;

/**
 * @author Christian Tzolov
 */
public class ToWal2JsonParserListener extends PgLogicalDecodingBaseListener {

    private Change currentChange;
    private ChangeEvent currentChangeEvent;

    private String currentColumnName;
    private String currentColumnValue;
    private String currentColumnSqlType;

    private String tableName;
    private String schemaName;

    private boolean isReady;

    @Override
    public void enterTxStatement(PgLogicalDecodingParser.TxStatementContext ctx) {
    }

    @Override
    public void enterBeginTxStatement(PgLogicalDecodingParser.BeginTxStatementContext ctx) {
        currentChange = new Change();
        currentChange.setChange(new ArrayList<ChangeEvent>());
        isReady = false;
    }

    @Override
    public void enterCommitTxStatement(PgLogicalDecodingParser.CommitTxStatementContext ctx) {
        isReady = true;
    }

    @Override
    public void enterCommitTxId(PgLogicalDecodingParser.CommitTxIdContext ctx) {
        currentChange.setXid(Integer.valueOf(ctx.getText()));
    }

    @Override
    public void exitCommitTimestamp(PgLogicalDecodingParser.CommitTimestampContext ctx) {
        currentChange.setTimestamp(ctx.getText());
    }

    @Override
    public void enterInsertOp(PgLogicalDecodingParser.InsertOpContext ctx) {
        enterDmlOperation(ChangeEvent.Kind.insert);
    }

    @Override
    public void enterUpdateOp(PgLogicalDecodingParser.UpdateOpContext ctx) {
        enterDmlOperation(ChangeEvent.Kind.update);
    }

    @Override
    public void enterDeleteOp(PgLogicalDecodingParser.DeleteOpContext ctx) {
        enterDmlOperation(ChangeEvent.Kind.delete);
    }

    private void enterDmlOperation(ChangeEvent.Kind kind) {
        currentChangeEvent = new ChangeEvent();
        currentChangeEvent.setKind(kind);
        currentChangeEvent.setTable(tableName);
        currentChangeEvent.setSchema(schemaName);

        currentChange.getChange().add(currentChangeEvent);
    }

    public void enterSchemaname(@NotNull PgLogicalDecodingParser.SchemanameContext ctx) {
        schemaName = ctx.Identifier().getText();
    }

    public void enterTablename(@NotNull PgLogicalDecodingParser.TablenameContext ctx) {
        tableName = ctx.Identifier().getText();
    }

    @Override
    public void enterOldKeyValuePair(@NotNull PgLogicalDecodingParser.OldKeyValuePairContext ctx) {
        if (currentChangeEvent.getOldkeys() == null) {
            currentChangeEvent.setOldkeys(new ChangeEvent.OldKeys());
            currentChangeEvent.getOldkeys().setKeytypes(new ArrayList<String>());
            currentChangeEvent.getOldkeys().setKeyvalues(new ArrayList<>());
            currentChangeEvent.getOldkeys().setKeynames(new ArrayList<String>());
        }
    }

    @Override
    public void enterNewKeyValuePair(@NotNull PgLogicalDecodingParser.NewKeyValuePairContext ctx) {
        if (currentChangeEvent.getColumntypes() == null) {
            currentChangeEvent.setColumntypes(new ArrayList<String>());
        }
        if (currentChangeEvent.getColumnvalues() == null) {
            currentChangeEvent.setColumnvalues(new ArrayList<>());
        }
        if (currentChangeEvent.getColumnnames() == null) {
            currentChangeEvent.setColumnnames(new ArrayList<String>());
        }
    }

    @Override
    public void exitNewKeyValuePair(@NotNull PgLogicalDecodingParser.NewKeyValuePairContext ctx) {
        currentChangeEvent.getColumnnames().add(currentColumnName);
        currentChangeEvent.getColumnvalues().add(SqlUtils.fromSqlValue(currentColumnSqlType, currentColumnValue));
        currentChangeEvent.getColumntypes().add(currentColumnSqlType);
    }

    @Override
    public void exitOldKeyValuePair(@NotNull PgLogicalDecodingParser.OldKeyValuePairContext ctx) {
        currentChangeEvent.getOldkeys().getKeynames().add(currentColumnName);
        currentChangeEvent.getOldkeys().getKeyvalues().add(SqlUtils.fromSqlValue(currentColumnSqlType, currentColumnValue));
        currentChangeEvent.getOldkeys().getKeytypes().add(currentColumnSqlType);
    }

    @Override
    public void enterColumnname(@NotNull PgLogicalDecodingParser.ColumnnameContext ctx) {
        currentColumnName = ctx.Identifier().getText();
    }

    @Override
    public void enterTypedef(@NotNull PgLogicalDecodingParser.TypedefContext ctx) {
        currentColumnSqlType = SqlUtils.toJdbcType(ctx.getText().trim());
    }

    @Override
    public void enterValue(@NotNull PgLogicalDecodingParser.ValueContext ctx) {
        currentColumnValue = ctx.getText();
    }

    @Override
    public void enterQuotedValue(@NotNull PgLogicalDecodingParser.QuotedValueContext ctx) {
        String value = ctx.getText();
        if (value.startsWith("'") && value.endsWith("'")) {
            int length = value.length();
            value = value.substring(1, length - 1);
        }
        currentColumnValue = value;
    }

    public Change getCurrentChange() {
        if (isReady) {
            return currentChange;
        }
        return null;
    }
}
