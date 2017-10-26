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

import logicaldecoding.parser.PgLogicalDecodingLexer;
import logicaldecoding.parser.PgLogicalDecodingParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.Change;

/**
 * PostgreSQL is shipped with pre-build with logical decoding output plugin called 'test_decoding' (https://www.postgresql.org/docs/10/static/test-decoding.html).
 * <p>
 * The {@link ToWal2JsonParser} allows to parse the test_decoding log messages into Wal2Json's plugin format: https://github.com/eulerto/wal2json
 * <p>
 * This allows to emulate the wal2json plugin format event if the later is not installed in PostgreSQL! This covers
 * some public cloud use-case.
 *
 * @author Christian Tzolov
 */
public class ToWal2JsonParser {

    private ToWal2JsonParserListener parserListener = new ToWal2JsonParserListener();

    /**
     * @param logLine Log line in the Test-Decoding text format. The input can represent BEGIN/COMMIT of Tx or DML operation
     * @return Returns complete transaction (as {@link Change} instance or null if transaction has not been committed yet.
     */
    public Change parseLogLine(String logLine) {
        PgLogicalDecodingLexer lexer = new PgLogicalDecodingLexer(new ANTLRInputStream(logLine));
        PgLogicalDecodingParser parser = new PgLogicalDecodingParser(new CommonTokenStream(lexer));
        PgLogicalDecodingParser.LoglineContext tree = parser.logline();

        ParseTreeWalker.DEFAULT.walk(parserListener, tree);

        return parserListener.getCurrentChange();
    }
}
