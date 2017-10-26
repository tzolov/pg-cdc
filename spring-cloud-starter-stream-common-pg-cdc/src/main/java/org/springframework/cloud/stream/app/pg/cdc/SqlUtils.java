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

package org.springframework.cloud.stream.app.pg.cdc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.math.BigDecimal;


/**
 * @author Christian Tzolov
 */
public class SqlUtils {

    private final static ObjectMapper mapper = new ObjectMapper();

    public static String toJdbcType(String sqlType) {
        switch (sqlType.toLowerCase()) {
            case "timestamp without time zone":
                return "timestamp";
            case "timestamp with time zone":
                return "timestampz";
            case "time with time zone":
                return "timez";
            case "time without time zone":
                return "time";
            case "character varying":
                return "varchar";
            case "bit varying":
                return "varbit";
            case "double precision":
                return "float8";
            default:
                return sqlType;
        }
    }

    public static Object fromSqlValue(String sqlType, String value) {
        if (value == null || "null".equals(value)) {
            return "";
        }
        switch (sqlType) {
            case "int":
            case "integer":
            case "smallint":
            case "int2":
            case "int4": {
                return Integer.valueOf(value);
            }
            case "oid":
            case "int8":
            case "bigint": {
                return Long.valueOf(value);
            }
            case "real":
            case "float4": {
                return Float.valueOf(value);
            }
            case "money":
            case "float8":
            case "float": {
                return Double.valueOf(value);
            }
            case "bool":
            case "bit":
            case "boolean": {
                return Boolean.valueOf(value);
            }
            case "numeric":
            case "decimal": {
                return new BigDecimal(value);
            }
            case "char":
            case "bpchar":
            case "varchar":
            case "text":
            case "name":
            case "point": // PGpoint
            case "date": // java.sql.Date
            case "time": // java.sql.Time
            case "timetz": // java.sql.Time
            case "timestamp": // java.sql.Timestamp
            case "timestamptz": // java.sql.Timestamp
            case "varbit":
            case "json": // PGobject
            case "jsonb": // PGobject
            case "geometry":
            case "tsvector": {
                return value;
            }
        }

        throw new RuntimeException("Unsupported sql type:" + sqlType);
    }

    public static DateTime extractCommitTime(String timestampString) {
        //DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZZ");
        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSSZZ")
//                .appendFraction(DateTimeFieldType.millisOfSecond(), 0, 6)
                .toFormatter();

        return formatter.parseDateTime(timestampString);
    }


    public static String toPrettyJsonString(Object o) {
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
