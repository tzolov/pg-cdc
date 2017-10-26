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

import org.junit.Before;
import org.junit.Test;
import org.springframework.cloud.stream.app.pg.cdc.SqlUtils;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.Change;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.ChangeEvent;

import static org.junit.Assert.*;

/**
 * @author Christian Tzolov
 */
public class Wal2JsonParserTest {

    private ToWal2JsonParser parser;

    @Before
    public void setup() {
        parser = new ToWal2JsonParser();
    }

    @Test
    public void testParseBegin() {
        Change change = parser.parseLogLine("BEGIN 15228819");
        assertNull(change);

        change = parser.parseLogLine("COMMIT 15228819");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals(null, change.getTimestamp());
        assertEquals(0, change.getChange().size());
        assertEquals(null, change.getNextlsn());
    }

    @Test
    public void testParseCommitWithTimestampSecondFractions() {
        Change change = parser.parseLogLine("BEGIN 15228819");
        assertNull(change);

        change = parser.parseLogLine("COMMIT 15228819 (at 2016-03-29 08:25:43.3+02)");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals("2016-03-29 08:25:43.3+02", change.getTimestamp());
        assertEquals(2016, SqlUtils.extractCommitTime(change.getTimestamp()).getYear());
        assertEquals(0, change.getChange().size());
        assertEquals(null, change.getNextlsn());
    }

    @Test
    public void testParseCommitWithTimestampSecondHundreds() {
        Change change = parser.parseLogLine("BEGIN 15228819");
        assertNull(change);

        change = parser.parseLogLine("COMMIT 15228819 (at 2016-03-29 08:25:43.35+02)");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals("2016-03-29 08:25:43.35+02", change.getTimestamp());
        assertEquals(2016, SqlUtils.extractCommitTime(change.getTimestamp()).getYear());
        assertEquals(0, change.getChange().size());
        assertEquals(null, change.getNextlsn());
    }

    @Test
    public void testParseCommitWithTimestampMillis() {
        Change change = parser.parseLogLine("BEGIN 15228819");
        assertNull(change);

        change = parser.parseLogLine("COMMIT 15228819 (at 2000-01-01 01:00:00.303+02)");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals("2000-01-01 01:00:00.303+02", change.getTimestamp());
        assertEquals(2000, SqlUtils.extractCommitTime(change.getTimestamp()).getYear());
        assertEquals(0, change.getChange().size());
        assertEquals(null, change.getNextlsn());
    }

    @Test
    public void testParseCommitWithTimestampNanos() {
        Change change = parser.parseLogLine("BEGIN 15228819");
        assertNull(change);

        change = parser.parseLogLine("COMMIT 15228819 (at 2016-07-08 19:58:13.532396+02)");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals("2016-07-08 19:58:13.532396+02", change.getTimestamp());
        assertEquals(2016, SqlUtils.extractCommitTime(change.getTimestamp()).getYear());
        assertEquals(0, change.getChange().size());
        assertEquals(null, change.getNextlsn());
    }

    @Test
    public void testParseInsert() {
        Change change = parser.parseLogLine("BEGIN 15228819");
        assertNull(change);

        assertNull(parser.parseLogLine("table tmp.landkreis_neu: INSERT: alkis_id[text]:'LANDSBERG' beschriftung_pos[geometry]:null datenquelle[integer]:3 erfasst_am[date]:'2015-11-05' erfasst_durch[text]:'Schmidt.Sebastian2' the_geom[geometry]:'0106000020EC7A0000010000000103000000010000000C0000003B84A88392C750411155771E44765441900A7B95C9C4504109F537F0098C544156B5B1BDAFD050412B43E90B9F925441B01595649CDE5041DDD9BEDBDF925441411B22CC85EB5041978D3C8D989054410B76780B89EC5041E8A686D4748554419CBE9CBF34E65041031B275B547B54412A9A38A985D75041AC27CC7EC2755441450ED92F65CD504173159AF36A6E5441F53726354BC55041EF06C602AF6F544113194F8685C3504118721F00BC7354413B84A88392C750411155771E44765441' geaendert_am[date]:null geaendert_durch[text]:null landkreisschluessel[integer]:1889 name[text]:'Landsberg am Lech' landkreis_id[integer]:35 _version[integer]:4854754 kuerzel[text]:'LL'"));

        change = parser.parseLogLine("COMMIT 15228819");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals(1, change.getChange().size());

        ChangeEvent changeEvent = change.getChange().iterator().next();

        assertNull(changeEvent.getOldkeys());
        assertEquals(ChangeEvent.Kind.insert, changeEvent.getKind());
        assertEquals("landkreis_neu", changeEvent.getTable());
        assertEquals("tmp", changeEvent.getSchema());
        assertEquals(13, changeEvent.getColumnnames().size());
        assertEquals(13, changeEvent.getColumnvalues().size());
        assertEquals(13, changeEvent.getColumntypes().size());
    }

    @Test
    public void testParseInsert2() {
        Change change = parser.parseLogLine("BEGIN 15228819");
        assertNull(change);

        assertNull(parser.parseLogLine("table public.allsupportedtypes: INSERT: a_serial[integer]:5 a_numeric[numeric]:null a_real[real]:null a_double[double precision]:null a_char[character]:null a_varchar[character varying]:'text' a_text[text]:'text with blanks and [ ] brackets' a_boolean[boolean]:null a_json[json]:null a_jsonb[jsonb]:null a_date[date]:null a_timestamp[timestamp without time zone]:null a_interval[interval]:null a_tsvector[tsvector]:null a_uuid[uuid]:null a_postgis_geom[geometry]:null"));

        change = parser.parseLogLine("COMMIT 15228819");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals(1, change.getChange().size());

        ChangeEvent changeEvent = change.getChange().iterator().next();

        assertNull(changeEvent.getOldkeys());
        assertEquals(ChangeEvent.Kind.insert, changeEvent.getKind());
        assertEquals("allsupportedtypes", changeEvent.getTable());
        assertEquals("public", changeEvent.getSchema());
        assertEquals(16, changeEvent.getColumnnames().size());
        assertEquals(16, changeEvent.getColumnvalues().size());
        assertEquals(16, changeEvent.getColumntypes().size());
    }

    @Test
    public void testParseUpdate() {
        parser.parseLogLine("BEGIN 15228819");
        parser.parseLogLine("table tmp.landkreis_neu: UPDATE: old-key: alkis_id[text]:'LANDSBERG' datenquelle[integer]:3 erfasst_am[date]:'2015-11-05' erfasst_durch[text]:'Schmidt.Sebastian2' the_geom[geometry]:'0106000020EC7A0000010000000103000000010000000C0000003B84A88392C750411155771E44765441900A7B95C9C4504109F537F0098C544156B5B1BDAFD050412B43E90B9F925441B01595649CDE5041DDD9BEDBDF925441411B22CC85EB5041978D3C8D989054410B76780B89EC5041E8A686D4748554419CBE9CBF34E65041031B275B547B54412A9A38A985D75041AC27CC7EC2755441450ED92F65CD504173159AF36A6E5441F53726354BC55041EF06C602AF6F544113194F8685C3504118721F00BC7354413B84A88392C750411155771E44765441' landkreisschluessel[integer]:1889 name[text]:'Landsberg am Lech' landkreis_id[integer]:35 _version[integer]:4854754 kuerzel[text]:'LL' new-tuple: alkis_id[text]:'LANDSBERG' beschriftung_pos[geometry]:null datenquelle[integer]:3 erfasst_am[date]:'2015-11-05' erfasst_durch[text]:'Schmidt.Sebastian2' the_geom[geometry]:'0106000020EC7A0000010000000103000000010000000C0000003B84A88392C750411155771E44765441900A7B95C9C4504109F537F0098C544156B5B1BDAFD050412B43E90B9F925441B01595649CDE5041DDD9BEDBDF925441411B22CC85EB5041978D3C8D989054410B76780B89EC5041E8A686D4748554419CBE9CBF34E65041031B275B547B54412A9A38A985D75041AC27CC7EC2755441450ED92F65CD504173159AF36A6E5441F53726354BC55041EF06C602AF6F544113194F8685C3504118721F00BC7354413B84A88392C750411155771E44765441' geaendert_am[date]:null geaendert_durch[text]:null landkreisschluessel[integer]:1889 name[text]:'Landsberg am Lechtal' landkreis_id[integer]:35 _version[integer]:4854754 kuerzel[text]:'LL'");

        Change change = parser.parseLogLine("COMMIT 15228819");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals(1, change.getChange().size());

        ChangeEvent changeEvent = change.getChange().iterator().next();

        assertNotNull(changeEvent.getOldkeys());
        assertEquals(10, changeEvent.getOldkeys().getKeynames().size());
        assertEquals(10, changeEvent.getOldkeys().getKeyvalues().size());
        assertEquals(10, changeEvent.getOldkeys().getKeytypes().size());

        assertEquals(ChangeEvent.Kind.update, changeEvent.getKind());
        assertEquals("landkreis_neu", changeEvent.getTable());
        assertEquals("tmp", changeEvent.getSchema());

        assertEquals(13, changeEvent.getColumnnames().size());
        assertEquals(13, changeEvent.getColumnvalues().size());
        assertEquals(13, changeEvent.getColumntypes().size());
    }

    @Test
    public void testParseUpdate2() {
        parser.parseLogLine("BEGIN 15228819");
        parser.parseLogLine("table tmp.landkreis_neu: UPDATE: old-key: alkis_id[text]:'DEBYASDF' datenquelle[integer]:3 erfasst_am[date]:'2015-11-11' erfasst_durch[text]:'Schmidt.Sebastian2' the_geom[geometry]:'0106000020EC7A00000100000001030000000100000007000000C262AD677CC5504163ADF3D62E855441C07B7D2BC3CF5041A1A2DD9DE5935441FCF2D8E814E4504122FF966E168A54417E25B80B24DC504127755F9E0F6E544102110AD094C05041E71AB7913A6F5441839B803B1DC05041A4852AED687E5441C262AD677CC5504163ADF3D62E855441' landkreisschluessel[integer]:166 name[text]:'Landsberg am Lech' landkreis_id[integer]:55 kuerzel[text]:'LL' new-tuple: alkis_id[text]:'DEBYASDF' beschriftung_pos[geometry]:null datenquelle[integer]:3 erfasst_am[date]:'2015-11-11' erfasst_durch[text]:'Schmidt.Sebastian2' the_geom[geometry]:'0106000020EC7A0000010000000103000000010000000700000046BB834B77B05041E07EF7A05598544183DA47C04FBD50415D7AD5189EA95441FED90825CED950415F8BDF02799D5441C24DC0906BC65041E61E9A7E707A54410000008005A55041000000C03D7D544107F4561F18AB504121572EB78F91544146BB834B77B05041E07EF7A055985441' geaendert_am[date]:null geaendert_durch[text]:null landkreisschluessel[integer]:166 name[text]:'Landsberg am Lech' landkreis_id[integer]:55 _version[integer]:null kuerzel[text]:'LL'");

        Change change = parser.parseLogLine("COMMIT 15228819");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals(1, change.getChange().size());

        ChangeEvent changeEvent = change.getChange().iterator().next();

        assertNotNull(changeEvent.getOldkeys());
        assertEquals(9, changeEvent.getOldkeys().getKeynames().size());
        assertEquals(9, changeEvent.getOldkeys().getKeyvalues().size());
        assertEquals(9, changeEvent.getOldkeys().getKeytypes().size());

        assertEquals(ChangeEvent.Kind.update, changeEvent.getKind());
        assertEquals("landkreis_neu", changeEvent.getTable());
        assertEquals("tmp", changeEvent.getSchema());

        assertEquals(13, changeEvent.getColumnnames().size());
        assertEquals(13, changeEvent.getColumnvalues().size());
        assertEquals(13, changeEvent.getColumntypes().size());
    }

    @Test
    public void testParseDelete() {
        parser.parseLogLine("BEGIN 15228819");
        parser.parseLogLine("table tmp.landkreis_neu: DELETE: alkis_id[text]:'LANDSBERG' datenquelle[integer]:3 erfasst_am[date]:'2015-11-05' erfasst_durch[text]:'Schmidt.Sebastian2' the_geom[geometry]:'0106000020EC7A0000010000000103000000010000000C0000003B84A88392C750411155771E44765441900A7B95C9C4504109F537F0098C544156B5B1BDAFD050412B43E90B9F925441B01595649CDE5041DDD9BEDBDF925441411B22CC85EB5041978D3C8D989054410B76780B89EC5041E8A686D4748554419CBE9CBF34E65041031B275B547B54412A9A38A985D75041AC27CC7EC2755441450ED92F65CD504173159AF36A6E5441F53726354BC55041EF06C602AF6F544113194F8685C3504118721F00BC7354413B84A88392C750411155771E44765441' landkreisschluessel[integer]:1889 name[text]:'Landsberg am Lechtal' landkreis_id[integer]:35 _version[integer]:4854754 kuerzel[text]:'LL'");

        Change change = parser.parseLogLine("COMMIT 15228819");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals(1, change.getChange().size());

        ChangeEvent changeEvent = change.getChange().iterator().next();

        assertNotNull(changeEvent.getOldkeys());
        assertEquals(10, changeEvent.getOldkeys().getKeynames().size());
        assertEquals(10, changeEvent.getOldkeys().getKeyvalues().size());
        assertEquals(10, changeEvent.getOldkeys().getKeytypes().size());

        assertEquals(ChangeEvent.Kind.delete, changeEvent.getKind());
        assertEquals("landkreis_neu", changeEvent.getTable());
        assertEquals("tmp", changeEvent.getSchema());

        assertNull(changeEvent.getColumnnames());
        assertNull(changeEvent.getColumnvalues());
        assertNull(changeEvent.getColumntypes());
    }

    @Test
    public void testDeletelNamesWithUnderscores() {
        testDeleteWithSpecialType("table a.a: DELETE: _version[integer]:4854754",
                "_version", 4854754, "integer");
    }


    @Test
    public void testDeleteWithIntegerDataType() {
        testDeleteWithSpecialType("table a.a: DELETE: landkreisschluessel[integer]:1889",
                "landkreisschluessel", 1889, "integer");
    }

    @Test
    public void testDeleteNegativeIntegerDataType() {
        testDeleteWithSpecialType("table a.a: DELETE: landkreisschluessel[integer]:-1889",
                "landkreisschluessel", -1889, "integer");
    }

    @Test
    public void testDeleteWithTextDataType() {
        testDeleteWithSpecialType("table a.a: DELETE: name[text]:'text am Lech'",
                "name", "text am Lech", "text");
    }

    @Test
    public void testDeleteWithGeometryDataType() {
        testDeleteWithSpecialType("table a.a: DELETE: the_geom[geometry]:'0106000020EC7A0000010000000103000000010000000C0000003B84A88392C750411155771E44765441900A7B95C9C4504109F537F0098C544156B5B1BDAFD050412B43E90B9F925441B01595649CDE5041DDD9BEDBDF925441411B22CC85EB5041978D3C8D989054410B76780B89EC5041E8A686D4748554419CBE9CBF34E65041031B275B547B54412A9A38A985D75041AC27CC7EC2755441450ED92F65CD504173159AF36A6E5441F53726354BC55041EF06C602AF6F544113194F8685C3504118721F00BC7354413B84A88392C750411155771E44765441'",
                "the_geom",
                "0106000020EC7A0000010000000103000000010000000C0000003B84A88392C750411155771E44765441900A7B95C9C4504109F537F0098C544156B5B1BDAFD050412B43E90B9F925441B01595649CDE5041DDD9BEDBDF925441411B22CC85EB5041978D3C8D989054410B76780B89EC5041E8A686D4748554419CBE9CBF34E65041031B275B547B54412A9A38A985D75041AC27CC7EC2755441450ED92F65CD504173159AF36A6E5441F53726354BC55041EF06C602AF6F544113194F8685C3504118721F00BC7354413B84A88392C750411155771E44765441",
                "geometry");
    }

    @Test
    public void testDeleteWithRealDataType() {
        testDeleteWithSpecialType("table a.a: DELETE: height[real]:3.7",
                "height", 3.7f, "real");
    }

    @Test
    public void testDeleteWithBooleanDataType() {
        testDeleteWithSpecialType("table a.a: DELETE: toggle[boolean]:true",
                "toggle", true, "boolean");
    }

    @Test
    public void testDeleteWithJsonbType() {
        testDeleteWithSpecialType("table a.a: DELETE: value[jsonb]:'{\"jobsite_id\": -2.8}'",
                "value", "{\"jobsite_id\": -2.8}",
                "jsonb");
    }

    private void testDeleteWithSpecialType(String logLine, String expectedName, Object expectedValue, String sqlType) {

        parser.parseLogLine("BEGIN 15228819");
        parser.parseLogLine(logLine);

        Change change = parser.parseLogLine("COMMIT 15228819");

        assertEquals(15228819, change.getXid().intValue());
        assertEquals(1, change.getChange().size());

        ChangeEvent changeEvent = change.getChange().iterator().next();

        assertNotNull(changeEvent.getOldkeys());
        assertEquals(1, changeEvent.getOldkeys().getKeynames().size());
        assertEquals(1, changeEvent.getOldkeys().getKeyvalues().size());
        assertEquals(1, changeEvent.getOldkeys().getKeytypes().size());

        assertEquals(expectedName, changeEvent.getOldkeys().getKeynames().iterator().next());
        assertEquals(expectedValue, changeEvent.getOldkeys().getKeyvalues().iterator().next());
        assertEquals(sqlType, changeEvent.getOldkeys().getKeytypes().iterator().next());

        assertEquals(ChangeEvent.Kind.delete, changeEvent.getKind());
        assertEquals("a", changeEvent.getTable());
        assertEquals("a", changeEvent.getSchema());

        assertNull(changeEvent.getColumnnames());
        assertNull(changeEvent.getColumnvalues());
        assertNull(changeEvent.getColumntypes());
    }

}