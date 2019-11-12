/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.db;

import com.google.protobuf.Message;
import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import no.nb.nna.veidemann.api.contentwriter.v1.CrawledContent;
import no.nb.nna.veidemann.api.contentwriter.v1.RecordType;
import no.nb.nna.veidemann.api.contentwriter.v1.StorageRef;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for RethinkDbAdapter.
 * <p>
 * These tests are dependent on a running RethinkDB instance.
 */
public class RethinkDbAdapterIT {

    public static RethinkDbConfigAdapter configAdapter;
    public static RethinkDbAdapter dbAdapter;
    public static RethinkDbConnection conn;

    static RethinkDB r = RethinkDB.r;

    public RethinkDbAdapterIT() {
    }

    @BeforeClass
    public static void init() throws DbException {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));

        if (!DbService.isConfigured()) {
            CommonSettings settings = new CommonSettings();
            DbService.configure(new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword(""));
        }

        try {
            DbService.getInstance().getDbInitializer().delete();
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
        DbService.getInstance().getDbInitializer().initialize();

        dbAdapter = (RethinkDbAdapter) DbService.getInstance().getDbAdapter();
        configAdapter = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();
        conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
    }

    @AfterClass
    public static void shutdown() {
        DbService.getInstance().close();
    }

    @Before
    public void cleanDb() throws DbException {
        for (Tables table : Tables.values()) {
            if (table != Tables.SYSTEM) {
                try {
                    dbAdapter.executeRequest("delete", r.table(table.name).delete());
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    /**
     * Test of isDuplicateContent method, of class RethinkDbAdapter.
     */
    @Test
    public void testhasCrawledContent() throws DbException {
        CrawledContent cc1 = CrawledContent.newBuilder()
                .setDigest("testIsDuplicateContent")
                .setWarcId("warc-id1")
                .setTargetUri("target-uri1")
                .setDate(ProtoUtils.getNowTs())
                .build();
        CrawledContent cc2 = CrawledContent.newBuilder()
                .setDigest("testIsDuplicateContent")
                .setWarcId("warc-id2")
                .setTargetUri("target-uri2")
                .setDate(ProtoUtils.getNowTs())
                .build();
        CrawledContent cc3 = CrawledContent.newBuilder()
                .setDigest("testIsDuplicateContent")
                .setWarcId("warc-id3")
                .setTargetUri("target-uri3")
                .setDate(ProtoUtils.getNowTs())
                .build();

        assertThat(dbAdapter.hasCrawledContent(cc1).isPresent()).isFalse();
        dbAdapter.saveStorageRef(StorageRef.newBuilder()
                .setWarcId(cc1.getWarcId())
                .setRecordType(RecordType.REQUEST)
                .setStorageRef("warcfile:test:0")
                .build());

        Optional<CrawledContent> r2 = dbAdapter.hasCrawledContent(cc2);
        assertThat(r2.isPresent()).isTrue();
        assertThat(r2.get()).isEqualTo(cc1);

        Optional<CrawledContent> r3 = dbAdapter.hasCrawledContent(cc3);
        assertThat(r3.isPresent()).isTrue();
        assertThat(r3.get()).isEqualTo(cc1);

        CrawledContent cc4 = CrawledContent.newBuilder()
                .setWarcId("warc-id4")
                .build();

        assertThatThrownBy(() -> dbAdapter.hasCrawledContent(cc4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The required field 'digest' is missing from: 'CrawledContent");
    }

    /**
     * Test of deleteCrawledContent method, of class RethinkDbAdapter.
     */
    @Test
    public void testDeleteCrawledContent() throws DbException {
        CrawledContent cc = CrawledContent.newBuilder()
                .setDigest("testDeleteCrawledContent")
                .setWarcId("warc-id")
                .setTargetUri("target-uri")
                .setDate(ProtoUtils.getNowTs())
                .build();

        dbAdapter.hasCrawledContent(cc);
        dbAdapter.deleteCrawledContent(cc.getDigest());
        dbAdapter.deleteCrawledContent(cc.getDigest());
    }

    /**
     * Test of addExtractedText method, of class RethinkDbAdapter.
     */
    @Test
    public void testAddExtractedText() throws DbException {
        ExtractedText et1 = ExtractedText.newBuilder()
                .setWarcId("testAddExtractedText")
                .setText("text")
                .build();

        ExtractedText result1 = dbAdapter.addExtractedText(et1);
        assertThat(result1).isEqualTo(et1);

        assertThatThrownBy(() -> dbAdapter.addExtractedText(et1))
                .isInstanceOf(DbException.class)
                .hasMessageContaining("Duplicate primary key");

        ExtractedText et2 = ExtractedText.newBuilder()
                .setText("text")
                .build();

        assertThatThrownBy(() -> dbAdapter.addExtractedText(et2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The required field 'warc_id' is missing from: 'ExtractedText");
    }

    /**
     * Test of addCrawlLog method, of class RethinkDbAdapter.
     */
    @Test
    public void testSaveCrawlLog() throws DbException {
        CrawlLog cl = CrawlLog.newBuilder()
                .setContentType("text/plain")
                .setJobExecutionId("jeid")
                .setExecutionId("eid")
                .setCollectionFinalName("collection")
                .build();
        CrawlLog result = dbAdapter.saveCrawlLog(cl);
        assertThat(result.getContentType()).isEqualTo("text/plain");
        assertThat(result.getWarcId()).isNotEmpty();
    }

    @Test
    public void testPaused() throws DbException {
        assertThat(dbAdapter.getDesiredPausedState()).isFalse();
        assertThat(dbAdapter.isPaused()).isFalse();

        assertThat(dbAdapter.setDesiredPausedState(true)).isFalse();

        assertThat(dbAdapter.getDesiredPausedState()).isTrue();
        assertThat(dbAdapter.isPaused()).isTrue();

        assertThat(dbAdapter.setDesiredPausedState(true)).isTrue();
        assertThat(dbAdapter.isPaused()).isTrue();

        assertThat(dbAdapter.getDesiredPausedState()).isTrue();

        assertThat(dbAdapter.setDesiredPausedState(false)).isTrue();
        assertThat(dbAdapter.isPaused()).isFalse();

        CrawlHostGroup chg = CrawlHostGroup.newBuilder().setId("chg").setBusy(true).build();
        saveMessage(chg, Tables.CRAWL_HOST_GROUP);

        assertThat(dbAdapter.getDesiredPausedState()).isFalse();
        assertThat(dbAdapter.isPaused()).isFalse();

        assertThat(dbAdapter.setDesiredPausedState(true)).isFalse();
        assertThat(dbAdapter.isPaused()).isFalse();

        chg = chg.toBuilder().setBusy(false).build();
        saveMessage(chg, Tables.CRAWL_HOST_GROUP);

        assertThat(dbAdapter.getDesiredPausedState()).isTrue();
        assertThat(dbAdapter.isPaused()).isTrue();
    }

    public <T extends Message> T saveMessage(T msg, Tables table) throws DbException {
        Map rMap = ProtoUtils.protoToRethink(msg);

        return conn.executeInsert("db-save" + msg.getClass().getSimpleName(),
                r.table(table.name)
                        .insert(rMap)
                        .optArg("conflict", "replace"),
                (Class<T>) msg.getClass()
        );

    }
}
