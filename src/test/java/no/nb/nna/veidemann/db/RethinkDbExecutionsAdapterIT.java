/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import no.nb.nna.veidemann.api.contentwriter.v1.CrawledContent;
import no.nb.nna.veidemann.api.contentwriter.v1.RecordType;
import no.nb.nna.veidemann.api.contentwriter.v1.StorageRef;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.api.report.v1.CrawlExecutionsListRequest;
import no.nb.nna.veidemann.api.report.v1.JobExecutionsListRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RethinkDbExecutionsAdapterIT {
    public static RethinkDbConnection conn;
    public static RethinkDbExecutionsAdapter executionsAdapter;

    static RethinkDB r = RethinkDB.r;

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

        executionsAdapter = (RethinkDbExecutionsAdapter) DbService.getInstance().getExecutionsAdapter();
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
                    conn.exec("delete", r.table(table.name).delete());
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    private CrawlExecutionStatus createCrawlExecutionStatus(String jobId, String jobExecutionId, String seedId) throws DbException {
        Objects.requireNonNull(jobId, "jobId must be set");
        Objects.requireNonNull(jobExecutionId, "jobExecutionId must be set");
        Objects.requireNonNull(seedId, "seedId must be set");

        CrawlExecutionStatus status = CrawlExecutionStatus.newBuilder()
                .setJobId(jobId)
                .setJobExecutionId(jobExecutionId)
                .setSeedId(seedId)
                .setState(CrawlExecutionStatus.State.CREATED)
                .build();

        Map rMap = ProtoUtils.protoToRethink(status);
        rMap.put("lastChangeTime", r.now());
        rMap.put("createdTime", r.now());

        Insert qry = r.table(Tables.EXECUTIONS.name).insert(rMap);
        return conn.executeInsert("db-createExecutionStatus", qry, CrawlExecutionStatus.class);
    }

    @Test
    public void listCrawlExecutionStatus() throws DbException, InterruptedException {
        CrawlExecutionStatus ces1 = createCrawlExecutionStatus("jobId1", "jobExe1", "seedId1");
        CrawlExecutionStatus ces2 = createCrawlExecutionStatus("jobId1", "jobExe1", "seedId2");
        CrawlExecutionStatus ces3 = createCrawlExecutionStatus("jobId1", "jobExe2", "seedId1");

        // Check crawl executions list functions
        CrawlExecutionsListRequest.Builder request = CrawlExecutionsListRequest.newBuilder();
        ChangeFeed<CrawlExecutionStatus> eList = executionsAdapter.listCrawlExecutionStatus(request.build());
        assertThat(eList.stream())
                .hasSize(3)
                .containsExactlyInAnyOrder(ces1, ces2, ces3);

        request = CrawlExecutionsListRequest.newBuilder().addId(ces2.getId());
        eList = executionsAdapter.listCrawlExecutionStatus(request.build());
        assertThat(eList.stream())
                .hasSize(1)
                .containsExactlyInAnyOrder(ces2);

        request = CrawlExecutionsListRequest.newBuilder();
        request.getQueryTemplateBuilder().setJobExecutionId("jobExe1");
        request.getQueryMaskBuilder().addPaths("jobExecutionId");
        eList = executionsAdapter.listCrawlExecutionStatus(request.build());
        assertThat(eList.stream())
                .hasSize(2)
                .containsExactlyInAnyOrder(ces1, ces2);

        request = CrawlExecutionsListRequest.newBuilder();
        request.getQueryTemplateBuilder().setSeedId("seedId1");
        request.getQueryMaskBuilder().addPaths("seedId");
        eList = executionsAdapter.listCrawlExecutionStatus(request.build());
        assertThat(eList.stream())
                .hasSize(2)
                .containsExactlyInAnyOrder(ces1, ces3);

        request = CrawlExecutionsListRequest.newBuilder();
        request.getQueryTemplateBuilder().setJobExecutionId("jobExe1").setSeedId("seedId1");
        request.getQueryMaskBuilder().addPaths("jobExecutionId").addPaths("seedId");
        eList = executionsAdapter.listCrawlExecutionStatus(request.build());
        assertThat(eList.stream())
                .hasSize(1)
                .containsExactlyInAnyOrder(ces1);

        request = CrawlExecutionsListRequest.newBuilder();
        request.addState(CrawlExecutionStatus.State.CREATED);
        eList = executionsAdapter.listCrawlExecutionStatus(request.build());
        assertThat(eList.stream())
                .hasSize(3)
                .containsExactlyInAnyOrder(ces1, ces2, ces3);

        // Test watch query
        request = CrawlExecutionsListRequest.newBuilder().setWatch(true);
        ChangeFeed<CrawlExecutionStatus> feed = executionsAdapter.listCrawlExecutionStatus(request.build());
        new Thread(() -> {
            try {
                Thread.sleep(100);
                String id = createCrawlExecutionStatus("jobId1", "jobExe2", "seedId3").getId();
                Thread.sleep(100);
                executionsAdapter.setCrawlExecutionStateAborted(id, CrawlExecutionStatus.State.ABORTED_MANUAL);
                Thread.sleep(100);
                feed.stream().close();
            } catch (InterruptedException | DbException e) {
                e.printStackTrace();
            }
        }).start();
        assertThat(feed.stream())
                .hasSize(2)
                .allMatch(e -> e.getSeedId().equals("seedId3"));
    }

    @Test
    public void listJobExecutionStatus() throws DbException {
        JobExecutionStatus jes1 = executionsAdapter.createJobExecutionStatus("jobId1");
        jes1 = jes1.toBuilder()
                .putExecutionsState("UNDEFINED", 0)
                .putExecutionsState("CREATED", 0)
                .putExecutionsState("FETCHING", 0)
                .putExecutionsState("SLEEPING", 0)
                .putExecutionsState("FINISHED", 0)
                .putExecutionsState("ABORTED_TIMEOUT", 0)
                .putExecutionsState("ABORTED_SIZE", 0)
                .putExecutionsState("ABORTED_MANUAL", 0)
                .putExecutionsState("FAILED", 0)
                .putExecutionsState("DIED", 0)
                .putExecutionsState("UNRECOGNIZED", 0)
                .build();

        JobExecutionStatus jes2 = executionsAdapter.createJobExecutionStatus("jobId1");
        jes2 = executionsAdapter.setJobExecutionStateAborted(jes2.getId());
        jes2 = jes2.toBuilder()
                .putExecutionsState("UNDEFINED", 0)
                .putExecutionsState("CREATED", 0)
                .putExecutionsState("FETCHING", 0)
                .putExecutionsState("SLEEPING", 0)
                .putExecutionsState("FINISHED", 0)
                .putExecutionsState("ABORTED_TIMEOUT", 0)
                .putExecutionsState("ABORTED_SIZE", 0)
                .putExecutionsState("ABORTED_MANUAL", 0)
                .putExecutionsState("FAILED", 0)
                .putExecutionsState("DIED", 0)
                .putExecutionsState("UNRECOGNIZED", 0)
                .build();

        // Check job executions list functions
        ChangeFeed<JobExecutionStatus> jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.getDefaultInstance());
        assertThat(jList.stream()).hasSize(2).containsExactlyInAnyOrder(jes1, jes2);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().addId(jes2.getId()).build());
        assertThat(jList.stream()).hasSize(1).containsExactlyInAnyOrder(jes2);

        JobExecutionsListRequest.Builder req = JobExecutionsListRequest.newBuilder();
        req.getQueryTemplateBuilder().setState(State.RUNNING);
        req.getQueryMaskBuilder().addPaths("state");
        jList = executionsAdapter.listJobExecutionStatus(req.build());
        assertThat(jList.stream()).hasSize(1).containsExactlyInAnyOrder(jes1);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().addState(State.ABORTED_MANUAL).build());
        assertThat(jList.stream()).hasSize(1).containsExactlyInAnyOrder(jes2);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().addState(State.ABORTED_MANUAL).addState(State.RUNNING).build());
        assertThat(jList.stream()).hasSize(2).containsExactlyInAnyOrder(jes1, jes2);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().setStartTimeFrom(ProtoUtils.getNowTs()).build());
        assertThat(jList.stream()).hasSize(0);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().setStartTimeTo(ProtoUtils.getNowTs()).build());
        assertThat(jList.stream()).hasSize(2).containsExactlyInAnyOrder(jes1, jes2);
    }

    /**
     * Test of hasCrawledContent method, of class RethinkDbAdapter.
     */
    @Test
    public void testHasCrawledContent() throws DbException {
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

        assertThat(executionsAdapter.hasCrawledContent(cc1).isPresent()).isFalse();
        executionsAdapter.saveStorageRef(StorageRef.newBuilder()
                .setWarcId(cc1.getWarcId())
                .setRecordType(RecordType.REQUEST)
                .setStorageRef("warcfile:test:0")
                .build());

        Optional<CrawledContent> r2 = executionsAdapter.hasCrawledContent(cc2);
        assertThat(r2.isPresent()).isTrue();
        assertThat(r2.get()).isEqualTo(cc1);

        Optional<CrawledContent> r3 = executionsAdapter.hasCrawledContent(cc3);
        assertThat(r3.isPresent()).isTrue();
        assertThat(r3.get()).isEqualTo(cc1);

        CrawledContent cc4 = CrawledContent.newBuilder()
                .setWarcId("warc-id4")
                .build();

        assertThatThrownBy(() -> executionsAdapter.hasCrawledContent(cc4))
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

        executionsAdapter.hasCrawledContent(cc);
        executionsAdapter.deleteCrawledContent(cc.getDigest());
        executionsAdapter.deleteCrawledContent(cc.getDigest());
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
        CrawlLog result = executionsAdapter.saveCrawlLog(cl);
        assertThat(result.getContentType()).isEqualTo("text/plain");
        assertThat(result.getWarcId()).isNotEmpty();
    }

}
