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

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.Update;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
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

import java.text.ParseException;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

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

    private CrawlExecutionStatus setStartStatus(CrawlExecutionStatus ces, Timestamp startTime) throws DbException {
        CrawlExecutionStatus status = ces.toBuilder()
                .setState(CrawlExecutionStatus.State.SLEEPING)
                .setStartTime(startTime)
                .setLastChangeTime(ProtoUtils.getNowTs())
                .build();

        Map rMap = ProtoUtils.protoToRethink(status);

        Update qry = r.table(Tables.EXECUTIONS.name).get(ces.getId()).update(rMap);
        return conn.executeUpdate("db-updateExecutionStatus", qry, CrawlExecutionStatus.class);
    }

    @Test
    public void listCrawlExecutionStatus() throws DbException, InterruptedException, ParseException {
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

        request = CrawlExecutionsListRequest.newBuilder()
                .setOrderByPath("startTime");
        request.getQueryTemplateBuilder()
                .setSeedId("seedId1");
        request.getQueryMaskBuilder().addPaths("seedId");
        eList = executionsAdapter.listCrawlExecutionStatus(request.build());
        assertThat(eList.stream())
                .hasSize(2)
                .containsExactlyInAnyOrder(ces1, ces3);

        ces1 = setStartStatus(ces1, Timestamps.parse("2020-12-03T09:53:36.406Z"));
        ces3 = setStartStatus(ces3, Timestamps.parse("2020-12-01T09:53:36.406Z"));
        request = CrawlExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        request.getQueryTemplateBuilder()
                .setJobId("jobId1");
        request.getQueryMaskBuilder().addPaths("jobId");
        eList = executionsAdapter.listCrawlExecutionStatus(request.build());
        assertThat(eList.stream())
                .hasSize(1)
                .containsExactlyInAnyOrder(ces1);

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
        jes1 = jes1.toBuilder().build();

        JobExecutionStatus jes2 = executionsAdapter.createJobExecutionStatus("jobId1");
        jes2 = executionsAdapter.setJobExecutionStateAborted(jes2.getId());
        jes2 = jes2.toBuilder().build();

        // Check job executions list functions
        ChangeFeed<JobExecutionStatus> jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.getDefaultInstance());
        assertThat(jList.stream()).hasSize(2).containsExactlyInAnyOrder(jes1, jes2);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().addId(jes2.getId()).build());
        assertThat(jList.stream()).hasSize(1).containsExactlyInAnyOrder(jes2);

        JobExecutionsListRequest.Builder req = JobExecutionsListRequest.newBuilder();
        req.getQueryTemplateBuilder().setState(State.RUNNING);
        req.getQueryMaskBuilder().addPaths("state");
        jList = executionsAdapter.listJobExecutionStatus(req.build());
        assertThat(jList.stream()).hasSize(2).containsExactlyInAnyOrder(jes1, jes2);
        assertThat(jes2.getDesiredState()).isEqualTo(State.ABORTED_MANUAL);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().addState(State.ABORTED_MANUAL).build());
        assertThat(jList.stream()).hasSize(0);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().addState(State.ABORTED_MANUAL).addState(State.RUNNING).build());
        assertThat(jList.stream()).hasSize(2).containsExactlyInAnyOrder(jes1, jes2);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().setStartTimeFrom(ProtoUtils.getNowTs()).build());
        assertThat(jList.stream()).hasSize(0);

        jList = executionsAdapter.listJobExecutionStatus(JobExecutionsListRequest.newBuilder().setStartTimeTo(ProtoUtils.getNowTs()).build());
        assertThat(jList.stream()).hasSize(2).containsExactlyInAnyOrder(jes1, jes2);
    }
}
