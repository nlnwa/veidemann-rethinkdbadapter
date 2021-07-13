package no.nb.nna.veidemann.db.queryoptimizer;

import com.google.protobuf.util.Timestamps;
import com.rethinkdb.ast.ReqlAst;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.api.report.v1.JobExecutionsListRequest;
import no.nb.nna.veidemann.db.ListJobExecutionQueryBuilder;
import no.nb.nna.veidemann.db.RethinkAstDecompiler;
import org.junit.jupiter.api.Test;

import java.text.ParseException;

import static com.rethinkdb.RethinkDB.r;
import static org.assertj.core.api.Assertions.assertThat;

class QueryOptimizerForJobExecutionsTest {
    @Test
    public void testListAll() throws ParseException {
        // Test list all
        JobExecutionsListRequest.Builder req = JobExecutionsListRequest.newBuilder();
        ReqlAst q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        ReqlAst expected = r.table("job_executions");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListById() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        JobExecutionsListRequest.Builder req;

        // Test list by one id
        req = JobExecutionsListRequest.newBuilder()
                .addId("id1");
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("id1");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by two ids
        req = JobExecutionsListRequest.newBuilder()
                .addId("id1")
                .addId("id2");
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("id1", "id2");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by state and id
        req = JobExecutionsListRequest.newBuilder()
                .addId("id2")
                .addState(State.CREATED);
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("id2").filter(p1 -> p1.g("state").default_("UNDEFINED").eq("CREATED"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByState() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        JobExecutionsListRequest.Builder req;

        // Test list by state
        req = JobExecutionsListRequest.newBuilder()
                .addState(State.CREATED);
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("CREATED").optArg("index", "state");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by state and id
        req = JobExecutionsListRequest.newBuilder()
                .addState(State.CREATED)
                .addId("id2");
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("id2").filter(p1 -> p1.g("state").default_("UNDEFINED").eq("CREATED"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by state and order by state
        req = JobExecutionsListRequest.newBuilder()
                .addState(State.RUNNING)
                .setOrderByPath("state");
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions")
                .between("RUNNING", "RUNNING").optArg("index", "state").optArg("right_bound", "closed")
                .orderBy().optArg("index", "state");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by two states and order by state
        req = JobExecutionsListRequest.newBuilder()
                .addState(State.CREATED)
                .addState(State.RUNNING)
                .setOrderByPath("state");
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("CREATED", "RUNNING").optArg("index", "state")
                .orderBy(p1 -> p1.g("state"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByStartTime() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        JobExecutionsListRequest.Builder req;

        req = JobExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions")
                .between(r.iso8601("2020-12-02T09:53:36.406Z"), r.maxval())
                .optArg("index", "startTime");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = JobExecutionsListRequest.newBuilder()
                .setStartTimeTo(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions")
                .between(r.minval(), r.iso8601("2020-12-02T09:53:36.406Z"))
                .optArg("index", "startTime");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = JobExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"))
                .setStartTimeTo(Timestamps.parse("2020-12-03T09:53:36.406Z"));
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions")
                .between(r.iso8601("2020-12-02T09:53:36.406Z"), r.iso8601("2020-12-03T09:53:36.406Z"))
                .optArg("index", "startTime");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = JobExecutionsListRequest.newBuilder()
                .addState(State.CREATED)
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("CREATED").optArg("index", "state")
                .filter(p1 -> p1.g("startTime").ge(r.iso8601("2020-12-02T09:53:36.406Z")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = JobExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"))
                .addState(State.CREATED);
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("CREATED").optArg("index", "state")
                .filter(p1 -> p1.g("startTime").ge(r.iso8601("2020-12-02T09:53:36.406Z")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByJobId() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        JobExecutionsListRequest.Builder req;

        req = JobExecutionsListRequest.newBuilder();
        req.getQueryTemplateBuilder()
                .setJobId("jid1");
        req.getQueryMaskBuilder().addPaths("jobId");
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("jid1").optArg("index", "jobId");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by state and jobId
        req = JobExecutionsListRequest.newBuilder()
                .addState(State.CREATED);
        req.getQueryTemplateBuilder()
                .setJobId("jid1");
        req.getQueryMaskBuilder().addPaths("jobId");
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").getAll("CREATED").optArg("index", "state")
                .filter(p1 -> p1.g("jobId").default_("").eq("jid1"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by startTime and jobId
        req = JobExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        req.getQueryTemplateBuilder()
                .setJobId("jid1");
        req.getQueryMaskBuilder().addPaths("jobId");
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").between(r.array("jid1", r.iso8601("2020-12-02T09:53:36.406Z")), r.array("jid1", r.maxval()))
                .optArg("index", "jobId_startTime").optArg("right_bound", "closed");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by jobId and order by startTime
        req = JobExecutionsListRequest.newBuilder()
                .setOrderByPath("startTime")
                .setOrderDescending(true)
                .setPageSize(1);
        req.getQueryTemplateBuilder()
                .setJobId("jid2");
        req.getQueryMaskBuilder().addPaths("jobId");
        q = new ListJobExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("job_executions").between(r.array("jid2", r.minval()), r.array("jid2", r.maxval()))
                .optArg("index", "jobId_startTime").optArg("right_bound", "closed")
                .orderBy().optArg("index", r.desc("jobId_startTime")).skip(0).limit(1);
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }
}
