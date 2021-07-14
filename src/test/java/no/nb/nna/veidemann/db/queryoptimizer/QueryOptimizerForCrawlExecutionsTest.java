package no.nb.nna.veidemann.db.queryoptimizer;

import com.google.protobuf.util.Timestamps;
import com.rethinkdb.ast.ReqlAst;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus.State;
import no.nb.nna.veidemann.api.report.v1.CrawlExecutionsListRequest;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.ListCrawlExecutionQueryBuilder;
import no.nb.nna.veidemann.db.RethinkAstDecompiler;
import org.junit.jupiter.api.Test;

import java.text.ParseException;

import static com.rethinkdb.RethinkDB.r;
import static org.assertj.core.api.Assertions.assertThat;

class QueryOptimizerForCrawlExecutionsTest {
    @Test
    public void testListCrawlExecutions() throws DbException {
        ReqlAst q;
        ReqlAst expected;
        CrawlExecutionsListRequest.Builder req;

        // Test list all
        req = CrawlExecutionsListRequest.newBuilder();
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListById() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        CrawlExecutionsListRequest.Builder req;

        // Test list by one id
        req = CrawlExecutionsListRequest.newBuilder()
                .addId("id1");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("id1");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by two ids
        req = CrawlExecutionsListRequest.newBuilder()
                .addId("id1")
                .addId("id2");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("id1", "id2");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by state and id
        req = CrawlExecutionsListRequest.newBuilder()
                .addId("id2")
                .addState(State.CREATED);
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("id2").filter(p1 -> p1.g("state").default_("UNDEFINED").eq("CREATED"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByState() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        CrawlExecutionsListRequest.Builder req;

        // Test list by state
        req = CrawlExecutionsListRequest.newBuilder()
                .addState(State.CREATED);
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("CREATED").optArg("index", "state");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by state and id
        req = CrawlExecutionsListRequest.newBuilder()
                .addState(State.CREATED)
                .addId("id2");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("id2").filter(p1 -> p1.g("state").default_("UNDEFINED").eq("CREATED"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by state and order by state
        req = CrawlExecutionsListRequest.newBuilder()
                .addState(State.CREATED)
                .setOrderByPath("state");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").between("CREATED", "CREATED")
                .optArg("index", "state").optArg("right_bound", "closed")
                .orderBy().optArg("index", "state");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByStartTime() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        CrawlExecutionsListRequest.Builder req;

        req = CrawlExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions")
                .between(r.iso8601("2020-12-02T09:53:36.406Z"), r.maxval())
                .optArg("index", "startTime");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = CrawlExecutionsListRequest.newBuilder()
                .setStartTimeTo(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions")
                .between(r.minval(), r.iso8601("2020-12-02T09:53:36.406Z"))
                .optArg("index", "startTime");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = CrawlExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"))
                .setStartTimeTo(Timestamps.parse("2020-12-03T09:53:36.406Z"));
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions")
                .between(r.iso8601("2020-12-02T09:53:36.406Z"), r.iso8601("2020-12-03T09:53:36.406Z"))
                .optArg("index", "startTime");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = CrawlExecutionsListRequest.newBuilder()
                .addState(State.CREATED)
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("CREATED").optArg("index", "state")
                .filter(p1 -> p1.g("startTime").ge(r.iso8601("2020-12-02T09:53:36.406Z")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = CrawlExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"))
                .addState(State.CREATED);
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("CREATED").optArg("index", "state")
                .filter(p1 -> p1.g("startTime").ge(r.iso8601("2020-12-02T09:53:36.406Z")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = CrawlExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        req.getQueryTemplateBuilder()
                .setJobId("jid1");
        req.getQueryMaskBuilder().addPaths("jobId");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("jid1").optArg("index", "jobId")
                .filter(p1 -> p1.g("startTime").ge(r.iso8601("2020-12-02T09:53:36.406Z")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = CrawlExecutionsListRequest.newBuilder()
                .setStartTimeTo(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        req.getQueryTemplateBuilder()
                .setJobId("jid1");
        req.getQueryMaskBuilder().addPaths("jobId");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("jid1").optArg("index", "jobId")
                .filter(p1 -> p1.g("startTime").lt(r.iso8601("2020-12-02T09:53:36.406Z")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByJobId() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        CrawlExecutionsListRequest.Builder req;

        req = CrawlExecutionsListRequest.newBuilder();
        req.getQueryTemplateBuilder()
                .setJobId("jid1");
        req.getQueryMaskBuilder().addPaths("jobId");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("jid1").optArg("index", "jobId");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by state and jobId
        req = CrawlExecutionsListRequest.newBuilder()
                .addState(State.CREATED);
        req.getQueryTemplateBuilder()
                .setJobId("jid1");
        req.getQueryMaskBuilder().addPaths("jobId");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("CREATED").optArg("index", "state")
                .filter(p1 -> p1.g("jobId").default_("").eq("jid1"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by startTime and jobId
        req = CrawlExecutionsListRequest.newBuilder()
                .setStartTimeFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"));
        req.getQueryTemplateBuilder()
                .setJobId("jid1");
        req.getQueryMaskBuilder().addPaths("jobId");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("jid1").optArg("index", "jobId")
                .filter(p1 -> p1.g("startTime").ge(r.iso8601("2020-12-02T09:53:36.406Z")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListBySeedId() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        CrawlExecutionsListRequest.Builder req;

        // Test list by seed id
        req = CrawlExecutionsListRequest.newBuilder();
        req.getQueryTemplateBuilder()
                .setSeedId("seed1");
        req.getQueryMaskBuilder().addPaths("seedId");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("seed1").optArg("index", "seedId");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by seed id and order by startTime
        req = CrawlExecutionsListRequest.newBuilder()
                .setOrderByPath("startTime");
        req.getQueryTemplateBuilder()
                .setSeedId("seed1");
        req.getQueryMaskBuilder().addPaths("seedId");
        q = new ListCrawlExecutionQueryBuilder(req.build()).getListQuery();
        expected = r.table("executions").getAll("seed1").optArg("index", "seedId")
                .orderBy(p1 -> p1.g("startTime"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }
}
