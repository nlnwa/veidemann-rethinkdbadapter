package no.nb.nna.veidemann.db.queryoptimizer;

import com.rethinkdb.ast.ReqlAst;
import no.nb.nna.veidemann.api.report.v1.CrawlLogListRequest;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.ListCrawlLogQueryBuilder;
import no.nb.nna.veidemann.db.RethinkAstDecompiler;
import org.junit.jupiter.api.Test;

import java.text.ParseException;

import static com.rethinkdb.RethinkDB.r;
import static org.assertj.core.api.Assertions.assertThat;

class QueryOptimizerForCrawlLogsTest {
    @Test
    public void testListCrawlExecutions() throws DbException {
        ReqlAst q;
        ReqlAst expected;
        CrawlLogListRequest.Builder req;

        // Test list all
        req = CrawlLogListRequest.newBuilder();
        q = new ListCrawlLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("crawl_log");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListById() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        CrawlLogListRequest.Builder req;

        // Test list by one id
        req = CrawlLogListRequest.newBuilder()
                .addWarcId("id1");
        q = new ListCrawlLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("crawl_log").getAll("id1");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by two ids
        req = CrawlLogListRequest.newBuilder()
                .addWarcId("id1")
                .addWarcId("id2");
        q = new ListCrawlLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("crawl_log").getAll("id1", "id2");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByCrawlExecutionId() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        CrawlLogListRequest.Builder req;

        req = CrawlLogListRequest.newBuilder();
        req.getQueryTemplateBuilder()
                .setExecutionId("eid1");
        req.getQueryMaskBuilder().addPaths("executionId");
        q = new ListCrawlLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("crawl_log").getAll("eid1").optArg("index", "executionId");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by CrawlExecutionId with order by same field
        req = CrawlLogListRequest.newBuilder()
                .setOrderByPath("executionId");
        req.getQueryTemplateBuilder()
                .setExecutionId("eid1");
        req.getQueryMaskBuilder().addPaths("executionId");
        q = new ListCrawlLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("crawl_log").between("eid1", "eid1").optArg("index", "executionId")
                .optArg("right_bound", "closed").distinct()
                .orderBy().optArg("index", "executionId");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by CrawlExecutionId with order by JobExecutionId
        req = CrawlLogListRequest.newBuilder()
                .setOrderByPath("jobExecutionId");
        req.getQueryTemplateBuilder()
                .setExecutionId("eid1");
        req.getQueryMaskBuilder().addPaths("executionId");
        q = new ListCrawlLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("crawl_log").getAll("eid1").optArg("index", "executionId")
                .orderBy(p1 -> p1.g("jobExecutionId"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByJobExecutionId() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        CrawlLogListRequest.Builder req;

        req = CrawlLogListRequest.newBuilder();
        req.getQueryTemplateBuilder()
                .setJobExecutionId("jeid1");
        req.getQueryMaskBuilder().addPaths("jobExecutionId");
        q = new ListCrawlLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("crawl_log").filter(p1 -> p1.g("jobExecutionId").eq("jeid1"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by JobExecutionId with order by same field
        req = CrawlLogListRequest.newBuilder()
                .setOrderByPath("jobExecutionId");
        req.getQueryTemplateBuilder()
                .setJobExecutionId("jeid1");
        req.getQueryMaskBuilder().addPaths("jobExecutionId");
        q = new ListCrawlLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("crawl_log").filter(p2 -> p2.g("jobExecutionId").eq("jeid1"))
                .orderBy(p1 -> p1.g("jobExecutionId"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));


        // Test list by JobExecutionId with order CrawlExecutionId
        req = CrawlLogListRequest.newBuilder()
                .setOrderByPath("executionId");
        req.getQueryTemplateBuilder()
                .setJobExecutionId("jeid1");
        req.getQueryMaskBuilder().addPaths("jobExecutionId");
        q = new ListCrawlLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("crawl_log").orderBy().optArg("index", "executionId")
                .filter(p1 -> p1.g("jobExecutionId").eq("jeid1"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }
}