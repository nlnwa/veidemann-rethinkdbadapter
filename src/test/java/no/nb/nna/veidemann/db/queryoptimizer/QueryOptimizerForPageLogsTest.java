package no.nb.nna.veidemann.db.queryoptimizer;

import com.rethinkdb.ast.ReqlAst;
import no.nb.nna.veidemann.api.report.v1.PageLogListRequest;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.ListPageLogQueryBuilder;
import no.nb.nna.veidemann.db.RethinkAstDecompiler;
import org.junit.jupiter.api.Test;

import java.text.ParseException;

import static com.rethinkdb.RethinkDB.r;
import static org.assertj.core.api.Assertions.assertThat;

class QueryOptimizerForPageLogsTest {
    @Test
    public void testListAll() throws DbException {
        ReqlAst q;
        ReqlAst expected;
        PageLogListRequest.Builder req;

        req = PageLogListRequest.newBuilder();
        q = new ListPageLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("page_log");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListById() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        PageLogListRequest.Builder req;

        // Test list by one id
        req = PageLogListRequest.newBuilder()
                .addWarcId("id1");
        q = new ListPageLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("page_log").getAll("id1");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by two ids
        req = PageLogListRequest.newBuilder()
                .addWarcId("id1")
                .addWarcId("id2");
        q = new ListPageLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("page_log").getAll("id1", "id2");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByCrawlExecutionId() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        PageLogListRequest.Builder req;

        req = PageLogListRequest.newBuilder();
        req.getQueryTemplateBuilder()
                .setExecutionId("eid1");
        req.getQueryMaskBuilder().addPaths("executionId");
        q = new ListPageLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("page_log").getAll("eid1").optArg("index", "executionId");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by CrawlExecutionId with order by same field
        req = PageLogListRequest.newBuilder()
                .setOrderByPath("executionId");
        req.getQueryTemplateBuilder()
                .setExecutionId("eid1");
        req.getQueryMaskBuilder().addPaths("executionId");
        q = new ListPageLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("page_log").between("eid1", "eid1").optArg("index", "executionId")
                .optArg("right_bound", "closed").orderBy().optArg("index", "executionId");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by CrawlExecutionId with order by JobExecutionId
        req = PageLogListRequest.newBuilder()
                .setOrderByPath("jobExecutionId");
        req.getQueryTemplateBuilder()
                .setExecutionId("eid1");
        req.getQueryMaskBuilder().addPaths("executionId");
        q = new ListPageLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("page_log").getAll("eid1").optArg("index", "executionId")
                .orderBy(p1 -> p1.g("jobExecutionId"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListByJobExecutionId() throws ParseException {
        ReqlAst q;
        ReqlAst expected;
        PageLogListRequest.Builder req;

        req = PageLogListRequest.newBuilder();
        req.getQueryTemplateBuilder()
                .setJobExecutionId("jeid1");
        req.getQueryMaskBuilder().addPaths("jobExecutionId");
        q = new ListPageLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("page_log").filter(p1 -> p1.g("jobExecutionId").eq("jeid1"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list by JobExecutionId with order by same field
        req = PageLogListRequest.newBuilder()
                .setOrderByPath("jobExecutionId");
        req.getQueryTemplateBuilder()
                .setJobExecutionId("jeid1");
        req.getQueryMaskBuilder().addPaths("jobExecutionId");
        q = new ListPageLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("page_log").filter(p2 -> p2.g("jobExecutionId").eq("jeid1"))
                .orderBy(p1 -> p1.g("jobExecutionId"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));


        // Test list by JobExecutionId with order CrawlExecutionId
        req = PageLogListRequest.newBuilder()
                .setOrderByPath("executionId");
        req.getQueryTemplateBuilder()
                .setJobExecutionId("jeid1");
        req.getQueryMaskBuilder().addPaths("jobExecutionId");
        q = new ListPageLogQueryBuilder(req.build()).getListQuery();
        expected = r.table("page_log").orderBy().optArg("index", "executionId")
                .filter(p1 -> p1.g("jobExecutionId").eq("jeid1"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }
}