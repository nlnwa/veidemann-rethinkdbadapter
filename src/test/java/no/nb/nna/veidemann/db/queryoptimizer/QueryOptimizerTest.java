package no.nb.nna.veidemann.db.queryoptimizer;

import com.google.protobuf.util.Timestamps;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Javascript;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.ListConfigObjectQueryBuilder;
import no.nb.nna.veidemann.db.RethinkAstDecompiler;
import org.junit.jupiter.api.Test;

import java.text.ParseException;

import static com.rethinkdb.RethinkDB.r;
import static no.nb.nna.veidemann.api.config.v1.Kind.crawlScheduleConfig;
import static org.assertj.core.api.Assertions.assertThat;

class QueryOptimizerTest {
    @Test
    public void testFieldMask() throws DbException, ParseException {
        ReqlAst q;
        ReqlAst expected;

        // Test list by template filter
        ListRequest.Builder req = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);
        req.getQueryTemplateBuilder().getMetaBuilder()
                .setName("csc3")
                .setDescription("csc2desc");
        req.getQueryTemplateBuilder().getCrawlScheduleConfigBuilder().setValidFrom(Timestamps.parse("2020-12-02T09:53:36.406Z"));

        ListRequest.Builder lr = req.clone();
        lr.getQueryMaskBuilder().addPaths("meta.name");
        q = new ListConfigObjectQueryBuilder(lr.build()).getListQuery();
        expected = r.table("config").getAll("csc3").optArg("index", "name")
                .filter((p1) -> p1.getField("kind").default_("undefined").eq("crawlScheduleConfig"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        lr = req.clone();
        lr.getQueryMaskBuilder().addPaths("crawlScheduleConfig.validFrom");
        q = new ListConfigObjectQueryBuilder(lr.build()).getListQuery();
        expected = r.table("config").filter(p1 -> p1.g("crawlScheduleConfig").g("validFrom").default_((Javascript) null)
                .eq(r.iso8601("2020-12-02T09:53:36.406Z")).and(p1.g("kind").eq("crawlScheduleConfig")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }
}
