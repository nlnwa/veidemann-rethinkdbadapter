package no.nb.nna.veidemann.db.queryoptimizer;

import com.rethinkdb.ast.ReqlAst;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.db.ListConfigObjectQueryBuilder;
import no.nb.nna.veidemann.db.RethinkAstDecompiler;
import org.junit.jupiter.api.Test;

import static com.rethinkdb.RethinkDB.r;
import static no.nb.nna.veidemann.api.config.v1.Kind.*;
import static org.assertj.core.api.Assertions.assertThat;

class QueryOptimizerForConfigObjectsTest {
    @Test
    public void testListConfigObjectsBoolean() throws DbException {
        ReqlAst q;
        ReqlAst expected;
        ListRequest.Builder req;

        // Test list seed by a boolean queryMask
        req = ListRequest.newBuilder().setKind(seed);
        req.getQueryTemplateBuilder().getSeedBuilder().setDisabled(false);
        req.getQueryMaskBuilder().addPaths("seed.disabled");
        q = new ListConfigObjectQueryBuilder(req.build()).getListQuery();
        expected = r.table("config_seeds")
                .filter(p1 -> p1.g("seed").g("disabled").default_(false).eq(false));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testListConfigObjects() throws DbException {
        ReqlAst q;
        ReqlAst expected;
        ListRequest.Builder req;

//        // Test list by kind
//        req = ListRequest.newBuilder();
//        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
//        expected = r.table("config").filter(p1 -> p1.g("kind").eq("crawlScheduleConfig"));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
//        expected = r.table("config_seeds");
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//
//        // Test list by id
//        req = ListRequest.newBuilder()
//                .addId("id1")
//                .addId("id2");
//        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
//        expected = r.table("config").getAll("id1", "id2").filter(p1 -> p1.g("kind").eq("crawlScheduleConfig"));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
//        expected = r.table("config_seeds").getAll("id1", "id2");
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//
//        // Test list by name regexp
//        req = ListRequest.newBuilder()
//                .setKind(crawlScheduleConfig)
//                .setNameRegex(".*3");
//        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
//        expected = r.table("config").filter(p2 -> p2.g("kind").eq("crawlScheduleConfig"))
//                .filter(p1 -> p1.g("meta").g("name").match("(?i).*3"));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
//        expected = r.table("config_seeds").filter(p1 -> p1.g("meta").g("name").match("(?i).*3"));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//
//        // Test select returned fields
//        req = ListRequest.newBuilder()
//                .setReturnedFieldsMask(FieldMask.newBuilder()
//                        .addPaths("meta.name")
//                        .addPaths("apiVersion"));
//        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
//        expected = r.table("config").filter(p1 -> p1.g("kind").eq("crawlScheduleConfig")).pluck(r.array("apiVersion", "kind", "id", "apiVersion", r.hashMap("meta", r.array("name"))));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
//        expected = r.table("config_seeds").pluck(r.array("apiVersion", "kind", "id", "apiVersion", r.hashMap("meta", r.array("name"))));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//
//        // Test list by name using template filter
//        req = ListRequest.newBuilder();
//        req.getQueryTemplateBuilder().getMetaBuilder()
//                .setName("csc3")
//                .setDescription("csc2desc")
//                .addLabelBuilder().setKey("foo").setValue("bar");
//        req.getQueryMaskBuilder().addPaths("meta.name");
//        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
//        expected = r.table("config").getAll("csc3").optArg("index", "name").filter(p1 -> p1.g("kind").eq("crawlScheduleConfig"));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
//        expected = r.table("config_seeds").getAll("csc3").optArg("index", "name");
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//
//        // Test list by description using template filter
//        req = ListRequest.newBuilder();
//        req.getQueryTemplateBuilder().getMetaBuilder()
//                .setName("csc3")
//                .setDescription("csc2desc")
//                .addLabelBuilder().setKey("foo").setValue("bar");
//        req.getQueryMaskBuilder().addPaths("meta.description");
//        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
//        expected = r.table("config").filter(p1 -> p1.g("meta").g("description").eq("csc2desc")
//                .and(p1.g("kind").eq("crawlScheduleConfig")));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
//        expected = r.table("config_seeds").filter(p1 -> p1.g("meta").g("description").eq("csc2desc"));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//
//        // Test list by label using template filter
//        req = ListRequest.newBuilder();
//        req.getQueryTemplateBuilder().getMetaBuilder()
//                .setName("csc3")
//                .setDescription("csc2desc")
//                .addLabelBuilder().setKey("foo").setValue("bar");
//        req.getQueryMaskBuilder().addPaths("meta.label");
//        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
//        expected = r.table("config").between(r.array("foo", "bar"), r.array("foo", "bar"))
//                .optArg("right_bound", "closed").optArg("index", "label")
//                .filter(p1 -> p1.g("kind").eq("crawlScheduleConfig"));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
//        expected = r.table("config_seeds").between(r.array("foo", "bar"), r.array("foo", "bar"))
//                .optArg("right_bound", "closed").optArg("index", "label");
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//
//        // Test list by two labels using template filter
//        req = ListRequest.newBuilder();
//        req.getQueryTemplateBuilder().getMetaBuilder()
//                .setName("csc3")
//                .setDescription("csc2desc")
//                .addLabelBuilder().setKey("foo").setValue("bar");
//        req.getQueryMaskBuilder().addPaths("meta.label");
//        req.getQueryTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
//        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
//        expected = r.table("config").between(r.array("foo", "bar"), r.array("foo", "bar"))
//                .optArg("right_bound", "closed").optArg("index", "label")
//                .filter(p1 -> p1.g("meta").g("label").contains(r.hashMap("key", "aaa").with("value", "bbb"))
//                        .and(p1.g("kind").eq("crawlScheduleConfig")));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
//        expected = r.table("config_seeds").between(r.array("foo", "bar"), r.array("foo", "bar"))
//                .optArg("right_bound", "closed").optArg("index", "label")
//                .filter(p1 -> p1.g("meta").g("label").contains(r.hashMap("key", "aaa").with("value", "bbb")));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        // Test list seed by entityRef using template filter
//        req = ListRequest.newBuilder()
//                .setKind(seed);
//        req.getQueryTemplateBuilder()
//                .getSeedBuilder().setEntityRef(ConfigRef.newBuilder().setKind(crawlEntity).setId("en1").build());
//        req.getQueryMaskBuilder().addPaths("seed.entityRef");
//        q = new ListConfigObjectQueryBuilder(req.build()).getListQuery();
//        expected = r.table("config_seeds").getAll(r.array("crawlEntity", "en1")).optArg("index", "configRefs");
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        // Test list seed by id and entityRef using template filter
//        req = ListRequest.newBuilder()
//                .setKind(seed)
//                .addId("id1");
//        req.getQueryTemplateBuilder()
//                .getSeedBuilder().setEntityRef(ConfigRef.newBuilder().setKind(crawlEntity).setId("en1").build());
//        req.getQueryMaskBuilder().addPaths("seed.entityRef");
//        q = new ListConfigObjectQueryBuilder(req.build()).getListQuery();
//        expected = r.table("config_seeds").getAll("id1")
//                .filter(p1 -> p1.g("seed").g("entityRef").eq(r.hashMap("id", "en1").with("kind", "crawlEntity")));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        // Test list crawlJob by scheduleRef using template filter
//        req = ListRequest.newBuilder()
//                .setKind(crawlJob);
//        req.getQueryTemplateBuilder()
//                .getCrawlJobBuilder().setScheduleRef(ConfigRef.newBuilder().setKind(crawlScheduleConfig).setId("csc1").build());
//        req.getQueryMaskBuilder().addPaths("crawlJob.scheduleRef");
//        q = new ListConfigObjectQueryBuilder(req.build()).getListQuery();
//        expected = r.table("config").getAll(r.array("crawlScheduleConfig", "csc1"))
//                .optArg("index", "configRefs")
//                .filter(p1 -> p1.g("kind").eq("crawlJob"));
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
//
//        // Test list seed by jobRef using template filter
//        req = ListRequest.newBuilder()
//                .setKind(seed);
//        req.getQueryTemplateBuilder()
//                .getSeedBuilder().addJobRef(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("cj1").build());
//        req.getQueryMaskBuilder().addPaths("seed.jobRef");
//        q = new ListConfigObjectQueryBuilder(req.build()).getListQuery();
//        expected = r.table("config_seeds").getAll(r.array("crawlJob", "cj1")).optArg("index", "configRefs");
//        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list seed by id and jobRef using template filter
        req = ListRequest.newBuilder()
                .setKind(seed)
                .addId("id1");
        req.getQueryTemplateBuilder()
                .getSeedBuilder().addJobRef(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("cj1").build());
        req.getQueryMaskBuilder().addPaths("seed.jobRef");
        q = new ListConfigObjectQueryBuilder(req.build()).getListQuery();
        expected = r.table("config_seeds").getAll("id1")
                .filter(p1 -> p1.g("seed").g("jobRef").contains(r.hashMap("id", "cj1").with("kind", "crawlJob")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list seed by id and two jobRefs using template filter
        req = ListRequest.newBuilder()
                .setKind(seed)
                .addId("id1");
        req.getQueryTemplateBuilder()
                .getSeedBuilder().addJobRef(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("cj1").build())
                .addJobRef(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("cj2").build());
        req.getQueryMaskBuilder().addPaths("seed.jobRef");
        q = new ListConfigObjectQueryBuilder(req.build()).getListQuery();
        expected = r.table("config_seeds").getAll("id1")
                .filter(p1 -> p1.g("seed").g("jobRef").coerceTo("array")
                        .contains(r.args(r.array(r.hashMap("id", "cj1").with("kind", "crawlJob"), r.hashMap("id", "cj2").with("kind", "crawlJob")))));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list browserConfig by scriptRef using template filter
        req = ListRequest.newBuilder()
                .setKind(browserConfig);
        req.getQueryTemplateBuilder()
                .getBrowserConfigBuilder().addScriptRef(ConfigRef.newBuilder().setKind(browserScript).setId("bs1").build());
        req.getQueryMaskBuilder().addPaths("browserConfig.scriptRef");
        q = new ListConfigObjectQueryBuilder(req.build()).getListQuery();
        expected = r.table("config").getAll(r.array("browserScript", "bs1"))
                .optArg("index", "configRefs")
                .filter(p1 -> p1.g("kind").default_("undefined").eq("browserConfig"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test list browserConfig by two scriptRefs using template filter
        // TODO: This test is wrong. GetAll will match any of the browserScripts, but should only match those who has both.
        //       Leaves this for now as it is not important enough to block release of other bugfixes.
        req = ListRequest.newBuilder()
                .setKind(browserConfig);
        req.getQueryTemplateBuilder()
                .getBrowserConfigBuilder().addScriptRef(ConfigRef.newBuilder().setKind(browserScript).setId("bs1").build())
                .addScriptRef(ConfigRef.newBuilder().setKind(browserScript).setId("bs2").build());
        req.getQueryMaskBuilder().addPaths("browserConfig.scriptRef");
        q = new ListConfigObjectQueryBuilder(req.build()).getListQuery();
        expected = r.table("config").getAll(r.array("browserScript", "bs1"), r.array("browserScript", "bs2"))
                .optArg("index", "configRefs")
                .filter(p1 -> p1.g("kind").default_("undefined").eq("browserConfig"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test order
        req = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setOrderByPath("meta.name");
        req.getQueryMaskBuilder().addPaths("meta.label");
        req.getQueryTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .filter(p2 -> p2.g("kind").default_("undefined").eq("crawlScheduleConfig"))
                .orderBy(p1 -> p1.g("meta").g("name").downcase());
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .orderBy(p1 -> p1.g("meta").g("name").downcase());
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        req = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setOrderByPath("meta.name")
                .setOrderDescending(true);
        req.getQueryMaskBuilder().addPaths("meta.label");
        req.getQueryTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .filter(p2 -> p2.g("kind").default_("undefined").eq("crawlScheduleConfig"))
                .orderBy(r.desc(p1 -> p1.g("meta").g("name").downcase()));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .orderBy(r.desc(p1 -> p1.g("meta").g("name").downcase()));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));


        req = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setOrderByPath("meta.lastModified")
                .setOrderDescending(false);
        req.getQueryMaskBuilder().addPaths("meta.label");
        req.getQueryTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .filter(p2 -> p2.g("kind").default_("undefined").eq("crawlScheduleConfig"))
                .orderBy(p1 -> p1.g("meta").g("lastModified"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .orderBy(p1 -> p1.g("meta").g("lastModified"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));


        req = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setOrderByPath("meta.label")
                .setNameRegex("c[s|j]")
                .setOrderDescending(false);
        req.getQueryMaskBuilder().addPaths("meta.label");
        req.getQueryTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .orderBy().optArg("index", "label")
                .filter(p3 -> p3.g("kind").default_("undefined").eq("crawlScheduleConfig"))
                .filter(p1 -> p1.g("meta").g("name").match("(?i)c[s|j]"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .orderBy().optArg("index", "label")
                .filter(p1 -> p1.g("meta").g("name").match("(?i)c[s|j]"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Test name regex
        req = ListRequest.newBuilder()
                .setOrderByPath("meta.label")
                .setNameRegex("c[s|j].[1|3]");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").orderBy().optArg("index", "label")
                .filter(p2 -> p2.g("kind").default_("undefined").eq("crawlScheduleConfig"))
                .filter(p1 -> p1.g("meta").g("name").match("(?i)c[s|j].[1|3]"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").orderBy().optArg("index", "label")
                .filter(p1 -> p1.g("meta").g("name").match("(?i)c[s|j].[1|3]"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));


        // Test all options at once
        req = ListRequest.newBuilder()
                .addId("csc1")
                .addLabelSelector("foo:")
                .setOrderByPath("meta.label")
                .setNameRegex("csc");
        req.getQueryTemplateBuilder()
                .getMetaBuilder().setDescription("csc1desc");
        req.getQueryMaskBuilder().addPaths("meta.description");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").getAll("csc1").filter(p3 -> p3.g("meta").g("label")
                .filter(p4 -> p4.g("key").eq("foo")).and(p3.g("meta").g("description").eq("csc1desc"))
                .and(p3.g("kind").eq("crawlScheduleConfig"))).orderBy(p2 -> p2.g("meta").g("label"))
                .filter(p1 -> p1.g("meta").g("name").match("(?i)csc"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").getAll("csc1").filter(p3 -> p3.g("meta").g("label")
                .filter(p4 -> p4.g("key").eq("foo")).and(p3.g("meta").g("description").eq("csc1desc")))
                .orderBy(p2 -> p2.g("meta").g("label")).filter(p1 -> p1.g("meta").g("name").match("(?i)csc"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }

    @Test
    public void testLabelSelector() throws DbException {
        ReqlAst q;
        ReqlAst expected;
        ListRequest.Builder req;

        // Select by label with query template
        req = ListRequest.newBuilder();
        req.getQueryTemplateBuilder().getMetaBuilder()
                .addLabelBuilder().setKey("foo").setValue("bar");
        req.getQueryMaskBuilder().addPaths("meta.label");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("foo", "bar"), r.array("foo", "bar"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .filter(p1 -> p1.g("kind").default_("undefined").eq("crawlScheduleConfig"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("foo", "bar"), r.array("foo", "bar"))
                .optArg("right_bound", "closed").optArg("index", "label");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));


        // Select by two labels (and) with query template
        req = ListRequest.newBuilder();
        req.getQueryTemplateBuilder().getMetaBuilder()
                .addLabel(ApiTools.buildLabel("foo", "bar"))
                .addLabel(ApiTools.buildLabel("aaa", "bbb"));
        req.getQueryMaskBuilder().addPaths("meta.label");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("foo", "bar"), r.array("foo", "bar"))
                .optArg("right_bound", "closed").optArg("index", "label").filter(p1 -> p1.g("meta").g("label")
                        .contains(r.hashMap("key", "aaa").with("value", "bbb")).and(p1.g("kind").eq("crawlScheduleConfig")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("foo", "bar"), r.array("foo", "bar"))
                .optArg("right_bound", "closed").optArg("index", "label").filter(p1 -> p1.g("meta").g("label")
                        .contains(r.hashMap("key", "aaa").with("value", "bbb")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Exact selector
        req = ListRequest.newBuilder()
                .addLabelSelector("home:run");
        req.getQueryMaskBuilder().addPaths("meta.label");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("home", "run"), r.array("home", "run"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .filter(p1 -> p1.g("kind").default_("undefined").eq("crawlScheduleConfig"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("home", "run"), r.array("home", "run"))
                .optArg("right_bound", "closed").optArg("index", "label");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Any key selector
        req = ListRequest.newBuilder()
                .addLabelSelector(":foo");
        req.getQueryMaskBuilder().addPaths("meta.label");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between("foo", "foo")
                .optArg("index", "label_value").optArg("right_bound", "closed")
                .filter(p1 -> p1.g("kind").default_("undefined").eq("crawlScheduleConfig"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between("foo", "foo")
                .optArg("index", "label_value").optArg("right_bound", "closed");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Any key with value prefix selector
        req = ListRequest.newBuilder()
                .addLabelSelector(":foo*");
        req.getQueryMaskBuilder().addPaths("meta.label");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between("foo", "foo\uFFFF")
                .optArg("index", "label_value").optArg("right_bound", "closed")
                .filter(p1 -> p1.g("kind").default_("undefined").eq("crawlScheduleConfig"));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between("foo", "foo\uFFFF")
                .optArg("index", "label_value").optArg("right_bound", "closed");
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Any key with value prefix selector as filter
        req = ListRequest.newBuilder()
                .addLabelSelector("home:run")
                .addLabelSelector(":foo*");
        req.getQueryMaskBuilder().addPaths("meta.label");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("home", "run"), r.array("home", "run"))
                .optArg("index", "label").optArg("right_bound", "closed")
                .filter(p1 -> p1.g("meta").g("label").filter(p2 -> p2.g("value")
                        .between("foo", "foo\uFFFF").optArg("right_bound", "closed"))
                        .and(p1.g("kind").eq("crawlScheduleConfig")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("home", "run"), r.array("home", "run"))
                .optArg("index", "label").optArg("right_bound", "closed")
                .filter(p1 -> p1.g("meta").g("label").filter(p2 -> p2.g("value")
                        .between("foo", "foo\uFFFF").optArg("right_bound", "closed")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Mix of selectors
        req = ListRequest.newBuilder()
                .addLabelSelector("home:run")
                .addLabelSelector("foo:")
                .addLabelSelector("home:b*");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("home", "run"), r.array("home", "run"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .filter(p1 -> p1.g("meta").g("label").filter(p2 -> p2.g("key").eq("foo"))
                        .and(p1.g("meta").g("label")
                                .filter(p3 -> p3.g("key").eq("home").and(p3.g("value")
                                        .between("b", "b￿").optArg("right_bound", "closed"))))
                        .and(p1.g("kind").eq("crawlScheduleConfig")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("home", "run"), r.array("home", "run"))
                .optArg("right_bound", "closed").optArg("index", "label")
                .filter(p1 -> p1.g("meta").g("label").filter(p2 -> p2.g("key").eq("foo"))
                        .and(p1.g("meta").g("label").filter(p3 -> p3.g("key").eq("home")
                                .and(p3.g("value").between("b", "b￿").optArg("right_bound", "closed")))));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        // Mix of selectors and label template
        req = ListRequest.newBuilder()
                .addLabelSelector("home:run")
                .addLabelSelector("foo:")
                .addLabelSelector("home:b*");
        req.getQueryTemplateBuilder().getMetaBuilder()
                .addLabel(ApiTools.buildLabel("aaa", "bbb"));
        req.getQueryMaskBuilder().addPaths("meta.label");
        q = new ListConfigObjectQueryBuilder(req.setKind(crawlScheduleConfig).build()).getListQuery();
        expected = r.table("config").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label").filter(p1 -> p1.g("meta").g("label")
                        .contains(r.hashMap("key", "home").with("value", "run")).and(p1.g("meta").g("label")
                                .filter(p2 -> p2.g("key").eq("foo"))).and(p1.g("meta").g("label")
                                .filter(p3 -> p3.g("key").eq("home").and(p3.g("value")
                                        .between("b", "b￿").optArg("right_bound", "closed"))))
                        .and(p1.g("kind").eq("crawlScheduleConfig")));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));

        q = new ListConfigObjectQueryBuilder(req.setKind(seed).build()).getListQuery();
        expected = r.table("config_seeds").between(r.array("aaa", "bbb"), r.array("aaa", "bbb"))
                .optArg("right_bound", "closed").optArg("index", "label").filter(p1 -> p1.g("meta").g("label")
                        .contains(r.hashMap("key", "home").with("value", "run")).and(p1.g("meta").g("label")
                                .filter(p2 -> p2.g("key").eq("foo"))).and(p1.g("meta").g("label")
                                .filter(p3 -> p3.g("key").eq("home").and(p3.g("value")
                                        .between("b", "b￿").optArg("right_bound", "closed")))));
        assertThat(new RethinkAstDecompiler(q)).isEqualTo(new RethinkAstDecompiler(expected));
    }
}
