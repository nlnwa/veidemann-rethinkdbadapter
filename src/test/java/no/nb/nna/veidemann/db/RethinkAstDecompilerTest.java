package no.nb.nna.veidemann.db;

import com.rethinkdb.ast.ReqlAst;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static com.rethinkdb.RethinkDB.r;
import static org.assertj.core.api.Assertions.assertThat;

class RethinkAstDecompilerTest {

    @Test
    void testToString() {
        ReqlAst ast = r.table("mintabell").get("foo").filter(p1 -> p1.eq("ugh")).orderBy("bar").group(p2 -> p2, p3 -> p3.filter(p4 -> p4.le(r.array(p4, "oo"))));
        String expected = "r.table(\"mintabell\").get(\"foo\").filter(p4 -> p4.eq(\"ugh\")).orderBy(\"bar\").group(p1 -> p1, p2 -> p2.filter(p3 -> p3.le(r.array(p3, \"oo\"))))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("mintabell").get("foo").filter(p1 -> p1.eq("ugh")).orderBy("bar").group(p2 -> p2, p3 -> p3.filter(p4 -> r.array(p4, "oo")));
        expected = "r.table(\"mintabell\").get(\"foo\").filter(p4 -> p4.eq(\"ugh\")).orderBy(\"bar\").group(p1 -> p1, p2 -> p2.filter(p3 -> r.array(p3, \"oo\")))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("mintabell").insert(r.hashMap("id", "myId").with("x", 4)).optArg("array", r.array("foo")).optArg("index", "myIndex").optArg("map", r.hashMap("aa", "bb")).optArg("test", true);
        expected = "r.table(\"mintabell\").insert(r.hashMap(\"id\", \"myId\").with(\"x\", 4)).optArg(\"array\", r.array(\"foo\")).optArg(\"index\", \"myIndex\").optArg(\"map\", r.hashMap(\"aa\", \"bb\")).optArg(\"test\", true)";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("mintabell").getAll("foo", "bar").filter((var1) -> var1.eq(3.14));
        expected = "r.table(\"mintabell\").getAll(\"foo\", \"bar\").filter(p1 -> p1.eq(3.14))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("mintabell").between(r.minval(), r.maxval());
        expected = "r.table(\"mintabell\").between(r.minval(), r.maxval())";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("mintabell").filter(p1 -> p1.g("foo").g("bar").eq("aa"));
        expected = "r.table(\"mintabell\").filter(p1 -> p1.g(\"foo\").g(\"bar\").eq(\"aa\"))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("mintabell").filter(p1 -> r.expr(r.array(r.hashMap("key", "foo").with("value", "bar"),
                r.hashMap("key", "aaa").with("value", "bbb"))).contains(p1.g("meta").g("label"), "f"));
        expected = "r.table(\"mintabell\").filter(p1 -> r.array(r.hashMap(\"key\", \"foo\").with(\"value\", \"bar\"), " +
                "r.hashMap(\"key\", \"aaa\").with(\"value\", \"bbb\")).contains(p1.g(\"meta\").g(\"label\"), \"f\"))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("config").between(r.array("crawlScheduleConfig", "home"), r.array("crawlScheduleConfig", "home")).optArg("index", "kind_label_key").filter(row -> row.g("meta").g("label").contains(r.hashMap("key", "home").with("value", "run")));
        expected = "r.table(\"config\").between(r.array(\"crawlScheduleConfig\", \"home\"), r.array(\"crawlScheduleConfig\", \"home\")).optArg(\"index\", \"kind_label_key\").filter(p1 -> p1.g(\"meta\").g(\"label\").contains(r.hashMap(\"key\", \"home\").with(\"value\", \"run\")))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("config").filter(p1 -> p1.eq("foo").or(p1.eq("bar")).and(p1.ne("aaa")));
        expected = "r.table(\"config\").filter(p1 -> p1.eq(\"foo\").or(p1.eq(\"bar\")).and(p1.ne(\"aaa\")))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("config").filter(p2 -> r.args(p2)).orderBy("foo", r.desc(p1 -> p1), r.array("a", "b"));
        expected = "r.table(\"config\").filter(p2 -> r.args(p2)).orderBy(\"foo\", r.desc(p1 -> p1), r.array(\"a\", \"b\"))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        ast = r.table("config").filter(p1 -> p1.g("kind").eq("crawlScheduleConfig")
                .and(r.expr(r.array(r.hashMap("key", "foo").with("value", "bar"), r.hashMap("key", "aaa").with("value", "bbb")))
                        .contains(p1.g("meta").g("label"))));
        expected = "r.table(\"config\").filter(p1 -> p1.g(\"kind\").eq(\"crawlScheduleConfig\")" +
                ".and(r.array(r.hashMap(\"key\", \"foo\").with(\"value\", \"bar\"), r.hashMap(\"key\", \"aaa\").with(\"value\", \"bbb\"))" +
                ".contains(p1.g(\"meta\").g(\"label\"))))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);

        String t = "2020-12-02T10:13:49.025+01";
        ast = r.table("mintabell").filter(p1 -> p1.g("foo").contains(r.now(), OffsetDateTime.parse(t),
                r.time(2020, 12, 2, 10, 13, 49, 25), r.iso8601(p1)));
        expected = "r.table(\"mintabell\").filter(p1 -> p1.g(\"foo\").contains(r.now(), " +
                "r.iso8601(\"2020-12-02T10:13:49.025+01\"), r.time(2020, 12, 2, 10, 13, 49, 25), r.iso8601(p1)))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);
    }

    @Test
    void testEquals() {
        ReqlAst ast1 = r.table("mintabell").getAll("foo", "bar").filter((var1) -> var1.eq(new String[]{"ugh"}));
        ReqlAst ast2 = r.table("mintabell").getAll("foo", "bar").filter((row) -> row.eq(r.array("ugh")));
        String expected = "r.table(\"mintabell\").getAll(\"foo\", \"bar\").filter(p1 -> p1.eq(r.array(\"ugh\")))";

        assertThat(new RethinkAstDecompiler(ast1)).isEqualTo(new RethinkAstDecompiler(ast2));
        assertThat(new RethinkAstDecompiler(ast1).toString()).isEqualTo(expected);
        assertThat(new RethinkAstDecompiler(ast2).toString()).isEqualTo(expected);
    }

    @Test
    void isEqual() {
        ReqlAst ast1 = r.table("mintabell").getAll("foo", "bar").filter((var1) -> var1.eq(new String[]{"ugh"}));
        ReqlAst ast2 = r.table("mintabell").getAll("foo", "bar").filter((row) -> row.eq(r.array("ugh")));
        assertThat(RethinkAstDecompiler.isEqual(ast1, ast2)).isTrue();
    }

    @Test
    void testDefault() {
        ReqlAst ast = r.table("mintabell").filter(p1 -> p1.g("foo").default_("").eq("foo"));
        String expected = "r.table(\"mintabell\").filter(p1 -> p1.g(\"foo\").default(\"\").eq(\"foo\"))";
        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);
    }

    @Test
    void testHasFields() {
        ReqlAst ast = r.table("mintabell").getAll("foo").filter(p1 -> p1.hasFields("foo"));
        String expected = "r.table(\"mintabell\").getAll(\"foo\").filter(p1 -> p1.hasFields(\"foo\"))";

        assertThat(new RethinkAstDecompiler(ast).toString()).isEqualTo(expected);
    }
}
