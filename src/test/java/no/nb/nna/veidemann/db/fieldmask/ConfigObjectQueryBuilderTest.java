/*
 * Copyright 2019 National Library of Norway.
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

package no.nb.nna.veidemann.db.fieldmask;

import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import no.nb.nna.veidemann.api.commons.v1.FieldMask;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.db.RethinkAstVisualizer;
import org.junit.Test;

import java.util.List;

import static com.rethinkdb.RethinkDB.r;
import static org.assertj.core.api.Assertions.assertThat;

public class ConfigObjectQueryBuilderTest {
    @Test
    public void createPluckQuery() {
        FieldMask m = FieldMask.newBuilder()
                .addPaths("meta.name")
                .addPaths("crawlConfig.extra.createScreenshot")
                .addPaths("meta")
                .addPaths("crawlConfig.priorityWeight")
                .build();

        ConfigObjectQueryBuilder queryBuilder = new ConfigObjectQueryBuilder(m);
        List q = queryBuilder.createPluckQuery();

        assertThat(q).containsExactlyInAnyOrder("apiVersion", "kind", "id", "meta",
                r.hashMap("crawlConfig", r.array(r.hashMap("extra", r.array("createScreenshot")), "priorityWeight")));
    }

    @Test
    public void buildFilterQuery() {
        FieldMask m = FieldMask.newBuilder()
                .addPaths("meta.name")
                .addPaths("crawlConfig.priorityWeight")
                .build();

        ConfigObject.Builder cob = ConfigObject.newBuilder();
        cob.getCrawlConfigBuilder().setPriorityWeight(2);
        cob.getCrawlConfigBuilder().getExtraBuilder().setCreateScreenshot(true);
        cob.getMetaBuilder().setName("foo");

        ConfigObjectQueryBuilder queryBuilder = new ConfigObjectQueryBuilder(m);

        ReqlFunction1 qry = queryBuilder.buildFilterQuery(cob);
        ReqlExpr o = r.table("table").filter(qry);

        RethinkAstVisualizer dc = new RethinkAstVisualizer(o);
        String decompiled = dc.toString();

        ReqlExpr expected = r.table("table").filter(row ->
                row.g("meta").g("name").eq("foo")
                        .and(row.g("crawlConfig").g("priorityWeight").eq(2.0))
        );
        String expectedDecompiled = new RethinkAstVisualizer(expected).toString();

        assertThat(decompiled).isEqualTo(expectedDecompiled);
    }

    @Test
    public void buildUpdateQuery() {
        FieldMask m = FieldMask.newBuilder()
                .addPaths("meta.name")
                .addPaths("crawlConfig.extra.createScreenshot")
                .addPaths("meta")
                .addPaths("crawlConfig.priorityWeight")
                .build();

        ConfigObject.Builder cob = ConfigObject.newBuilder();
        cob.getCrawlConfigBuilder().setPriorityWeight(2).build();

        ConfigObjectQueryBuilder queryBuilder = new ConfigObjectQueryBuilder(m);

        ReqlFunction1 qry = queryBuilder.buildUpdateQuery(cob);
        ReqlExpr o = r.table("table").insert(qry)
                .optArg("conflict", (id, old_doc, new_doc) -> r.branch(
                        old_doc.eq(new_doc),
                        old_doc,
                        new_doc.merge(
                                r.hashMap("meta", r.hashMap()
                                        .with("lastModified", r.now())
                                        .with("lastModifiedBy", "user")
                                ))));


        RethinkAstVisualizer dc = new RethinkAstVisualizer(o);
        String decompiled = dc.toString();

        ReqlExpr expected = r.table("table").insert((ReqlFunction1) row ->
                r.hashMap("crawlConfig", r.hashMap("priorityWeight", 2.0)
                        .with("extra", null))
                        .with("meta", r.hashMap("name", "")
                                .with("description", "")
                                .with("label", r.array())
                                .with("annotation", r.array()))
        ).optArg("conflict", (id, old_doc, new_doc) -> r.branch(
                old_doc.eq(new_doc),
                old_doc,
                new_doc.merge(
                        r.hashMap("meta", r.hashMap()
                                .with("lastModified", r.now())
                                .with("lastModifiedBy", "user")
                        ))));
        String expectedDecompiled = new RethinkAstVisualizer(expected).toString();

        assertThat(decompiled).isEqualTo(expectedDecompiled);
    }
}