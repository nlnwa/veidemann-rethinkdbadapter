/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.db.initializer;

import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.contentwriter.v1.CrawledContent;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import no.nb.nna.veidemann.db.Tables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.rethinkdb.RethinkDB.r;
import static org.assertj.core.api.Assertions.assertThat;

public class DbInitializerTestIT {
    public static RethinkDbAdapter db;

    @Before
    public void init() throws DbException {
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

        db = (RethinkDbAdapter) DbService.getInstance().getDbAdapter();
    }

    @After
    public void shutdown() {
        DbService.getInstance().close();
    }

    @Test
    public void initialize() throws DbException {
        DbService.getInstance().getDbInitializer().initialize();

        String version = db.executeRequest("", r.table(Tables.SYSTEM.name).get("db_version").g("db_version"));
        assertThat(version).isEqualTo(CreateNewDb.DB_VERSION);

        long configObjectCount = db.executeRequest("", r.table(Tables.CONFIG.name).count());
        assertThat(configObjectCount).isGreaterThan(0);

        Map o = db.executeRequest("", r.table(Tables.CONFIG.name)
                .group("kind")
                .count()
                .ungroup()
                .map(doc -> r.array(doc.g("group").coerceTo("string"), doc.g("reduction")))
                .coerceTo("object")
        );
        assertThat(o.get("politenessConfig")).isEqualTo(1L);
        assertThat(o.get("browserScript")).isEqualTo(1L);
        assertThat(o.get("crawlJob")).isEqualTo(4L);
        assertThat(o.get("browserConfig")).isEqualTo(1L);
        assertThat(o.get("crawlConfig")).isEqualTo(1L);
        assertThat(o.get("crawlScheduleConfig")).isEqualTo(3L);

        try (Cursor<Map> configObjects = db.executeRequest("", r.table(Tables.CONFIG.name))) {
            assertThat(configObjects.iterator())
                    .hasSize(13)
                    .allSatisfy(r -> {
                        assertThat(r.get("apiVersion")).isEqualTo("v1");
                        assertThat(r).containsKey("kind");
                        assertThat(r).containsKey(r.get("kind"));
                        assertThat(r).containsKey("meta");
                        assertThat((Map) r.get("meta")).containsKey("name");
                    });
        }

        try (Cursor<Map> configObjects = db.executeRequest("", r.table(Tables.CONFIG.name)
                .filter(r.hashMap("kind", "browserConfig")))) {
            assertThat(configObjects.iterator())
                    .hasSize(1)
                    .allSatisfy(r -> {
                        assertThat(r.get("apiVersion")).isEqualTo("v1");
                        assertThat(r.get("kind")).isEqualTo("browserConfig");
                        assertThat(r).containsKey("browserConfig");
                        assertThat((Map) r.get("browserConfig")).containsEntry("maxInactivityTimeMs", 2000L);
                    });
        }
    }

    @Test
    public void upgrade() throws DbException {
        new CreateDbV0_1(db, "veidemann").run();
        DbService.getInstance().getDbInitializer().initialize();

        String version = db.executeRequest("", r.table(Tables.SYSTEM.name).get("db_version").g("db_version"));
        assertThat(version).isEqualTo(CreateNewDb.DB_VERSION);

        long configObjectCount = db.executeRequest("", r.table(Tables.CONFIG.name).count());
        assertThat(configObjectCount).isGreaterThan(0);

        Map o = db.executeRequest("", r.table(Tables.CONFIG.name)
                .group("kind")
                .count()
                .ungroup()
                .map(doc -> r.array(doc.g("group").coerceTo("string"), doc.g("reduction")))
                .coerceTo("object")
        );
        assertThat(o.get("politenessConfig")).isEqualTo(1L);
        assertThat(o.get("browserScript")).isEqualTo(1L);
        assertThat(o.get("crawlJob")).isEqualTo(4L);
        assertThat(o.get("browserConfig")).isEqualTo(1L);
        assertThat(o.get("crawlConfig")).isEqualTo(1L);
        assertThat(o.get("crawlScheduleConfig")).isEqualTo(3L);

        try (Cursor<Map> configObjects = db.executeRequest("", r.table(Tables.CONFIG.name))) {
            assertThat(configObjects.iterator())
                    .hasSize(13)
                    .allSatisfy(r -> {
                        assertThat(r.get("apiVersion")).isEqualTo("v1");
                        assertThat(r).containsKey("kind");
                        assertThat(r).containsKey(r.get("kind"));
                        assertThat(r).containsKey("meta");
                        assertThat((Map) r.get("meta")).containsKey("name");
                    });
        }

        try (Cursor<Map> configObjects = db.executeRequest("", r.table(Tables.SEEDS.name))) {
            assertThat(configObjects.iterator())
                    .hasSize(4)
                    .allSatisfy(r -> {
                        assertThat(r.get("apiVersion")).isEqualTo("v1");
                        assertThat(r.get("kind")).isEqualTo("seed");
                        assertThat(r).containsKey("seed");
                        assertThat(r).containsKey("meta");
                        assertThat((Map) r.get("meta")).containsKey("name");
                        checkConfigRef((Map) r.get("seed"), "entityRef", Kind.crawlEntity);
                        checkConfigRefList((Map) r.get("seed"), "jobRef", Kind.crawlJob);
                    });
        }
        try (Cursor<Map> configObjects = db.executeRequest("", r.table(Tables.SEEDS.name)
                .getAll(r.array(Kind.crawlEntity.name(), "d816019f-103e-44b8-aa3b-93cd727104c6"))
                .optArg("index", "configRefs"))) {
            assertThat(configObjects.iterator())
                    .hasSize(1)
                    .allSatisfy(r -> {
                        assertThat(r.get("apiVersion")).isEqualTo("v1");
                        assertThat(r.get("kind")).isEqualTo("seed");
                        assertThat(r).containsKey("seed");
                        assertThat(r).containsKey("meta");
                        assertThat((Map) r.get("meta")).containsKey("name");
                        checkConfigRef((Map) r.get("seed"), "entityRef", Kind.crawlEntity);
                        checkConfigRefList((Map) r.get("seed"), "jobRef", Kind.crawlJob);
                    });
        }

        try (Cursor<Map> configObjects = db.executeRequest("", r.table(Tables.CRAWL_ENTITIES.name))) {
            assertThat(configObjects.iterator())
                    .hasSize(4)
                    .allSatisfy(r -> {
                        assertThat(r.get("apiVersion")).isEqualTo("v1");
                        assertThat(r.get("kind")).isEqualTo("crawlEntity");
                        assertThat(r).containsKey("crawlEntity");
                        assertThat(r).containsKey("meta");
                        assertThat((Map) r.get("meta")).containsKey("name");
                    });
        }

        try (Cursor<Map> configObjects = db.executeRequest("", r.table(Tables.CONFIG.name)
                .filter(r.hashMap("kind", "browserConfig")))) {
            assertThat(configObjects.iterator())
                    .hasSize(1)
                    .allSatisfy(r -> {
                        assertThat(r.get("apiVersion")).isEqualTo("v1");
                        assertThat(r.get("kind")).isEqualTo("browserConfig");
                        assertThat(r).containsKey("browserConfig");
                        assertThat((Map) r.get("browserConfig")).containsEntry("maxInactivityTimeMs", 2000L);
                    });
        }

        try (Cursor<Map> configObjects = db.executeRequest("", r.table(Tables.CONFIG.name)
                .filter(r.hashMap("kind", "crawlConfig")))) {
            assertThat(configObjects.iterator())
                    .hasSize(1)
                    .allSatisfy(r -> {
                        assertThat(r.get("apiVersion")).isEqualTo("v1");
                        assertThat(r.get("kind")).isEqualTo("crawlConfig");
                        assertThat(r).containsKey("crawlConfig");
                        assertThat((Map) r.get("crawlConfig")).containsKey("extra");
                        assertThat(((Map<String, Map>) r.get("crawlConfig")).get("extra")).containsEntry("createScreenshot", true);
                    });
        }

        try (Cursor<Map> crawledContent = db.executeRequest("", r.table(Tables.CRAWLED_CONTENT.name))) {
            assertThat(crawledContent.iterator())
                    .hasSize(11)
                    .allSatisfy(r -> {
                        CrawledContent cc = ProtoUtils.rethinkToProto(r, CrawledContent.class);
                        assertThat(cc.getWarcId()).isNotEmpty();
                        assertThat(cc.getDigest()).isNotEmpty();

                        if (cc.getWarcId().equals("2e2b8f3d-c561-4643-97e9-55be42a42194")) {
                            // This is missing in test crawl log, so values should be empty
                            assertThat(!cc.hasDate());
                            assertThat(cc.getTargetUri()).isEmpty();
                        } else {
                            assertThat(cc.hasDate());
                            assertThat(cc.getTargetUri()).isNotEmpty();
                        }
                    });
        }
    }

    private void checkConfigRef(Map<String, ? extends Object> parent, String refName, Kind expectedKind) {
        assertThat(parent).containsKey(refName);
        assertThat(((Map<String, Map>) parent).get(refName).get("kind")).isEqualTo(expectedKind.name());
        assertThat(((Map<String, Map<String, String>>) parent).get(refName).get("id")).isNotEmpty();
    }

    private void checkConfigRefList(Map<String, ? extends Object> parent, String refName, Kind expectedKind) {
        assertThat(parent).containsKey(refName);
        List<Map<String, ? extends Object>> refs = (List<Map<String, ? extends Object>>) parent.get(refName);
        assertThat(refs).allSatisfy(ref -> {
            assertThat(((Map<String, String>) ref).get("kind")).isEqualTo(expectedKind.name());
            assertThat(((Map<String, String>) ref).get("id")).isNotEmpty();
        });
    }
}