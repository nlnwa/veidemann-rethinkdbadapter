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
package no.nb.nna.veidemann.db.initializer;

import com.google.gson.Gson;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConfigAdapter;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.stream.StreamSupport;

import static com.rethinkdb.RethinkDB.r;

public class PopulateDbWithTestData implements Runnable {
    RethinkDbConnection conn;

    @Override
    public void run() {
        conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
        populateDb();
    }

    private final void populateDb() {
        RethinkDbConfigAdapter db = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();

        readYamlFile("testdata-V0_1/schedule-configs.yaml", "config_crawl_schedule_configs");
        readYamlFile("testdata-V0_1/politeness-configs.yaml", "config_politeness_configs");
        readYamlFile("testdata-V0_1/browser-configs.yaml", "config_browser_configs");
        readYamlFile("testdata-V0_1/crawl-configs.yaml", "config_crawl_configs");
        readYamlFile("testdata-V0_1/browser-scripts.yaml", "config_browser_scripts");
        readYamlFile("testdata-V0_1/crawl-jobs.yaml", "config_crawl_jobs");
        readYamlFile("testdata-V0_1/seeds.yaml", "config_seeds");
        readYamlFile("testdata-V0_1/crawl-entities.yaml", "config_crawl_entities");
        readYamlFile("testdata-V0_1/rolemappings.yaml", "config_role_mappings");

        readJsonFile("testdata-V0_1/crawllogs.json", "crawl_log");
        readJsonFile("testdata-V0_1/crawled-content.json", "crawled_content");
    }

    void readYamlFile(String fileName, String tableName) {
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream(fileName)) {

            Yaml yaml = new Yaml();
            StreamSupport.stream(yaml.loadAll(in).spliterator(), false)
                    .forEach(o -> {
                        try {
                            Object c = conn.exec("db-save-" + tableName, r.table(tableName).insert(o));
                        } catch (DbException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    void readJsonFile(String fileName, String tableName) {
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream(fileName)) {

            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            Gson g = new Gson();
            String line = reader.readLine();
            do {
                Map m = g.fromJson(line, Map.class);
                Object c = conn.exec("db-save-" + tableName, r.table(tableName).insert(m));
                line = reader.readLine();
            } while (line != null);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
    }

    public <T extends Message> T saveMessage(T msg, String table) throws DbException {
        FieldDescriptor metaField = msg.getDescriptorForType().findFieldByName("meta");
        Map rMap = ProtoUtils.protoToRethink(msg);

        if (metaField == null) {
            return conn.executeInsert("db-save" + msg.getClass().getSimpleName(),
                    r.table(table)
                            .insert(rMap)
                            .optArg("conflict", "replace"),
                    (Class<T>) msg.getClass()
            );
        } else {
            // Check that name is set if this is a new object
            if (!rMap.containsKey("id") && (!rMap.containsKey("meta") || !((Map) rMap.get("meta")).containsKey("name"))) {
                throw new IllegalArgumentException("Trying to store a new " + msg.getClass().getSimpleName()
                        + " object, but meta.name is not set.");
            }

            rMap.put("meta", updateMeta((Map) rMap.get("meta")));

            return conn.executeInsert("db-save" + msg.getClass().getSimpleName(),
                    r.table(table)
                            .insert(rMap)
                            // A rethink function which copies created and createby from old doc,
                            // and copies name if not existent in new doc
                            .optArg("conflict", (id, old_doc, new_doc) -> new_doc.merge(
                                    r.hashMap("meta", r.hashMap()
                                            .with("name", r.branch(new_doc.g("meta").hasFields("name"),
                                                    new_doc.g("meta").g("name"), old_doc.g("meta").g("name")))
                                            .with("created", old_doc.g("meta").g("created"))
                                            .with("createdBy", old_doc.g("meta").g("createdBy"))
                                    ))),
                    (Class<T>) msg.getClass()
            );
        }
    }

    private Map updateMeta(Map meta) {
        if (meta == null) {
            meta = r.hashMap();
        }

        if (!meta.containsKey("created")) {
            meta.put("created", r.now());
            meta.put("createdBy", "anonymous");
        }

        meta.put("lastModified", r.now());
        meta.put("lastModifiedBy", "anonymous");

        return meta;
    }
}
