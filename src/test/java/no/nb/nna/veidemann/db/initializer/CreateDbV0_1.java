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

import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDbV0_1 implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateDbV0_1.class);

    static final RethinkDB r = RethinkDB.r;

    final RethinkDbConnection conn;

    final String dbName;

    public CreateDbV0_1(RethinkDbConnection conn, String dbName) {
        this.conn = conn;
        this.dbName = dbName;
    }

    @Override
    public void run() {
        try {
            createDb();
            new PopulateDbWithTestData().run();
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
    }

    private final void createDb() throws DbException {
        conn.exec("create-db", r.dbCreate(dbName));

        conn.exec(r.tableCreate(Tables.SYSTEM.name));
        conn.exec(r.table(Tables.SYSTEM.name).insert(r.hashMap("id", "db_version").with("db_version", "0.1")));
        conn.exec(r.table(Tables.SYSTEM.name).insert(r.hashMap("id", "log_levels")
                .with("logLevel",
                        r.array(r.hashMap("logger", "no.nb.nna.veidemann").with("level", "INFO"))
                )));

        conn.exec(r.tableCreate("crawl_log").optArg("primary_key", "warcId"));
        conn.exec(r.table("crawl_log")
                .indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp"))));
        conn.exec(r.table("crawl_log").indexCreate("executionId"));

        conn.exec(r.tableCreate("page_log").optArg("primary_key", "warcId"));
        conn.exec(r.table("page_log").indexCreate("executionId"));

        conn.exec(r.tableCreate(Tables.CRAWLED_CONTENT.name).optArg("primary_key", "digest"));

        conn.exec(r.tableCreate("extracted_text").optArg("primary_key", "warcId"));

        conn.exec(r.tableCreate("config_browser_scripts"));

        conn.exec(r.tableCreate(Tables.URI_QUEUE.name));
        conn.exec(r.table(Tables.URI_QUEUE.name).indexCreate("surt"));
        conn.exec(r.table(Tables.URI_QUEUE.name).indexCreate("executionId"));
        conn.exec(r.table(Tables.URI_QUEUE.name).indexCreate("crawlHostGroupKey_sequence_earliestFetch",
                uri -> r.array(uri.g("crawlHostGroupId"),
                        uri.g("politenessId"),
                        uri.g("sequence"),
                        uri.g("earliestFetchTimeStamp"))));

        conn.exec(r.tableCreate(Tables.EXECUTIONS.name));
        conn.exec(r.table(Tables.EXECUTIONS.name).indexCreate("startTime"));

        conn.exec(r.tableCreate(Tables.CRAWL_ENTITIES.name));

        conn.exec(r.tableCreate(Tables.SEEDS.name));
        conn.exec(r.table(Tables.SEEDS.name).indexCreate("jobId").optArg("multi", true));
        conn.exec(r.table(Tables.SEEDS.name).indexCreate("entityId"));

        conn.exec(r.tableCreate("config_crawl_jobs"));

        conn.exec(r.tableCreate("config_crawl_configs"));

        conn.exec(r.tableCreate("config_crawl_schedule_configs"));

        conn.exec(r.tableCreate("config_browser_configs"));

        conn.exec(r.tableCreate("config_politeness_configs"));

        conn.exec(r.tableCreate("config_crawl_host_group_configs"));

        conn.exec(r.tableCreate("crawl_host_group"));
        conn.exec(r.table("crawl_host_group").indexCreate("nextFetchTime"));

        conn.exec(r.tableCreate("already_crawled_cache")
                .optArg("durability", "soft")
                .optArg("shards", 3)
                .optArg("replicas", 1));

        conn.exec(r.tableCreate("config_role_mappings"));

        createMetaIndexes(
                "config_browser_scripts",
                Tables.CRAWL_ENTITIES.name,
                Tables.SEEDS.name,
                "config_crawl_jobs",
                "config_crawl_configs",
                "config_crawl_schedule_configs",
                "config_browser_configs",
                "config_politeness_configs",
                "config_crawl_host_group_configs"
        );

        conn.exec(r.table(Tables.URI_QUEUE.name)
                .indexWait("surt", "executionId", "crawlHostGroupKey_sequence_earliestFetch"));
        conn.exec(r.table("crawl_log").indexWait("surt_time", "executionId"));
        conn.exec(r.table("page_log").indexWait("executionId"));
        conn.exec(r.table(Tables.SEEDS.name).indexWait("jobId", "entityId"));
        conn.exec(r.table("crawl_host_group").indexWait("nextFetchTime"));
        conn.exec(r.table(Tables.EXECUTIONS.name).indexWait("startTime"));
    }

    private final void createMetaIndexes(String... tables) throws DbException {
        for (String table : tables) {
            conn.exec(r.table(table).indexCreate("name", row -> row.g("meta").g("name").downcase()));
            conn.exec(r.table(table)
                    .indexCreate("label",
                            row -> row.g("meta").g("label").map(
                                    label -> r.array(label.g("key").downcase(), label.g("value").downcase())))
                    .optArg("multi", true));
            conn.exec(r.table(table)
                    .indexCreate("label_value",
                            row -> row.g("meta").g("label").map(
                                    label -> label.g("value").downcase()))
                    .optArg("multi", true));
        }
        for (String table : tables) {
            conn.exec(r.table(table).indexWait("name", "label", "label_value"));
        }
    }
}
