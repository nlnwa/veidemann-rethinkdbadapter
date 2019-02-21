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
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateNewDb implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateNewDb.class);

    public static final String DB_VERSION = "1.2";

    static final RethinkDB r = RethinkDB.r;

    final RethinkDbConnection conn;

    final String dbName;

    public CreateNewDb(String dbName, RethinkDbConnection conn) {
        this.dbName = dbName;
        this.conn = conn;
    }

    @Override
    public void run() {
        try {
            createDb();
            createTables();
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
    }

    private final void createDb() throws DbQueryException, DbConnectionException {
        if (conn.exec(r.dbList().contains(dbName))) return;

        conn.exec("create-db", r.dbCreate(dbName));
    }

    private final void createTables() throws DbQueryException, DbConnectionException {
        createSystemTable();
        createConfigsTable();
        createLocksTable();
        createCrawlLogTable();
        createPageLogTable();
        createCrawledContentTable();
        createStorageRefTable();
        createExtractedTextTable();
        createUriQueueTable();
        createCrawlExecutionsTable();
        createJobExecutionsTable();
        createCrawlEntitiesTable();
        createSeedsTable();
        createCrawlHostGroupTable();
    }

    private boolean tableExists(String tableName) throws DbQueryException, DbConnectionException {
        return conn.exec(r.tableList().contains(tableName));
    }

    private void createSystemTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.SYSTEM.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);
        conn.exec(r.tableCreate(tableName));
        conn.exec(r.table(tableName).insert(r.hashMap("id", "db_version").with("db_version", DB_VERSION)));
        conn.exec(r.table(tableName).insert(r.hashMap("id", "log_levels")
                .with("logLevel",
                        r.array(r.hashMap("logger", "no.nb.nna.veidemann").with("level", "INFO"))
                )));
    }

    private void createConfigsTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.CONFIG.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName));
        conn.exec(r.table(tableName)
                .indexCreate("configRefs", row -> row
                        .g(Kind.browserConfig.name()).g("scriptRef").map(d -> r.array(d.g("kind"), d.g("id")))
                        .add(r.array(row.g(Kind.crawlJob.name()).g("scheduleRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                        .add(r.array(row.g(Kind.crawlJob.name()).g("crawlConfigRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                        .add(r.array(row.g(Kind.crawlConfig.name()).g("collectionRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                        .add(r.array(row.g(Kind.crawlConfig.name()).g("browserConfigRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                        .add(r.array(row.g(Kind.crawlConfig.name()).g("politenessRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                ).optArg("multi", true)
        );

        conn.exec(r.table(tableName).indexWait("configRefs"));
        createMetaIndexes(Tables.CONFIG);
    }

    private void createLocksTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.LOCKS.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName));
    }

    private void createCrawlHostGroupTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.CRAWL_HOST_GROUP.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName));
        conn.exec(r.table(tableName).indexCreate("nextFetchTime"));

        conn.exec(r.table(tableName).indexWait("nextFetchTime"));
    }

    private void createCrawlLogTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.CRAWL_LOG.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName).optArg("primary_key", "warcId"));
        conn.exec(r.table(tableName)
                .indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp"))));
        conn.exec(r.table(tableName).indexCreate("executionId"));

        conn.exec(r.table(tableName).indexWait("surt_time", "executionId"));
    }

    private void createPageLogTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.PAGE_LOG.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName).optArg("primary_key", "warcId"));
        conn.exec(r.table(tableName).indexCreate("executionId"));

        conn.exec(r.table(tableName).indexWait("executionId"));
    }

    private void createCrawledContentTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.CRAWLED_CONTENT.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName).optArg("primary_key", "digest"));
    }

    private void createStorageRefTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.STORAGE_REF.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName).optArg("primary_key", "warcId"));

    }

    private void createExtractedTextTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.EXTRACTED_TEXT.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName).optArg("primary_key", "warcId"));
    }

    private void createUriQueueTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.URI_QUEUE.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName));
        conn.exec(r.table(tableName).indexCreate("surt"));
        conn.exec(r.table(tableName).indexCreate("executionId"));
        conn.exec(r.table(tableName).indexCreate("crawlHostGroupKey_sequence_earliestFetch",
                uri -> r.array(uri.g("crawlHostGroupId"),
                        uri.g("politenessRef").g("id"),
                        uri.g("sequence"),
                        uri.g("earliestFetchTimeStamp"))));

        conn.exec(r.table(tableName)
                .indexWait("surt", "executionId", "crawlHostGroupKey_sequence_earliestFetch"));
    }

    private void createCrawlExecutionsTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.EXECUTIONS.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName));
        conn.exec(r.table(tableName).indexCreate("createdTime"));
        conn.exec(r.table(tableName).indexCreate("jobId"));
        conn.exec(r.table(tableName).indexCreate("state"));
        conn.exec(r.table(tableName).indexCreate("seedId"));
        conn.exec(r.table(tableName).indexCreate("jobExecutionId"));

        conn.exec(r.table(tableName).indexWait("createdTime", "jobId", "state", "seedId", "jobExecutionId"));
    }

    private void createJobExecutionsTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.JOB_EXECUTIONS.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName));
        conn.exec(r.table(tableName).indexCreate("startTime"));
        conn.exec(r.table(tableName).indexCreate("jobId"));
        conn.exec(r.table(tableName).indexCreate("state"));

        conn.exec(r.table(tableName).indexWait("startTime", "jobId", "state"));
    }

    private void createCrawlEntitiesTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.CRAWL_ENTITIES.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName));
        createMetaIndexes(Tables.CRAWL_ENTITIES);
    }

    private void createSeedsTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.SEEDS.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);

        conn.exec(r.tableCreate(tableName));
        conn.exec(r.table(tableName)
                .indexCreate("configRefs", row -> row
                        .g(Kind.seed.name()).g("jobRef").map(d -> r.array(d.g("kind"), d.g("id")))
                        .add(r.array(row.g(Kind.seed.name()).g("entityRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                ).optArg("multi", true)
        );
        conn.exec(r.table(tableName).indexWait("configRefs"));
        createMetaIndexes(Tables.SEEDS);
    }

    private final void createMetaIndexes(Tables... tables) throws DbQueryException, DbConnectionException {
        for (Tables table : tables) {
            conn.exec(r.table(table.name).indexCreate("name", row -> row.g("meta").g("name").downcase()));
            conn.exec(r.table(table.name)
                    .indexCreate("label",
                            row -> row.g("meta").g("label").map(
                                    label -> r.array(label.g("key").downcase(), label.g("value").downcase())))
                    .optArg("multi", true));
            conn.exec(r.table(table.name)
                    .indexCreate("kind_label_key",
                            row -> row.g("meta").g("label").map(
                                    label -> r.array(row.g("kind"), label.g("key").downcase())))
                    .optArg("multi", true));
            conn.exec(r.table(table.name)
                    .indexCreate("label_value",
                            row -> row.g("meta").g("label").map(
                                    label -> label.g("value").downcase()))
                    .optArg("multi", true));
            conn.exec(r.table(table.name).indexCreate("lastModified", row -> row.g("meta").g("lastModified")));
            conn.exec(r.table(table.name).indexCreate("lastModifiedBy", row -> row.g("meta").g("lastModifiedBy").downcase()));
        }
        for (Tables table : tables) {
            conn.exec(r.table(table.name).indexWait("name", "label", "kind_label_key", "label_value", "lastModified", "lastModifiedBy"));
        }
    }
}
