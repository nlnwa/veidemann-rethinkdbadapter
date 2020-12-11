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

import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateNewDb extends TableCreator implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateNewDb.class);

    public static final String DB_VERSION = "1.12";

    public CreateNewDb(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
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
        createCrawlLogTable();
        createPageLogTable();
        createCrawledContentTable();
        createStorageRefTable();
        createUriQueueTable();
        createCrawlExecutionsTable();
        createJobExecutionsTable();
        createCrawlEntitiesTable();
        createSeedsTable();
        createEventTable();

        waitForIndexes();
    }

    private void createSystemTable() throws DbQueryException, DbConnectionException {
        String tableName = Tables.SYSTEM.name;
        if (tableExists(tableName)) return;

        LOG.info("Creating table {}", tableName);
        conn.exec(r.tableCreate(tableName));
        conn.exec(r.table(tableName).insert(r.hashMap("id", "db_version").with("db_version", DB_VERSION)));
        conn.exec(r.table(tableName).insert(r.hashMap("id", "state").with("shouldPause", false)));
        conn.exec(r.table(tableName).insert(r.hashMap("id", "log_levels")
                .with("logLevel",
                        r.array(r.hashMap("logger", "no.nb.nna.veidemann").with("level", "INFO"))
                )));
    }

    private void createConfigsTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.CONFIG);
        createIndex(Tables.CONFIG, "configRefs", true, row ->
                r.add(configRefPlural(row, Kind.browserConfig.name(), "scriptRef"))
                        .add(configRefSingular(row, Kind.crawlJob.name(), "scheduleRef"))
                        .add(configRefSingular(row, Kind.crawlJob.name(), "crawlConfigRef"))
                        .add(configRefSingular(row, Kind.crawlJob.name(), "scopeScriptRef"))
                        .add(configRefSingular(row, Kind.crawlConfig.name(), "collectionRef"))
                        .add(configRefSingular(row, Kind.crawlConfig.name(), "browserConfigRef"))
                        .add(configRefSingular(row, Kind.crawlConfig.name(), "politenessRef"))
        );
        createMetaIndexes(Tables.CONFIG);
    }

    private void createCrawlLogTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.CRAWL_LOG, "warcId");
        createIndex(Tables.CRAWL_LOG, "executionId");
    }

    private void createPageLogTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.PAGE_LOG, "warcId");
        createIndex(Tables.PAGE_LOG, "executionId");
    }

    private void createCrawledContentTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.CRAWLED_CONTENT, "digest");
    }

    private void createStorageRefTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.STORAGE_REF, "warcId");
    }

    private void createUriQueueTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.URI_QUEUE);
    }

    private void createCrawlExecutionsTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.EXECUTIONS);
        createIndex(Tables.EXECUTIONS, "startTime");
        createIndex(Tables.EXECUTIONS, "jobId");
        createIndex(Tables.EXECUTIONS, "state");
        createIndex(Tables.EXECUTIONS, "seedId");
        createIndex(Tables.EXECUTIONS, "jobExecutionId_seedId", row ->
                r.array(row.g("jobExecutionId"), row.g("seedId")));
        createIndex(Tables.EXECUTIONS, "seedId_createdTime", row ->
                r.array(row.g("seedId"), row.g("createdTime")));
    }

    private void createJobExecutionsTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.JOB_EXECUTIONS);
        createIndex(Tables.JOB_EXECUTIONS, "startTime");
        createIndex(Tables.JOB_EXECUTIONS, "jobId");
        createIndex(Tables.JOB_EXECUTIONS, "state");
        createIndex(Tables.JOB_EXECUTIONS, "jobId_startTime", row ->
                r.array(row.g("jobId"), row.g("startTime")));
    }

    private void createCrawlEntitiesTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.CRAWL_ENTITIES);
        createMetaIndexes(Tables.CRAWL_ENTITIES);
    }

    private void createSeedsTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.SEEDS);
        createIndex(Tables.SEEDS, "configRefs", true, row -> r.add(
                configRefPlural(row, Kind.seed.name(), "jobRef"),
                configRefSingular(row, Kind.seed.name(), "entityRef")));
        createMetaIndexes(Tables.SEEDS);
    }

    private void createEventTable() throws DbQueryException, DbConnectionException {
        createTable(Tables.EVENTS);
        createIndex(Tables.EVENTS, "state_lastModified", e -> r.array(e.g("state"), e.g("activity").nth(0).g("modifiedTime")));
        createIndex(Tables.EVENTS, "assignee_lastModified", e -> r.array(e.g("assignee"), e.g("activity").nth(0).g("modifiedTime")));
        createIndex(Tables.EVENTS, "label", true);
        createIndex(Tables.EVENTS, "lastModified", e -> e.g("activity").nth(0).g("modifiedTime"));
    }

    private final void createMetaIndexes(Tables table) throws DbQueryException, DbConnectionException {
        createIndex(table, "name", row -> row.g("meta").g("name").downcase());
        createIndex(table, "label", true, row -> row.g("meta").g("label").map(
                label -> r.array(label.g("key").downcase(), label.g("value").downcase())));
        createIndex(table, "kind_label_key", true, row -> row.g("meta").g("label").map(
                label -> r.array(row.g("kind"), label.g("key").downcase())));
        createIndex(table, "label_value", true, row -> row.g("meta").g("label").map(
                label -> label.g("value").downcase()));
        createIndex(table, "lastModified", row -> row.g("meta").g("lastModified"));
        createIndex(table, "lastModifiedBy", row -> row.g("meta").g("lastModifiedBy").downcase());
    }
}
