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

package no.nb.nna.veidemann.db.initializer;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Branch;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TableCreator {
    private static final Logger LOG = LoggerFactory.getLogger(TableCreator.class);

    static final RethinkDB r = RethinkDB.r;

    final RethinkDbConnection conn;

    final String dbName;

    private Map<Tables, List<String>> createdIndexes = new HashMap<>();

    public TableCreator(String dbName, RethinkDbConnection conn) {
        this.dbName = dbName;
        this.conn = conn;
    }

    void createTable(Tables table) throws DbQueryException, DbConnectionException {
        if (tableExists(table)) return;
        LOG.info("Creating table {}", table.name);
        conn.exec(r.tableCreate(table.name));
    }

    void createTable(Tables table, String primaryKeyName) throws DbQueryException, DbConnectionException {
        if (tableExists(table)) return;
        LOG.info("Creating table {}", table.name);
        conn.exec(r.tableCreate(table.name).optArg("primary_key", primaryKeyName));
    }

    void deleteTable(Tables table) throws DbQueryException, DbConnectionException {
        if (!tableExists(table)) return;
        LOG.info("Deleting table {}", table.name);
        conn.exec(r.tableDrop(table.name));
    }

    void deleteTable(String tableName) throws DbQueryException, DbConnectionException {
        if (!tableExists(tableName)) return;
        LOG.info("Deleting table {}", tableName);
        conn.exec(r.tableDrop(tableName));
    }

    void createIndex(Tables table, String indexName) throws DbQueryException, DbConnectionException {
        createIndex(table, indexName, false);
    }

    void createIndex(Tables table, String indexName, boolean multi) throws DbQueryException, DbConnectionException {
        if (!tableExists(table)) return;
        if (indexExists(table, indexName)) return;
        LOG.info("Creating index {} on table {}", indexName, table.name);
        conn.exec(r.table(table.name).indexCreate(indexName).optArg("multi", multi));
        createdIndexes.computeIfAbsent(table, k -> new ArrayList<>()).add(indexName);
    }

    void createIndex(Tables table, String indexName, ReqlFunction1 func1) throws DbQueryException, DbConnectionException {
        createIndex(table, indexName, false, func1);
    }

    void createIndex(Tables table, String indexName, boolean multi, ReqlFunction1 func1) throws DbQueryException, DbConnectionException {
        if (!tableExists(table)) return;
        if (indexExists(table, indexName)) return;
        LOG.info("Creating index {} on table {}", indexName, table.name);
        conn.exec(r.table(table.name).indexCreate(indexName, func1).optArg("multi", multi));
        createdIndexes.computeIfAbsent(table, k -> new ArrayList<>()).add(indexName);
    }

    void deleteIndex(Tables table, String indexName) throws DbQueryException, DbConnectionException {
        deleteIndex(table.name, indexName);
    }

    void deleteIndex(String tableName, String indexName) throws DbQueryException, DbConnectionException {
        if (!tableExists(tableName)) return;
        if (!indexExists(tableName, indexName)) return;
        LOG.info("Deleting index {} from table {}", indexName, tableName);
        conn.exec(r.table(tableName).indexDrop(indexName));
    }

    boolean tableExists(Tables table) throws DbQueryException, DbConnectionException {
        return conn.exec(r.tableList().contains(table.name));
    }

    boolean tableExists(String tableName) throws DbQueryException, DbConnectionException {
        return conn.exec(r.tableList().contains(tableName));
    }

    boolean indexExists(Tables table, String indexName) throws DbQueryException, DbConnectionException {
        return indexExists(table.name, indexName);
    }

    boolean indexExists(String tableName, String indexName) throws DbQueryException, DbConnectionException {
        return conn.exec(r.table(tableName).indexList().contains(indexName));
    }

    void waitForIndexes() throws DbQueryException, DbConnectionException {
        for (Entry<Tables, List<String>> t : createdIndexes.entrySet()) {
            conn.exec(r.table(t.getKey().name).indexWait(r.args(t.getValue())));
        }
    }

    /**
     * Helper method to assist in creating configRefs index when configRef is plural
     *
     * @param row   ConfigObject
     * @param kind  Kind of ConfigObject
     * @param field Kind of the ConfigRef
     * @return An array with config reference or empty array
     */
    Branch configRefPlural(ReqlExpr row, String kind, String field) {
        return r.branch(
                row.hasFields(kind).and(row.g(kind).hasFields(field)),
                row.g(kind).g(field).map(d -> r.array(d.g("kind"), d.g("id"))),
                r.array());
    }

    /**
     * Helper method to assist in creating configRefs index when configRef is singular
     *
     * @param row   ConfigObject
     * @param kind  Kind of ConfigObject
     * @param field Kind of ConfigRef
     * @return An array with config reference or empty array
     */
    Branch configRefSingular(ReqlExpr row, String kind, String field) {
        return r.branch(
                row.hasFields(kind).and(row.g(kind).hasFields(field)),
                r.array(row.g(kind).g(field).do_(d -> r.array(d.g("kind"), d.g("id")))),
                r.array());
    }
}
