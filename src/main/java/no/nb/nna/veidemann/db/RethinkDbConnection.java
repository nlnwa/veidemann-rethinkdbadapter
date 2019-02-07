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
package no.nb.nna.veidemann.db;

import com.google.protobuf.Message;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Get;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Update;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.CrawlQueueAdapter;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbInitializer;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbServiceSPI;
import no.nb.nna.veidemann.commons.db.DistributedLock;
import no.nb.nna.veidemann.commons.db.DistributedLock.Key;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import no.nb.nna.veidemann.db.opentracing.ConnectionTracingInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RethinkDbConnection implements DbServiceSPI {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbConnection.class);

    private static final long MAX_WAIT_FOR_DB_MILLIS = 1000 * 60 * 30; // Half an hour

    static final String RETHINK_ARRAY_LIMIT_KEY = "RETHINK_ARRAY_LIMIT";

    static final RethinkDB r = RethinkDB.r;

    private Connection conn;

    private RethinkDbAdapter dbAdapter;

    private RethinkDbConfigAdapter configAdapter;

    private RethinkDbCrawlQueueAdapter queueAdapter;

    private RethinkDbExecutionsAdapter executionsAdapter;

    private RethinkDbInitializer dbInitializer;

    public <T> T exec(ReqlAst qry) throws DbConnectionException, DbQueryException {
        return exec("db-query", qry);
    }

    public <T> T exec(String operationName, ReqlAst qry) throws DbConnectionException, DbQueryException {
        synchronized (this) {
            if (!conn.isOpen()) {
                try {
                    conn.connect();
                } catch (TimeoutException ex) {
                    LOG.debug(ex.toString(), ex);
                    throw new DbConnectionException("Timed out waiting for connection", ex);
                }
            }
        }

        int retries = 0;
        long startTime = System.currentTimeMillis();

        OptArgs globalOpts = OptArgs.of(ConnectionTracingInterceptor.OPERATION_NAME_KEY, operationName);
        int arrayLimit = getArrayLimit();
        if (arrayLimit > 0) {
            globalOpts = globalOpts.with("array_limit", arrayLimit);
        }

        while (true) {
            try {
                T result = qry.run(conn, globalOpts);
                if (result instanceof Map
                        && ((Map) result).containsKey("errors")
                        && !((Map) result).get("errors").equals(0L)) {
                    DbQueryException ex = new DbQueryException((String) ((Map) result).get("first_error"));
                    LOG.error(ex.toString(), ex);
                    throw ex;
                }
                return result;
            } catch (ReqlOpFailedError e) {
                if (System.currentTimeMillis() < startTime + MAX_WAIT_FOR_DB_MILLIS && (
                        e.getMessage().contains("primary replica")
                                || e.getMessage().contains("java.net.SocketException: Operation timed out (Read failed)")
                )) {
                    LOG.error("DB not available at attempt #{}, waiting. Cause: {}", retries, e.toString(), e);
                    try {
                        r.db(conn.db().get()).wait_().optArg("wait_for", "ready_for_writes").run(conn);
                    } catch (Exception ex) {
                        LOG.warn("Failed waiting for db to have state ready_for_writes. Sleeping for 5 seconds before retry", ex);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    }
                    retries++;
                } else {
                    LOG.warn(e.toString(), e);
                    throw new DbQueryException(e.getMessage(), e);
                }
            } catch (ReqlError e) {
                LOG.warn(e.toString(), e);
                throw new DbQueryException(e.getMessage(), e);
            }
        }
    }

    public <T extends Message> T executeInsert(String operationName, Insert qry, Class<T> type) throws DbException {
        return executeInsertOrUpdate(operationName, qry, type);
    }

    public <T extends Message> T executeUpdate(String operationName, Update qry, Class<T> type) throws DbException {
        return executeInsertOrUpdate(operationName, qry, type);
    }

    private <T extends Message> T executeInsertOrUpdate(String operationName, ReqlExpr qry, Class<T> type) throws DbException {
        if (qry instanceof Insert) {
            qry = ((Insert) qry).optArg("return_changes", "always");
        } else if (qry instanceof Update) {
            qry = ((Update) qry).optArg("return_changes", "always");
        }

        Map<String, Object> response = exec(operationName, qry);
        List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

        Map newDoc = changes.get(0).get("new_val");
        return ProtoUtils.rethinkToProto(newDoc, type);
    }

    public <T extends Message> T executeGet(String operationName, Get qry, Class<T> type) throws DbException {
        Map<String, Object> response = exec(operationName, qry);

        if (response == null) {
            return null;
        }

        return ProtoUtils.rethinkToProto(response, type);
    }

    @Override
    public void close() {
        conn.close();
    }

    public Connection getConnection() {
        return conn;
    }

    @Override
    public DbAdapter getDbAdapter() {
        return dbAdapter;
    }

    @Override
    public ConfigAdapter getConfigAdapter() {
        return configAdapter;
    }

    @Override
    public CrawlQueueAdapter getCrawlQueueAdapter() {
        return queueAdapter;
    }

    @Override
    public ExecutionsAdapter getExecutionsAdapter() {
        return executionsAdapter;
    }

    @Override
    public DbInitializer getDbInitializer() {
        return dbInitializer;
    }

    @Override
    public DistributedLock createDistributedLock(Key key, int expireSeconds) {
        return new RethinkDbDistributedLock(this, key, expireSeconds);
    }

    @Override
    public List<Key> listExpiredDistributedLocks(String domain) throws DbQueryException, DbConnectionException {
        return RethinkDbDistributedLock.listExpiredDistributedLocks(this, domain);
    }

    @Override
    public List<Key> listExpiredDistributedLocks() throws DbQueryException, DbConnectionException {
        return RethinkDbDistributedLock.listExpiredDistributedLocks(this);
    }

    public void connect(CommonSettings settings) throws DbConnectionException {
        conn = connect(settings.getDbHost(), settings.getDbPort(), settings.getDbName(), settings.getDbUser(),
                settings.getDbPassword(), 30);

        dbAdapter = new RethinkDbAdapter(this);
        configAdapter = new RethinkDbConfigAdapter(this);
        queueAdapter = new RethinkDbCrawlQueueAdapter(this);
        executionsAdapter = new RethinkDbExecutionsAdapter(this);
        dbInitializer = new RethinkDbInitializer(this);
    }

    private Connection connect(String dbHost, int dbPort, String dbName, String dbUser, String dbPassword,
                               int reConnectAttempts) throws DbConnectionException {
        Connection c = null;
        int attempts = 0;
        while (c == null) {
            attempts++;
            try {
                c = r.connection()
                        .hostname(dbHost)
                        .port(dbPort)
                        .db(dbName)
                        .user(dbUser, dbPassword)
                        .connect();
            } catch (ReqlDriverError e) {
                LOG.warn(e.getMessage());
                if (attempts < reConnectAttempts) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    LOG.error("Too many connection attempts, giving up");
                    throw new DbConnectionException("Too many connection attempts", e);
                }
            }
        }
        return new ConnectionTracingInterceptor(c, true);
    }

    private int getArrayLimit() {
        return Integer.parseInt(System.getProperty(RETHINK_ARRAY_LIMIT_KEY, "0"));
    }
}
