/*
 * Copyright 2017 National Library of Norway.
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
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbInitializer;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbUpgradeException;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RethinkDbInitializer implements DbInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbInitializer.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbInitializer(RethinkDbConnection conn) {
        this.conn = conn;
    }

    public void initialize() throws DbUpgradeException, DbQueryException, DbConnectionException {
        String dbName = conn.getConnection().db().get();

        if (!(boolean) conn.exec(r.dbList().contains(dbName))) {
            // No existing database, creating a new one
            LOG.info("Creating database: " + dbName);
            new CreateNewDb(dbName, conn).run();
            LOG.info("Populating database with default data");
            new PopulateDbWithDefaultData().run();
        } else {
            String version = getCurrentDbVersion();
            if (CreateNewDb.DB_VERSION.equals(version)) {
                LOG.info("Database found and is newest version: {}", version);
                // Executes create because this method only creates missing tables
                new CreateNewDb(dbName, conn).run();
            } else {
                LOG.info("Database with version {} found, upgrading", version);
                upgrade(version);
            }
        }
        LOG.info("DB initialized");
    }

    @Override
    public void delete() throws DbException {
        try {
            conn.exec(r.dbDrop("veidemann"));
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
    }

    public RethinkDbConnection getDbConnection() {
        return conn;
    }

    private void upgrade(String fromVersion) throws DbUpgradeException, DbQueryException, DbConnectionException {
        String dbName = conn.getConnection().db().get();

        switch (fromVersion) {
            case "0.1":
                new Upgrade0_1To0_2(dbName, conn).run();
                break;
            case "0.2":
                new Upgrade0_2To0_3(dbName, conn).run();
                break;
            case "0.3":
                new Upgrade0_3To0_4(dbName, conn).run();
                break;
            case "0.4":
                new Upgrade0_4To1_0(dbName, conn).run();
                break;
            case "1.0":
                new Upgrade1_0To1_1(dbName, conn).run();
                break;
            case "1.1":
                new Upgrade1_1To1_2(dbName, conn).run();
                break;
            case "1.2":
                new Upgrade1_2To1_3(dbName, conn).run();
                break;
            case "1.3":
                new Upgrade1_3To1_4(dbName, conn).run();
                break;
            case "1.4":
                new Upgrade1_4To1_5(dbName, conn).run();
                break;
            case "1.5":
                new Upgrade1_5To1_6(dbName, conn).run();
                break;
            case "1.6":
                new Upgrade1_6To1_7(dbName, conn).run();
                break;
            case "1.7":
                new Upgrade1_7To1_8(dbName, conn).run();
                break;
            case "1.8":
                new Upgrade1_8To1_9(dbName, conn).run();
                break;
            case "1.9":
                new Upgrade1_9To1_10(dbName, conn).run();
                break;
            case "1.10":
                new Upgrade1_10To1_11(dbName, conn).run();
                break;
            case "1.11":
                new Upgrade1_11To1_12(dbName, conn).run();
                break;
            case "1.12":
                new Upgrade1_12To1_13(dbName, conn).run();
                break;
            case "1.13":
                new Upgrade1_13To1_14(dbName, conn).run();
                break;
            default:
                throw new DbUpgradeException("Unknown database version '" + fromVersion + "', unable to upgrade");
        }

        String currentVersion = getCurrentDbVersion();
        String targetVersion = CreateNewDb.DB_VERSION;
        if (!currentVersion.equals(targetVersion)) {
            upgrade(currentVersion);
        }
    }

    private String getCurrentDbVersion() throws DbQueryException, DbConnectionException {
        return conn.exec(r.table(Tables.SYSTEM.name).get("db_version").g("db_version"));
    }

}
