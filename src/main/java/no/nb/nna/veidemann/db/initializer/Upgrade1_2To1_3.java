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

import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;


public class Upgrade1_2To1_3 extends UpgradeDbBase {
    public Upgrade1_2To1_3(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        // create new compound jobExecutionId and seedId index for crawl executions table
        createIndex(Tables.EXECUTIONS, "jobExecutionId_seedId", row ->
                r.array(row.g("jobExecutionId"), row.g("seedId"))
        );

        // delete old jobExecutionId index
        deleteIndex(Tables.EXECUTIONS, "jobExecutionId");

        // create new compound seedId and createdTime index for craw executions table
        createIndex(Tables.EXECUTIONS, "seedId_createdTime", row ->
                r.array(row.g("seedId"), row.g("createdTime")));

        // create new compound jobId and startTime index for job executions table
        createIndex(Tables.JOB_EXECUTIONS, "jobId_startTime", row ->
                r.array(row.g("jobId"), row.g("startTime")));
    }

    @Override
    String fromVersion() {
        return "1.2";
    }

    @Override
    String toVersion() {
        return "1.3";
    }
}
