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


public class Upgrade1_8To1_9 extends UpgradeDbBase {
    public Upgrade1_8To1_9(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        deleteIndex(Tables.URI_QUEUE, "surt");
        deleteIndex(Tables.URI_QUEUE, "executionId");
        deleteIndex(Tables.URI_QUEUE, "crawlHostGroupKey_sequence_earliestFetch");
        deleteTable("crawl_host_group");
    }

    @Override
    String fromVersion() {
        return "1.8";
    }

    @Override
    String toVersion() {
        return "1.9";
    }
}
