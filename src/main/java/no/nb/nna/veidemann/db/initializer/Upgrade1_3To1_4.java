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

import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;


public class Upgrade1_3To1_4 extends UpgradeDbBase {
    public Upgrade1_3To1_4(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        // delete erroneous configRefs index
        deleteIndex(Tables.CONFIG, "configRefs");
        // recreate configRefs index
        createIndex(Tables.CONFIG, "configRefs", true, row ->
                        r.add(configRefPlural(row, Kind.browserConfig.name(), "scriptRef"))
                                .add(configRefSingular(row, Kind.crawlJob.name(), "scheduleRef"))
                                .add(configRefSingular(row, Kind.crawlJob.name(), "crawlConfigRef"))
                                .add(configRefSingular(row, Kind.crawlConfig.name(), "collectionRef"))
                                .add(configRefSingular(row, Kind.crawlConfig.name(), "browserConfigRef"))
                                .add(configRefSingular(row, Kind.crawlConfig.name(), "politenessRef")));

        // delete erroneous configRefs index
        deleteIndex(Tables.SEEDS, "configRefs");
        // recreate configRefs index
        createIndex(Tables.SEEDS, "configRefs", true, row -> r.add(
                configRefPlural(row, Kind.seed.name(), "jobRef"),
                configRefSingular(row, Kind.seed.name(), "entityRef")));

        waitForIndexes();
    }

    @Override
    String fromVersion() {
        return "1.3";
    }

    @Override
    String toVersion() {
        return "1.4";
    }
}
