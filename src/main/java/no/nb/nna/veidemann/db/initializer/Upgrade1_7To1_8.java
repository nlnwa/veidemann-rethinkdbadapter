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

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.RethinkDbConfigAdapter;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

// This upgrade fixes a regression in version 1.7 (bug in the scroll.js script)
public class Upgrade1_7To1_8 extends UpgradeDbBase {
    public Upgrade1_7To1_8(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        RethinkDbConfigAdapter db = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();

        // bad script id
        final String scrollScriptId = "75dfe01a-e9cc-4fd2-8aa5-c04878d9f1a1";

        // delete bad script
        conn.exec(r.db(dbName).table(Tables.CONFIG.name).get(scrollScriptId).delete());

        // save good script
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("default_objects/browser-scripts.yaml")) {
            readYamlFile(in, ConfigObject.class)
                    .forEach(o -> {
                        if (o.getId().equals(scrollScriptId)) {
                            try {
                                db.saveConfigObject(o);
                            } catch (DbException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    String fromVersion() {
        return "1.7";
    }

    @Override
    String toVersion() {
        return "1.8";
    }
}
