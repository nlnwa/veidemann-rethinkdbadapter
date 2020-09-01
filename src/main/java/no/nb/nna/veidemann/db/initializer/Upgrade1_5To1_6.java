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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;


public class Upgrade1_5To1_6 extends UpgradeDbBase {
    public Upgrade1_5To1_6(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        RethinkDbConfigAdapter db = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();

        // upgrade browser scripts
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("default_objects/browser-scripts.yaml")) {
            readYamlFile(in, ConfigObject.class)
                    .forEach(o -> {
                        try {
                            db.saveConfigObject(o);
                        } catch (DbException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    String fromVersion() {
        return "1.5";
    }

    @Override
    String toVersion() {
        return "1.6";
    }
}
