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
import java.util.Arrays;


public class Upgrade1_9To1_10 extends UpgradeDbBase {
    public Upgrade1_9To1_10(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        RethinkDbConfigAdapter db = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();

        // update the following scripts
        final String extractOutlinksScriptId = "52aeccf3-77d5-4c18-b55f-6561d582a7fb";
        final String scrollScriptId = "75dfe01a-e9cc-4fd2-8aa5-c04878d9f1a1";
        final String scrollTo = "0ef72ea7-f145-4908-adaa-ebf50890b09c";
        final String[] scriptIds = {extractOutlinksScriptId, scrollScriptId, scrollTo};

        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("default_objects/browser-scripts.yaml")) {
            readYamlFile(in, ConfigObject.class)
                    .forEach(o -> {
                        if (Arrays.stream(scriptIds).anyMatch(id -> id.equals(o.getId()))) {
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
        return "1.9";
    }

    @Override
    String toVersion() {
        return "1.10";
    }
}
