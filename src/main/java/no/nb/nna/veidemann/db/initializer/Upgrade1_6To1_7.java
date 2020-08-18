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

import com.google.protobuf.Message;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConfigAdapter;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// This upgrade fixes  a regression in version 1.6 (bug in the extract-outlinks.js script)
public class Upgrade1_6To1_7 extends UpgradeDbBase {
    public Upgrade1_6To1_7(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        RethinkDbConfigAdapter db = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();

        // remove all scripts named "extract-outlinks.js"
        conn.exec(r.db(dbName).table(Tables.CONFIG.name)
                .getAll("extract-outlinks.js")
                .optArg("index", "name").delete());
        // remove all scripts named "scroll.js"
        conn.exec(r.db(dbName).table(Tables.CONFIG.name)
                .getAll("scroll.js")
                .optArg("index", "name").delete());
        // remove all scripts named "scrollTo.js"
        conn.exec(r.db(dbName).table(Tables.CONFIG.name)
                .getAll("scrollTo.js".toLowerCase())
                .optArg("index", "name").delete());

        // Save new browser scripts
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

    <T extends Message> Stream<T> readYamlFile(InputStream in, Class<T> type) {
        Yaml yaml = new Yaml();
        return StreamSupport.stream(yaml.loadAll(in).spliterator(), false)
                .map(o -> ProtoUtils.rethinkToProto((Map) o, type));
    }

    @Override
    String fromVersion() {
        return "1.6";
    }

    @Override
    String toVersion() {
        return "1.7";
    }
}
