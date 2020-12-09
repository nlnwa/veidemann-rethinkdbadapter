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
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConfigAdapter;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;


public class Upgrade1_11To1_12 extends UpgradeDbBase {
    public Upgrade1_11To1_12(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        RethinkDbConfigAdapter db = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();

        // Update configRefIndex with scopeScriptRef
        deleteIndex(Tables.CONFIG, "configRefs");
        createIndex(Tables.CONFIG, "configRefs", true, row ->
                r.add(configRefPlural(row, Kind.browserConfig.name(), "scriptRef"))
                        .add(configRefSingular(row, Kind.crawlJob.name(), "scheduleRef"))
                        .add(configRefSingular(row, Kind.crawlJob.name(), "crawlConfigRef"))
                        .add(configRefSingular(row, Kind.crawlJob.name(), "scopeScriptRef"))
                        .add(configRefSingular(row, Kind.crawlConfig.name(), "collectionRef"))
                        .add(configRefSingular(row, Kind.crawlConfig.name(), "browserConfigRef"))
                        .add(configRefSingular(row, Kind.crawlConfig.name(), "politenessRef"))
        );

        // add default scope check script
        final String scopeCheckId = "bfde69f5-1e4c-4207-a209-263058b9e41f";

        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("default_objects/browser-scripts.yaml")) {
            readYamlFile(in, ConfigObject.class)
                    .forEach(o -> {
                        if (scopeCheckId.equals(o.getId())) {
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

        // update crawl jobs with scopeScriptRef
        try (ChangeFeed<ConfigObject> crawlJobs = db.listConfigObjects(ListRequest.newBuilder().setKind(Kind.crawlJob).build());) {
            ConfigRef scopeScriptRef = ConfigRef.newBuilder().setKind(Kind.browserScript).setId(scopeCheckId).build();
            crawlJobs.stream().forEach(job -> {
                if (!job.getCrawlJob().hasScopeScriptRef()) {
                    ConfigObject.Builder jobBuilder = job.toBuilder();
                    jobBuilder.getCrawlJobBuilder().setScopeScriptRef(scopeScriptRef);
                    try {
                        db.saveConfigObject(jobBuilder.build());
                    } catch (DbException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    @Override
    String fromVersion() {
        return "1.11";
    }

    @Override
    String toVersion() {
        return "1.12";
    }
}
