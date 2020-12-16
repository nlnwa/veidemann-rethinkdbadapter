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

package no.nb.nna.veidemann.db.fieldmask;

import no.nb.nna.veidemann.api.commons.v1.FieldMask;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigObjectOrBuilder;

public class ConfigObjectQueryBuilder extends RethinkDbFieldMasksQueryBuilder<ConfigObjectOrBuilder> {
    private final static ObjectPathAccessor<ConfigObjectOrBuilder> OBJ_DEF = new ObjectPathAccessor<>(ConfigObject.class);

    public ConfigObjectQueryBuilder() {
        super(OBJ_DEF);
    }

    public ConfigObjectQueryBuilder(FieldMask fieldMask) {
        super(OBJ_DEF.createForFieldMaskProto(fieldMask));
    }

    @Override
    protected void init() {
        addPrimaryIndex("id", "id");
        addIgnoreCaseIndex("name", "meta.name");
        addIgnoreCaseIndex("label", "meta.label");
        addIgnoreCaseIndex("kind_label_key", "kind", "meta.label.key");
        addIgnoreCaseIndex("label_value", "meta.label.value");
        addIndex("lastModified", "meta.lastModified");
        addIndex("lastModifiedBy", "meta.lastModifiedBy");
        addIndex("configRefs", "seed.jobRef");
        addIndex("configRefs", "seed.entityRef");
        addIndex("configRefs", "browserConfig.scriptRef");
        addIndex("configRefs", "crawlJob.scheduleRef");
        addIndex("configRefs", "crawlJob.crawlConfigRef");
        addIndex("configRefs", "crawlJob.scopeScriptRef");
        addIndex("configRefs", "crawlConfig.collectionRef");
        addIndex("configRefs", "crawlConfig.browserConfigRef");
        addIndex("configRefs", "crawlConfig.politenessRef");

        addReadOnlyPath("id");
        addReadOnlyPath("apiVersion");
        addReadOnlyPath("kind");
        addReadOnlyPath("meta.created");
        addReadOnlyPath("meta.createdBy");
        addReadOnlyPath("meta.lastModified");
        addReadOnlyPath("meta.lastModifiedBy");

        addMinimumReturnedField("apiVersion");
        addMinimumReturnedField("kind");
        addMinimumReturnedField("id");
    }

}
