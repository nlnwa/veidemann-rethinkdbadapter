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
import no.nb.nna.veidemann.api.eventhandler.v1.EventObject;
import no.nb.nna.veidemann.api.eventhandler.v1.EventObjectOrBuilder;

public class EventObjectQueryBuilder extends RethinkDbFieldMasksQueryBuilder<EventObjectOrBuilder> {
    private final static ObjectPathAccessor OBJ_DEF = new ObjectPathAccessor(EventObject.class);

    public EventObjectQueryBuilder() {
        super(OBJ_DEF);
    }

    public EventObjectQueryBuilder(FieldMask fieldMask) {
        super(OBJ_DEF.createForFieldMaskProto(fieldMask));
    }

    @Override
    protected void init() {
        addReadOnlyPath("id");
        addReadOnlyPath("lastUpdated");
        addReadOnlyPath("activity");
        addReadOnlyPath("created");

        addMinimumReturnedField("id");
    }
}
