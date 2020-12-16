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
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatusOrBuilder;

public class JobExecutionQueryBuilder extends RethinkDbFieldMasksQueryBuilder<JobExecutionStatusOrBuilder> {
    private final static ObjectPathAccessor<JobExecutionStatusOrBuilder> OBJ_DEF = new ObjectPathAccessor<>(JobExecutionStatus.class);

    public JobExecutionQueryBuilder() {
        super(OBJ_DEF);
    }

    public JobExecutionQueryBuilder(FieldMask fieldMask) {
        super(OBJ_DEF.createForFieldMaskProto(fieldMask));
    }

    @Override
    protected void init() {
        addPrimaryIndex("id", "id");
        addIndex("startTime", "startTime");
        addIndex("jobId", "jobId");
        addIndex("state", "state");
        addIndex("jobId_startTime", "jobId", "startTime");

        addMinimumReturnedField("id");
    }

}
