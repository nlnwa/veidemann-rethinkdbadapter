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

package no.nb.nna.veidemann.db;

import com.rethinkdb.gen.ast.ReqlExpr;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatusOrBuilder;
import no.nb.nna.veidemann.api.report.v1.JobExecutionsListRequest;
import no.nb.nna.veidemann.db.fieldmask.JobExecutionQueryBuilder;
import no.nb.nna.veidemann.db.queryoptimizer.QueryOptimizer;

import java.util.stream.Collectors;

public class ListJobExecutionQueryBuilder {
    private static final JobExecutionQueryBuilder NO_MASK_BUILDER = new JobExecutionQueryBuilder();

    private final ReqlExpr q;
    private final JobExecutionsListRequest request;

    public ListJobExecutionQueryBuilder(JobExecutionsListRequest request) {
        this.request = request;

        QueryOptimizer<JobExecutionStatusOrBuilder> optimizer = new QueryOptimizer<>(NO_MASK_BUILDER, Tables.JOB_EXECUTIONS);
        if (request.getIdCount() > 0) {
            optimizer.wantIdQuery(request.getIdList());
        }
        if (request.hasStartTimeFrom() || request.hasStartTimeTo()) {
            optimizer.wantBetweenQuery("startTime", request.getStartTimeFrom(), request.getStartTimeTo());
        }
        if (request.getStateCount() > 0) {
            optimizer.wantGetAllQuery("state", request.getStateList().stream().map(Enum::name).collect(Collectors.toList()));
        }
        if (request.hasQueryTemplate() && request.hasQueryMask()) {
            JobExecutionQueryBuilder queryBuilder = new JobExecutionQueryBuilder(request.getQueryMask());
            optimizer.wantFieldMaskQuery(queryBuilder, request.getQueryTemplate());
        }
        if (!request.getOrderByPath().isEmpty()) {
            optimizer.wantOrderQuery(request.getOrderByPath(), request.getOrderDescending());
        }

        q = optimizer.render();
    }

    public ReqlExpr getListQuery() {
        ReqlExpr query = q;

        if (request.hasReturnedFieldsMask()) {
            JobExecutionQueryBuilder queryBuilder = new JobExecutionQueryBuilder(request.getReturnedFieldsMask());
            query = query.pluck(queryBuilder.createPluckQuery());
        }

        if (request.getWatch()) {
            query = query.changes();
        }

        if (request.getPageSize() > 0 || request.getOffset() > 0) {
            query = query.skip(request.getOffset()).limit(request.getPageSize());
        }

        return query;
    }

    public ReqlExpr getCountQuery() {
        return q.count();
    }
}
