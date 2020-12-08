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
import no.nb.nna.veidemann.api.frontier.v1.PageLogOrBuilder;
import no.nb.nna.veidemann.api.report.v1.PageLogListRequest;
import no.nb.nna.veidemann.db.fieldmask.PageLogQueryBuilder;
import no.nb.nna.veidemann.db.queryoptimizer.QueryOptimizer;

public class ListPageLogQueryBuilder {
    private static final PageLogQueryBuilder NO_MASK_BUILDER = new PageLogQueryBuilder();

    private final ReqlExpr q;
    private final PageLogListRequest request;
    final Tables table;

    public ListPageLogQueryBuilder(PageLogListRequest request) {
        this.request = request;
        table = Tables.PAGE_LOG;

        QueryOptimizer<PageLogOrBuilder> optimizer = new QueryOptimizer<>(NO_MASK_BUILDER, table);

        if (!request.getOrderByPath().isEmpty()) {
            optimizer.wantOrderQuery(request.getOrderByPath(), request.getOrderDescending());
        }

        if (request.getWarcIdCount() > 0) {
            optimizer.wantIdQuery(request.getWarcIdList());
        }

        if (request.hasQueryTemplate() && request.hasQueryMask()) {
            PageLogQueryBuilder queryBuilder = new PageLogQueryBuilder(request.getQueryMask());
            optimizer.wantFieldMaskQuery(queryBuilder, request.getQueryTemplate());
        }

        q = optimizer.render();
    }

    public ReqlExpr getListQuery() {
        ReqlExpr query = q;

        if (request.hasReturnedFieldsMask()) {
            PageLogQueryBuilder queryBuilder = new PageLogQueryBuilder(request.getReturnedFieldsMask());
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
