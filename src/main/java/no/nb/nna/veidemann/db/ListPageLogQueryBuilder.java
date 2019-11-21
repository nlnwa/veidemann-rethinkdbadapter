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
import com.rethinkdb.gen.ast.Table;
import no.nb.nna.veidemann.api.report.v1.CrawlLogListRequest;
import no.nb.nna.veidemann.api.report.v1.PageLogListRequest;
import no.nb.nna.veidemann.db.fieldmask.CrawlLogQueryBuilder;
import no.nb.nna.veidemann.db.fieldmask.PageLogQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rethinkdb.RethinkDB.r;

public class ListPageLogQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ListPageLogQueryBuilder.class);
    private static final PageLogQueryBuilder NO_MASK_BUILDER = new PageLogQueryBuilder();

    private ReqlExpr q;
    private final PageLogListRequest request;
    final Tables table;

    public ListPageLogQueryBuilder(PageLogListRequest request) {
        this.request = request;
        table = Tables.PAGE_LOG;

        q = r.table(table.name);

        if (!request.getOrderByPath().isEmpty()) {
            if (request.getOrderDescending()) {
                q = q.orderBy().optArg("index",
                        r.desc(NO_MASK_BUILDER.getSortIndexForPath(request.getOrderByPath())));
            } else {
                q = q.orderBy().optArg("index",
                        r.asc(NO_MASK_BUILDER.getSortIndexForPath(request.getOrderByPath())));
            }
        }

        if (request.getWarcIdCount() > 0) {
            if (q instanceof Table) {
                q = ((Table) q).getAll(request.getWarcIdList().toArray());
            } else {
                q = q.filter(row -> r.expr(request.getWarcIdList().toArray()).contains(row.g("id")));
            }
        }

        if (request.hasQueryTemplate() && request.hasQueryMask()) {
            PageLogQueryBuilder queryBuilder = new PageLogQueryBuilder(request.getQueryMask());
            q = q.filter(queryBuilder.buildFilterQuery(request.getQueryTemplate()));
        }
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
