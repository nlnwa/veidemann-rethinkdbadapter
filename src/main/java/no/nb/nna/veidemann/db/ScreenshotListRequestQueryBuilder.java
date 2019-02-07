/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.db;

import com.rethinkdb.gen.ast.ReqlExpr;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListReply;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListRequest;
import no.nb.nna.veidemann.commons.db.DbException;

import static no.nb.nna.veidemann.db.RethinkDbAdapter.r;

/**
 *
 */
public class ScreenshotListRequestQueryBuilder extends ConfigListQueryBuilder<ScreenshotListRequest> {

    public ScreenshotListRequestQueryBuilder(ScreenshotListRequest request) {
        super(request, Tables.SCREENSHOT);
        setPaging(request.getPageSize(), request.getPage());

        if (request.getIdCount() > 0) {
            buildIdQuery(request.getIdList());
            if (!request.getImgData()) {
                withoutImg();
            }
        } else if (!request.getExecutionId().isEmpty()) {
            buildExecutionIdQuery(request.getExecutionId());
            if (!request.getImgData()) {
                withoutImg();
            }
        } else {
            buildAllQuery();
            if (!request.getImgData()) {
                withoutImg();
            }
        }

        addFilter(request.getFilterList());
    }

    public ScreenshotListReply.Builder executeList(RethinkDbConnection conn) throws DbException {
        return executeList(conn, ScreenshotListReply.newBuilder());
    }

    void buildExecutionIdQuery(String name) {
        ReqlExpr qry = r.table(table.name)
                .getAll(name)
                .optArg("index", "executionId");
        setListQry(qry);
    }

    void withoutImg() {
        setListQry(getListQry().without("img"));
    }
}
