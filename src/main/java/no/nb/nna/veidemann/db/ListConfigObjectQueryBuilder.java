/*
 * Copyright 2018 National Library of Norway.
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
import no.nb.nna.veidemann.api.config.v1.ConfigObjectOrBuilder;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.db.fieldmask.ConfigObjectQueryBuilder;
import no.nb.nna.veidemann.db.queryoptimizer.QueryOptimizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ListConfigObjectQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ListConfigObjectQueryBuilder.class);
    private static final ConfigObjectQueryBuilder NO_MASK_BUILDER = new ConfigObjectQueryBuilder();

    private ReqlExpr q;
    private final ListRequest request;
    final Tables table;

    public ListConfigObjectQueryBuilder(ListRequest request) {
        this.request = request;
        table = RethinkDbConfigAdapter.getTableForKind(request.getKind());

        QueryOptimizer<ConfigObjectOrBuilder> optimizer = new QueryOptimizer<>(NO_MASK_BUILDER, table);

        if (!request.getOrderByPath().isEmpty()) {
            optimizer.wantOrderQuery(request.getOrderByPath(), request.getOrderDescending());
        }

        if (request.getIdCount() > 0) {
            optimizer.wantIdQuery(request.getIdList());
        }

        if (request.hasQueryTemplate() && request.hasQueryMask()) {
            ConfigObjectQueryBuilder queryBuilder = new ConfigObjectQueryBuilder(request.getQueryMask());
            optimizer.wantFieldMaskQuery(queryBuilder, request.getQueryTemplate());
        }

        switch (request.getKind()) {
            case seed:
            case crawlEntity:
                break;
            default:
                optimizer.wantGetAllQuery("kind", request.getKind().name());
        }

        if (request.getLabelSelectorCount() > 0) {
            parseSelectorQuery(optimizer, request.getLabelSelectorList());
        }

        q = optimizer.render();

        if (!request.getNameRegex().isEmpty()) {
            final String qry = "(?i)" + request.getNameRegex();
            q = q.filter(doc -> doc.g("meta").g("name").match(qry));
        }
    }

    public ReqlExpr getListQuery() {
        ReqlExpr query = q;

        if (request.hasReturnedFieldsMask()) {
            ConfigObjectQueryBuilder queryBuilder = new ConfigObjectQueryBuilder(request.getReturnedFieldsMask());
            query = query.pluck(queryBuilder.createPluckQuery());
        }

        if (request.getPageSize() > 0 || request.getOffset() > 0) {
            query = query.skip(request.getOffset()).limit(request.getPageSize());
        }

        return query;
    }

    public ReqlExpr getCountQuery() {
        return q.count();
    }

    public ReqlExpr getSelectForUpdateQuery() {
        ReqlExpr query = q;

        if (request.getPageSize() > 0 || request.getOffset() > 0) {
            query = query.skip(request.getOffset()).limit(request.getPageSize());
        }

        return query;
    }

    void parseSelectorQuery(QueryOptimizer<ConfigObjectOrBuilder> optimizer, List<String> selector) {
        selector.forEach(s -> {
            String key;
            String value;

            int sepIdx = s.indexOf(':');
            if (sepIdx == -1) {
                key = "";
                value = s.toLowerCase();
            } else {
                key = s.substring(0, sepIdx).toLowerCase();
                value = s.substring(sepIdx + 1).toLowerCase();
            }

            LOG.debug("Adding selector: {key={}, value={}}", key, value);
            optimizer.wantLabelQuery(ApiTools.buildLabel(key, value));
        });
    }
}
