package no.nb.nna.veidemann.db;

import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import no.nb.nna.veidemann.api.eventhandler.v1.ListRequest;
import no.nb.nna.veidemann.db.fieldmask.EventObjectQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rethinkdb.RethinkDB.r;

public class ListEventObjectQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ListEventObjectQueryBuilder.class);
    private static final EventObjectQueryBuilder NO_MASK_BUILDER = new EventObjectQueryBuilder();

    private ReqlExpr q;
    private final ListRequest request;
    final Tables table;

    public ListEventObjectQueryBuilder(ListRequest request) {
        this.request = request;
        table = Tables.EVENTS;

        q = r.table(table.name);

        if (request.getIdCount() > 0) {
            if (q instanceof Table) {
                q = ((Table) q).getAll(request.getIdList().toArray());
            } else {
                q = q.filter(row -> r.expr(request.getIdList().toArray()).contains(row.g("id")));
            }
        } else if (request.hasQueryMask() && request.getQueryMask().getPathsList().contains("state")) {
            q = q.orderBy().optArg("index", r.desc("state_lastModified"))
                    .between(r.array(request.getQueryTemplate().getState().name(), r.minval()), r.array(request.getQueryTemplate().getState().name(), r.maxval()));
        } else if (request.hasQueryMask() && request.getQueryMask().getPathsList().contains("assignee")) {
            q = q.orderBy().optArg("index", r.desc("assignee_lastModified"))
                    .between(r.array(request.getQueryTemplate().getAssignee(), r.minval()), r.array(request.getQueryTemplate().getAssignee(), r.maxval()));
        } else {
            q = q.orderBy().optArg("index", r.desc("lastModified"));
        }


        if (request.hasQueryTemplate() && request.hasQueryMask()) {
            EventObjectQueryBuilder queryBuilder = new EventObjectQueryBuilder(request.getQueryMask());
            q = q.filter(queryBuilder.buildFilterQuery(request.getQueryTemplate()));
        }
    }

    public ReqlExpr getListQuery() {
        ReqlExpr query = q;

        if (request.hasReturnedFieldsMask()) {
            EventObjectQueryBuilder queryBuilder = new EventObjectQueryBuilder(request.getReturnedFieldsMask());
            query = query.pluck(queryBuilder.createPluckQuery());
        }

        if (request.getPageSize() > 0 || request.getOffset() > 0) {
            query = query.skip(request.getOffset()).limit(request.getPageSize());
        }
        return query;
    }

    public ReqlExpr getSelectForUpdateQuery() {
        ReqlExpr query = q;

        if (request.getPageSize() > 0 || request.getOffset() > 0) {
            query = query.skip(request.getOffset()).limit(request.getPageSize());
        }

        return query;
    }

    public ReqlExpr getCountQuery() {
        return q.count();
    }
}
