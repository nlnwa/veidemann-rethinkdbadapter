package no.nb.nna.veidemann.db;

import com.rethinkdb.gen.ast.ReqlExpr;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rethinkdb.RethinkDB.r;

public class ListEventObjectQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ListEventObjectQueryBuilder.class);

    private ReqlExpr q;
    private final ListRequest request;
    final Tables table;

    public ListEventObjectQueryBuilder(ListRequest request) {
        this.request = request;
        // table = RethinkDbEventAdapter

        q  = r.table("event");
    }

    public ReqlExpr getListQuery() {
        ReqlExpr query = q;

        if (request.hasReturnedFieldsMask()) {
            FieldMasks fm = FieldMasks.createForFieldMaskProto(request.getQueryMask());
            query = query.pluck(fm.createPluckQuery());
        }

        if (request.getPageSize() > 0 || request.getOffset() > 0) {
            query = query.skip(request.getOffset()).limit(request.getPageSize());
        }
        return query;
    }

    public ReqlExpr getCountQuery() { return q.count(); }
}
