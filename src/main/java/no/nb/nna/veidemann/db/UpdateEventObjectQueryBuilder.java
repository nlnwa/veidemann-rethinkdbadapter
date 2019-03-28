package no.nb.nna.veidemann.db;

import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import no.nb.nna.veidemann.api.eventhandler.v1.UpdateRequest;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.db.fieldmask.EventObjectQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rethinkdb.RethinkDB.r;
import static no.nb.nna.veidemann.db.RethinkDbEventAdapter.buildOptargConflictFunction;

public class UpdateEventObjectQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateEventObjectQueryBuilder.class);

    ReqlExpr q;

    public UpdateEventObjectQueryBuilder(UpdateRequest request) {
        ListEventObjectQueryBuilder l = new ListEventObjectQueryBuilder(request.getListRequest());
        q = l.getSelectForUpdateQuery();

        ReqlFunction1 updateDoc;
        if (request.hasUpdateMask()) {
            updateDoc = new EventObjectQueryBuilder(request.getUpdateMask())
                    .buildUpdateQuery(request.getUpdateTemplate());
        } else {
            updateDoc = new EventObjectQueryBuilder()
                    .buildUpdateQuery(request.getUpdateTemplate());
        }

        String user;
        if (EmailContextKey.email() == null || EmailContextKey.email().isEmpty()) {
            user = "anonymous";
        } else {
            user = EmailContextKey.email();
        }

        q = q.merge(updateDoc)
                .forEach(doc -> r.table(l.table.name)
                        .insert(doc)
                        // A rethink function which keeps old values for fields not allowed to be changed and updates activity log
                        .optArg("conflict", buildOptargConflictFunction(request.getComment()))
                );
    }

    public ReqlExpr getUpdateQuery() {
        return q;
    }
}
