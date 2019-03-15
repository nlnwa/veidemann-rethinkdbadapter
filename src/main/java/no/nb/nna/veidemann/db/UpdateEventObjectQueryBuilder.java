package no.nb.nna.veidemann.db;

import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import no.nb.nna.veidemann.api.config.v1.UpdateRequest;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rethinkdb.RethinkDB.r;

public class UpdateEventObjectQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateEventObjectQueryBuilder.class);

    ReqlExpr q;

    public UpdateEventObjectQueryBuilder(UpdateRequest request) {
        ListEventObjectQueryBuilder l = new ListEventObjectQueryBuilder(request.getListRequest());
        q = l.getSelectorForUpdateQuery();

        ReqlFunction1 updateDoc;
        if (request.hasUpdateMask()) {
            updateDoc = FieldMasks.createForFieldMaskProto(request.getUpdateMask())
                    .buildUpdateQuery(request.getListRequest(), request.getUpdateTemplate());
        } else {
            updateDoc = FieldMasks.CONFIG_OBJECT_DEF
                    .buildUpdateQuery(request.getListRequest(), request.getUpdateTemplate());
        }

        String user;
        if (EmailContextKey.email() == null || EmailContextKey.email().isEmpty()) {
            user = "anonymous";
        } else {
            user = EmailContextKey.email();
        }

        q  =  q.merge(updateDoc)
                .forEach(doc -> r.table(l.table.name)
                        .insert(doc)
                        .optArg("conflict",
                                (id, old_doc, new_doc) -> r.branch(
                                        old_doc.eq(new_doc),
                                        old_doc,
                                        new_doc.merge(
                                          r.hashMap()
                                        ))
                                ))
    }
}
