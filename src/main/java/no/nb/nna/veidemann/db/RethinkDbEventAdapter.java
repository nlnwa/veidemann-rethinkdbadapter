package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction3;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.eventhandler.v1.Activity.ChangeType;
import no.nb.nna.veidemann.api.eventhandler.v1.DeleteResponse;
import no.nb.nna.veidemann.api.eventhandler.v1.EventObject;
import no.nb.nna.veidemann.api.eventhandler.v1.EventObject.State;
import no.nb.nna.veidemann.api.eventhandler.v1.EventRef;
import no.nb.nna.veidemann.api.eventhandler.v1.ListCountResponse;
import no.nb.nna.veidemann.api.eventhandler.v1.ListLabelRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.ListLabelResponse;
import no.nb.nna.veidemann.api.eventhandler.v1.ListRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.SaveRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.UpdateRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.UpdateResponse;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.EventAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class RethinkDbEventAdapter implements EventAdapter {
    private static final ChangableField[] changeableFields = new ChangableField[]{
            new ChangableField("state", false),
            new ChangableField("assignee", false),
            new ChangableField("data", true),
            new ChangableField("severity", false),
            new ChangableField("label", true)
    };

    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbEventAdapter.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbEventAdapter(RethinkDbConnection conn) {
        this.conn = conn;
    }

    @Override
    public EventObject getEventObject(EventRef request) throws DbException {
        Map<String, Object> response = conn.exec("db-getConfigObject",
                r.table(Tables.EVENTS.name)
                        .get(request.getId())
        );

        if (response == null) {
            return null;
        }

        return ProtoUtils.rethinkToProto(response, EventObject.class);
    }

    @Override
    public ChangeFeed<EventObject> listEventObjects(no.nb.nna.veidemann.api.eventhandler.v1.ListRequest request) throws DbQueryException, DbConnectionException {
        ListEventObjectQueryBuilder q = new ListEventObjectQueryBuilder(request);

        Cursor<Map<String, Object>> res = conn.exec("db-listEventObjects", q.getListQuery());

        return new ChangeFeedBase<EventObject>(res) {
            @Override
            protected Function<Map<String, Object>, EventObject> mapper() {
                return eo -> {
                    EventObject res = ProtoUtils.rethinkToProto(eo, EventObject.class);
                    return res;
                };
            }
        };
    }

    @Override
    public ListCountResponse countEventObjects(ListRequest listRequest) throws DbException {
        ListEventObjectQueryBuilder q = new ListEventObjectQueryBuilder(listRequest);
        long res = conn.exec("db-countEventObjects", q.getCountQuery());
        return ListCountResponse.newBuilder().setCount(res).build();
    }

    @Override
    public EventObject saveEventObject(SaveRequest request) throws DbException {
        EventObject msg = request.getObject();
        if (msg.getType().isEmpty()) {
            throw new IllegalArgumentException("Missing type for event object");
        }
        if (msg.getSource().isEmpty()) {
            throw new IllegalArgumentException("Missing source for event object");
        }

        Map rMap;
        if (msg.getId().isEmpty()) {
            EventObject.Builder b = msg.toBuilder().setState(State.NEW);
            b.addActivityBuilder().setModifiedTime(ProtoUtils.getNowTs()).setModifiedBy(getCurrentUser())
                    .setComment(request.getComment())
                    .addDescriptionBuilder().setType(ChangeType.CREATED);
            rMap = ProtoUtils.protoToRethink(b.build());
        } else {
            rMap = ProtoUtils.protoToRethink(msg);
        }

        List<String> labels = (List<String>) rMap.getOrDefault("label", Collections.EMPTY_LIST);
        for (int i = 0; i < labels.size(); i++) {
            labels.set(i, labels.get(i).toLowerCase());
        }

        return conn.executeInsert("db-save" + msg.getClass().getSimpleName(),
                r.table(Tables.EVENTS.name)
                        .insert(rMap)
                        // A rethink function which keeps old values for fields not allowed to be changed and updates activity log
                        .optArg("conflict", buildOptargConflictFunction(request.getComment())),
                EventObject.class
        );
    }

    @Override
    public UpdateResponse updateEventObject(UpdateRequest request) throws DbException {
        UpdateEventObjectQueryBuilder q = new UpdateEventObjectQueryBuilder(request);

        Map res = conn.exec("db-updateEventObjects", q.getUpdateQuery());
        if (res.isEmpty()) {
            return UpdateResponse.getDefaultInstance();
        }
        if ((long) res.get("inserted") != 0 || (long) res.get("errors") != 0 || (long) res.get("deleted") != 0) {
            throw new DbQueryException("Only replaced or unchanged expected from an update query. Got: " + res);
        }
        return UpdateResponse.newBuilder().setUpdated((long) res.get("replaced")).build();
    }

    @Override
    public DeleteResponse deleteEventObject(EventObject object) throws DbException {
        Map<String, Object> response = conn.exec("db-deleteEventObject",
                r.table(Tables.EVENTS.name)
                        .get(object.getId())
                        .delete()
        );
        return DeleteResponse.newBuilder().setDeleted((long) response.get("deleted") == 1).build();
    }

    @Override
    public ListLabelResponse listLabels(ListLabelRequest listLabelRequest) throws DbException {
        try (Cursor<String> res = conn.exec("db-listLabels",
                r.table(Tables.EVENTS.name)
                        .distinct().optArg("index", "label")
                        .filter(l -> l.match(listLabelRequest.getText()))
        )) {
            return ListLabelResponse.newBuilder().addAllLabel(res).build();
        }
    }

    public static ReqlFunction3 buildOptargConflictFunction(String comment) {
        return (id, old_doc, new_doc) -> r.branch(buildCheckForChanges(old_doc, new_doc),
                old_doc,
                new_doc.merge(r.hashMap("type", old_doc.g("type"))
                        .with("source", old_doc.g("source"))
                        .with("activity", old_doc.g("activity").prepend(r.hashMap("modifiedBy", getCurrentUser())
                                .with("modifiedTime", r.now())
                                .with("comment", comment)
                                .with("description", buildDescription(old_doc, new_doc))))
                )
        );
    }

    private static ReqlExpr buildCheckForChanges(ReqlExpr old_doc, ReqlExpr new_doc) {
        ReqlExpr[] checks = new ReqlExpr[changeableFields.length];

        for (int i = 0; i < changeableFields.length; i++) {
            checks[i] = old_doc.g(changeableFields[i].name).default_("").eq(new_doc.g(changeableFields[i].name).default_(""));
        }

        return r.and(checks);
    }

    private static ReqlExpr buildDescription(ReqlExpr old_doc, ReqlExpr new_doc) {
        ReqlExpr[] messages = new ReqlExpr[changeableFields.length];

        for (int i = 0; i < changeableFields.length; i++) {
            String fName = changeableFields[i].name;
            messages[i] = r.branch(old_doc.g(fName).default_("").eq(new_doc.g(fName).default_("")).not(),
                    r.branch(changeableFields[i].isArray,
                            new_doc.g(fName).default_(r.array()).setDifference(old_doc.g(fName).default_(r.array()))
                                    .map(l -> r.hashMap("type", ChangeType.ARRAY_ADD.name())
                                            .with("field", fName)
                                            .with("oldVal", "")
                                            .with("newVal", l.coerceTo("string")))
                                    .add(old_doc.g(fName).default_(r.array()).setDifference(new_doc.g(fName).default_(r.array()))
                                            .map(l -> r.hashMap("type", ChangeType.ARRAY_DEL.name())
                                                    .with("field", fName)
                                                    .with("oldVal", l.coerceTo("string"))
                                                    .with("newVal", ""))
                                    ),
                            r.array(r.hashMap("type", ChangeType.VALUE_CHANGED.name())
                                    .with("field", fName)
                                    .with("oldVal", old_doc.g(fName).default_(""))
                                    .with("newVal", new_doc.g(fName).default_("")))
                    ),
                    r.array());
        }

        return r.do_(r.array(), a -> a.add(r.args(messages)));
    }

    private static String getCurrentUser() {
        String user = EmailContextKey.email();
        if (user == null || user.isEmpty()) {
            user = "anonymous";
        }
        return user;
    }

    private static class ChangableField {
        final String name;
        final boolean isArray;

        public ChangableField(String name, boolean isArray) {
            this.name = name;
            this.isArray = isArray;
        }
    }
}
