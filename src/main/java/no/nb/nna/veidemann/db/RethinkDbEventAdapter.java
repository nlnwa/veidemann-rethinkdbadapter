package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.config.v1.DeleteResponse;
import no.nb.nna.veidemann.api.config.v1.UpdateResponse;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

public class RethinkDbEventAdapter implements EventAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbEventAdapter.class);

    static final RethinkDB r  = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbEventAdapter(RethinkDbConnection conn) { this.conn = conn; }

    @Override
    public EventObject getEventObject (EventRef request) throws DbException {
       // final Tables table =


    }

    public boolean hasEventObject(EventRef request) throws DbQueryException, DbConnectionException {
          // final Tables table =
        return conn.exec("db-getEventObject",
                //r.table(table.name))
                r.table("events")
                        .getAll(request.getId()).contains()
        );
    }

    @Override
    public ChangeFeed <EventObject> listEventObjects(no.nb.nna.veidemann.api.eventhandler.v1.ListRequest request) throws DbQueryException, DbConnectionException {
        ListEventObjectQueryBuilder q = new ListEventObjectQueryBuilder(request);

        Cursor<Map<String, Object>> res = conn.exec("db-listEventObjects", q.getListQuery());

        return new ChangeFeedBase<EventObject>(res) {
            @Override
            protected Function<Map<String, Object>, EventObject> mapper() {
                return eo -> {
                    EventObject res = ProtoUtils.rethinkToPro(eo, EventObject.class);
                    return res;
                };
            }
        };
    }

    @Override
    public ListCountResponse countEventOBjects(no.nb.nna.veidemann.api.eventhandler.v1.ListRequest request) throws DbQueryException, DbConnectionException {
        ListEventObjectQueryBuilder q = new ListEventObjectQueryBuilder(request);
        long res = conn.exec("db-countEventObjects", q.getCountQuery());
        return ListCountResponse.newBuilder().setCount(res).build();
    }

    @Override
    public EventObject saveEventObject(EventObject object) throws DbException {
        return storeEventObject(object);
    }

    @Override
    public UpdateResponse updateEventObjects(UpdateRequest request) throws DbQueryException, DbConnectionException {
        UpdateEventObjectQueryBuilder q = new UpdateEventObjectQueryBuilder(request);

        Map res = conn.exec("db-updateEventObjects", q.getUpdateQuery());
        if ((long) res.get("inserted") != 0 || (long) res.get("errors") != 0 || (long) res.get("deleted") != 0) {
            throw new DbQueryException("Only replaced or unchanged expected from an update query. Got: " + res);
        }
        return UpdateResponse.newBuilder().setUpdated((long) res.get("replaced")).build();
    }

    @Override
    public DeleteResponse deleteEventObject(EventObject object) throws DbException {
        Map<String, Object> response = conn.exec("db-deleteEventObject",
                r.table(table.name)
                        .get(object.getId())
                        .delete()
        );
        return DeleteResponse.newBuilder().setDeleted((long) response.get("deleted") == 1).build();
    }

    private EventObject storeEventObject(EventObject msg) throws DbException {

    }


}
