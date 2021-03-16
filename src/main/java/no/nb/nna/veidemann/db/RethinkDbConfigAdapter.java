package no.nb.nna.veidemann.db;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigObject.SpecCase;
import no.nb.nna.veidemann.api.config.v1.ConfigObjectOrBuilder;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.DeleteResponse;
import no.nb.nna.veidemann.api.config.v1.GetLabelKeysRequest;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.LabelKeysResponse;
import no.nb.nna.veidemann.api.config.v1.ListCountResponse;
import no.nb.nna.veidemann.api.config.v1.LogLevels;
import no.nb.nna.veidemann.api.config.v1.UpdateRequest;
import no.nb.nna.veidemann.api.config.v1.UpdateResponse;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.fieldmask.ObjectPathAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

public class RethinkDbConfigAdapter implements ConfigAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbConfigAdapter.class);

    private static final ObjectPathAccessor<ConfigObjectOrBuilder> OBJECT_PATH_ACCESSOR = new ObjectPathAccessor<>(ConfigObject.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbConfigAdapter(RethinkDbConnection conn) {
        this.conn = conn;
    }

    @Override
    public ConfigObject getConfigObject(ConfigRef request) throws DbException {
        final Tables table = getTableForKind(request.getKind());

        Map<String, Object> response = conn.exec("db-getConfigObject",
                r.table(table.name)
                        .get(request.getId())
        );

        if (response == null) {
            return null;
        }

        return ProtoUtils.rethinkToProto(response, ConfigObject.class);
    }

    public boolean hasConfigObject(ConfigRef request) throws DbQueryException, DbConnectionException {
        final Tables table = getTableForKind(request.getKind());

        return conn.exec("db-getConfigObject",
                r.table(table.name)
                        .getAll(request.getId()).contains()
        );
    }

    @Override
    public ChangeFeed<ConfigObject> listConfigObjects(no.nb.nna.veidemann.api.config.v1.ListRequest request) throws DbQueryException, DbConnectionException {
        ListConfigObjectQueryBuilder q = new ListConfigObjectQueryBuilder(request);

        Object res = conn.exec("db-listConfigObjects", q.getListQuery());
        return new ChangeFeedBase<>(res) {
            @Override
            protected Function<Map<String, Object>, ConfigObject> mapper() {
                return co -> {
                    return ProtoUtils.rethinkToProto(co, ConfigObject.class);
                };
            }
        };
    }

    @Override
    public ListCountResponse countConfigObjects(no.nb.nna.veidemann.api.config.v1.ListRequest request) throws DbQueryException, DbConnectionException {
        ListConfigObjectQueryBuilder q = new ListConfigObjectQueryBuilder(request);
        long res = conn.exec("db-countConfigObjects", q.getCountQuery());
        return ListCountResponse.newBuilder().setCount(res).build();
    }

    @Override
    public ConfigObject saveConfigObject(ConfigObject object) throws DbException {
        object = ensureKindAndApiVersion(object);

        return storeConfigObject(object);
    }

    @Override
    public UpdateResponse updateConfigObjects(UpdateRequest request) throws DbQueryException, DbConnectionException {
        checkConfigRefKind(request.getUpdateTemplate(), true);

        UpdateConfigObjectQueryBuilder q = new UpdateConfigObjectQueryBuilder(request);

        Map res = conn.exec("db-updateConfigObjects", q.getUpdateQuery());
        if ((long) res.get("inserted") != 0 || (long) res.get("errors") != 0 || (long) res.get("deleted") != 0) {
            throw new DbQueryException("Only replaced or unchanged expected from an update query. Got: " + res);
        }
        return UpdateResponse.newBuilder().setUpdated((long) res.get("replaced")).build();
    }

    @Override
    public DeleteResponse deleteConfigObject(ConfigObject object) throws DbException {
        final Tables table = getTableForKind(object.getKind());

        switch (object.getKind()) {
            case browserScript:
                checkDependencies(object, Kind.browserConfig, "browserConfig.scriptRef");
                checkDependencies(object, Kind.crawlJob, "crawlJob.scopeScriptRef");
                break;
            case crawlEntity:
                checkDependencies(object, Kind.seed, "seed.entityRef");
                break;
            case crawlJob:
                checkDependencies(object, Kind.seed, "seed.jobRef");
                break;
            case crawlScheduleConfig:
                checkDependencies(object, Kind.crawlJob, "crawlJob.scheduleRef");
                break;
            case politenessConfig:
                checkDependencies(object, Kind.crawlConfig, "crawlConfig.politenessRef");
                break;
            case browserConfig:
                checkDependencies(object, Kind.crawlConfig, "crawlConfig.browserConfigRef");
                break;
            case crawlConfig:
                checkDependencies(object, Kind.crawlJob, "crawlJob.crawlConfigRef");
            case collection:
                checkDependencies(object, Kind.crawlConfig, "crawlConfig.collectionRef");
                break;
            case crawlHostGroupConfig:
                if (object.getId().equals("chg-default")) {
                    throw new DbQueryException("Removal of default Crawl Host Group Config not allowed");
                }
        }

        Map<String, Object> response = conn.exec("db-deleteConfigObject",
                r.table(table.name)
                        .get(object.getId())
                        .delete()
        );
        return DeleteResponse.newBuilder().setDeleted((long) response.get("deleted") == 1).build();
    }

    @Override
    public LabelKeysResponse getLabelKeys(GetLabelKeysRequest request) throws DbQueryException, DbConnectionException {
        Tables table = getTableForKind(request.getKind());

        try (Cursor<String> res = conn.exec("db-getLabelKeys",
                r.table(table.name)
                        .distinct().optArg("index", "kind_label_key")
                        .filter(k1 -> k1.nth(0).eq(request.getKind().name()))
                        .map(k2 -> k2.nth(1))
        )) {
            return LabelKeysResponse.newBuilder().addAllKey(res).build();
        }
    }

    public static ConfigObject ensureKindAndApiVersion(ConfigObject co) {
        if (co.getApiVersion().isEmpty()) {
            throw new IllegalArgumentException("apiVersion can not be empty");
        }

        if (co.getKind() == Kind.undefined) {
            if (co.getSpecCase() == SpecCase.SPEC_NOT_SET) {
                throw new IllegalArgumentException("Missing kind");
            } else {
                // Set kind from spec
                co = co.toBuilder().setKind(Kind.forNumber(co.getSpecCase().getNumber())).build();
            }
        }

        if (co.getSpecCase() != SpecCase.SPEC_NOT_SET && co.getKind().getNumber() != co.getSpecCase().getNumber()) {
            throw new IllegalArgumentException("Mismatch between kind and spec: " + co.getKind() + " != " + co.getSpecCase());
        }
        return co;
    }

    @Override
    public LogLevels getLogConfig() throws DbException {
        Map<String, Object> response = conn.exec("get-logconfig",
                r.table(Tables.SYSTEM.name)
                        .get("log_levels")
                        .pluck("logLevel")
        );

        return ProtoUtils.rethinkToProto(response, LogLevels.class);
    }

    @Override
    public LogLevels saveLogConfig(LogLevels logLevels) throws DbException {
        @SuppressWarnings("unchecked")
        Map<String, Object> doc = ProtoUtils.protoToRethink(logLevels);
        doc.put("id", "log_levels");
        return conn.executeInsert("save-logconfig",
                r.table(Tables.SYSTEM.name)
                        .insert(doc)
                        .optArg("conflict", "replace"),
                LogLevels.class
        );
    }

    static Tables getTableForKind(Kind kind) {
        switch (kind) {
            case undefined:
                throw new IllegalArgumentException("Missing kind");
            case seed:
                return Tables.SEEDS;
            case crawlEntity:
                return Tables.CRAWL_ENTITIES;
            default:
                return Tables.CONFIG;
        }
    }

    private ConfigObject storeConfigObject(ConfigObject msg) throws DbException {
        final Tables table = getTableForKind(msg.getKind());

        checkConfigRefKind(msg, false);

        FieldDescriptor metaField = msg.getDescriptorForType().findFieldByName("meta");
        @SuppressWarnings("unchecked")
        Map<String, Object> rMap = ProtoUtils.protoToRethink(msg);

        if (metaField == null) {
            throw new IllegalArgumentException("Missing meta");
        } else {
            // Check that name is set if this is a new object
            if (!rMap.containsKey("id") && (!rMap.containsKey("meta") || !((Map) rMap.get("meta")).containsKey("name"))) {
                throw new IllegalArgumentException("Trying to store a new " + msg.getClass().getSimpleName()
                        + " object, but meta.name is not set.");
            }

            rMap.put("meta", updateMeta((Map) rMap.get("meta")));

            return conn.executeInsert("db-save" + msg.getClass().getSimpleName(),
                    r.table(table.name)
                            .insert(rMap)
                            // A rethink function which copies created and createby from old doc,
                            // and copies name if not existent in new doc
                            .optArg("conflict", (id, old_doc, new_doc) -> new_doc.merge(
                                    r.hashMap("meta", r.hashMap()
                                            .with("name", r.branch(new_doc.g("meta").hasFields("name"),
                                                    new_doc.g("meta").g("name"), old_doc.g("meta").g("name")))
                                            .with("created", old_doc.g("meta").g("created"))
                                            .with("createdBy", old_doc.g("meta").g("createdBy"))
                                    ))),
                    ConfigObject.class
            );
        }
    }

    @SuppressWarnings("unchecked")
    private Map updateMeta(Map meta) {
        if (meta == null) {
            meta = r.hashMap();
        }

        String user = EmailContextKey.email();
        if (user == null || user.isEmpty()) {
            user = "anonymous";
        }

        if (!meta.containsKey("created")) {
            meta.put("created", r.now());
            meta.put("createdBy", user);
        }

        meta.put("lastModified", r.now());
        meta.put("lastModifiedBy", user);

        return meta;
    }

    /**
     * Check references to Config object.
     *
     * @param messageToCheck     the config message which other objects might refer.
     * @param dependentKind      the config message kind which might have a dependency to the object to check.
     * @param dependentFieldName the field name in the dependent message which might contain reference to the id field
     *                           in the object to check.
     * @throws IllegalStateException if there are dependencies.
     */
    private void checkDependencies(ConfigObject messageToCheck, Kind dependentKind,
                                   String dependentFieldName) throws DbException {

        no.nb.nna.veidemann.api.config.v1.ListRequest.Builder request = no.nb.nna.veidemann.api.config.v1.ListRequest.newBuilder()
                .setKind(dependentKind);
        OBJECT_PATH_ACCESSOR.setValue(dependentFieldName, request.getQueryTemplateBuilder(),
                ConfigRef.newBuilder().setKind(messageToCheck.getKind()).setId(messageToCheck.getId()).build());
        request.getQueryMaskBuilder().addPaths(dependentFieldName);

        long dependencyCount = conn.exec("db-checkDependency",
                new ListConfigObjectQueryBuilder(request.build()).getCountQuery());

        if (dependencyCount > 0) {
            throw new DbQueryException("Can't delete " + messageToCheck.getKind()
                    + ", there are " + dependencyCount + " " + dependentKind
                    + "(s) referring it");
        }
    }

    /**
     * Ensure all configRefs has correct kind and ref exists.
     *
     * @param object object to test
     * @param update if true, this is a partial update and required configRefs are not checked for existence
     * @throws DbQueryException
     * @throws DbConnectionException
     */
    void checkConfigRefKind(ConfigObject object, boolean update) throws DbQueryException, DbConnectionException {
        switch (object.getKind()) {
            case crawlEntity:
                break;
            case seed:
                checkConfigRefKind("entityRef", object.getSeed().getEntityRef(), Kind.crawlEntity, true, update);
                for (ConfigRef cr : object.getSeed().getJobRefList()) {
                    checkConfigRefKind("jobRef", cr, Kind.crawlJob, true, update);
                }
                break;
            case crawlJob:
                checkConfigRefKind("scheduleRef", object.getCrawlJob().getScheduleRef(), Kind.crawlScheduleConfig, false, update);
                checkConfigRefKind("crawlConfigRef", object.getCrawlJob().getCrawlConfigRef(), Kind.crawlConfig, true, update);
                checkConfigRefKind("scopeScriptRef", object.getCrawlJob().getScopeScriptRef(), Kind.browserScript, true, update);
                break;
            case crawlConfig:
                checkConfigRefKind("collectionRef", object.getCrawlConfig().getCollectionRef(), Kind.collection, true, update);
                checkConfigRefKind("browserConfigRef", object.getCrawlConfig().getBrowserConfigRef(), Kind.browserConfig, true, update);
                checkConfigRefKind("politenessRef", object.getCrawlConfig().getPolitenessRef(), Kind.politenessConfig, true, update);
            case crawlScheduleConfig:
                break;
            case browserConfig:
                for (ConfigRef cr : object.getBrowserConfig().getScriptRefList()) {
                    checkConfigRefKind("scriptRef", cr, Kind.browserScript, true, update);
                }
                break;
            case politenessConfig:
                break;
            case browserScript:
                break;
            case crawlHostGroupConfig:
                break;
            case roleMapping:
                break;
            case collection:
                break;
        }

    }

    private void checkConfigRefKind(String fieldName, ConfigRef configRef, Kind expectedKind, boolean mustBePresent, boolean update)
            throws DbQueryException, DbConnectionException {
        if (configRef == ConfigRef.getDefaultInstance()) {
            if (mustBePresent && !update) {
                throw new IllegalArgumentException("Reference missing. The field '" + fieldName + "' must have a reference of kind '" + expectedKind + "'");
            }
            return;
        }
        if (configRef.getKind() != expectedKind) {
            throw new IllegalArgumentException(fieldName + " has wrong kind: " + configRef.getKind());
        }
        if (!hasConfigObject(configRef)) {
            throw new IllegalArgumentException("Reference with kind '" + configRef.getKind() + "' and id '" + configRef.getId() + "' doesn't exist");
        }
    }
}
