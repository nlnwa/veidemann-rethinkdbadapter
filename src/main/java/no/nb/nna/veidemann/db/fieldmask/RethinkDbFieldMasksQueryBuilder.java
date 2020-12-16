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

package no.nb.nna.veidemann.db.fieldmask;

import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.MapObject;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.fieldmask.Indexes.Index;
import no.nb.nna.veidemann.db.fieldmask.MaskedObject.UpdateType;
import no.nb.nna.veidemann.db.queryoptimizer.QueryOptimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.rethinkdb.RethinkDB.r;

public abstract class RethinkDbFieldMasksQueryBuilder<T extends MessageOrBuilder> {
    private Indexes indexes = new Indexes();
    private List<String> readOnlyPaths = new ArrayList<>();
    private List<String> minimumReturnedFields = new ArrayList<>();
    private final ObjectOrMask<T> maskedObject;

    public RethinkDbFieldMasksQueryBuilder(ObjectOrMask<T> maskedObject) {
        this.maskedObject = maskedObject;
        init();
    }

    protected abstract void init();

    protected void addIndex(String indexName, String... path) {
        indexes.addIndex(indexName, path);
    }

    protected void addIgnoreCaseIndex(String indexName, String... path) {
        indexes.addIgnoreCaseIndex(indexName, path);
    }

    protected void addPrimaryIndex(String indexName, String path) {
        indexes.addPrimaryIndex(indexName, path);
    }

    public Index getPrimaryIndex() {
        return indexes.getPrimary();
    }

    public ObjectOrMask<T> getMaskedObject() {
        return maskedObject;
    }

    public Index getBestIndex() {
        return indexes.getBestIndex(maskedObject);
    }

    public List<Index> getBestIndexes() {
        return indexes.getBestIndexes(maskedObject);
    }

    public List<Index> getBestIndexes(String... path) {
        return indexes.getBestIndexes(path);
    }

    protected void addReadOnlyPath(String path) {
        readOnlyPaths.add(path);
    }

    protected void addMinimumReturnedField(String path) {
        minimumReturnedFields.add(path);
    }

    public ReqlExpr createOrderByQuery(ReqlExpr q, String fieldName, String indexName, boolean descending) {
        if (q instanceof Table && indexName != null) {
            if (descending) {
                q = q.orderBy().optArg("index",
                        r.desc(indexName));
            } else {
                q = q.orderBy().optArg("index",
                        r.asc(indexName));
            }
        } else {
            if (descending) {
                q = q.orderBy(r.desc(fieldName));
            } else {
                q = q.orderBy(r.asc(fieldName));
            }
        }
        return q;
    }

    public List createPluckQuery() {
        List p = new ArrayList(minimumReturnedFields);
        maskedObject.getMasks().children.forEach(e -> {
            innerCreatePluckQuery(p, e);
        });
        return p;
    }

    private void innerCreatePluckQuery(List p, PathElem<T> e) {
        if (maskedObject.getPathDef(e.fullName) != null) {
            p.add(e.name);
        } else {
            List cp = r.array();
            p.add(r.hashMap(e.name, cp));
            e.children.forEach(c -> innerCreatePluckQuery(cp, c));
        }
    }

    public void elems(QueryOptimizer<T> optimizer, T queryTemplate) {
        for (PathElem<T> p : maskedObject.getPaths()) {
            Object val = p.getValue(queryTemplate);
            List values = r.array();
            if (p.descriptor.isRepeated()) {
                for (Object v : (List) ProtoUtils.protoFieldToRethink(p.descriptor, val)) {
                    values.add(v);
                }
            } else {
                values.add(ProtoUtils.protoFieldToRethink(p.descriptor, val));
            }
            optimizer.wantMaskElem(p.fullName, values);
        }
    }

    public ReqlFunction1 buildFilterQuery(T queryTemplate) {
        return row -> {
            ReqlExpr e = row;
            boolean first = true;
            for (PathElem p : maskedObject.getPaths()) {
                if (first) {
                    e = innerBuildFilterQuery(row, p, queryTemplate);
                } else {
                    e = e.and(innerBuildFilterQuery(row, p, queryTemplate));
                }
                first = false;
            }
            return e;
        };
    }

    private ReqlExpr innerBuildFilterQuery(ReqlExpr exp, PathElem<T> p, T queryTemplate) {
        exp = buildGetFieldExpression(p, exp);
        Object val = p.getValue(queryTemplate);
        if (p.descriptor.isRepeated()) {
            List values = r.array();
            for (Object v : (List) ProtoUtils.protoFieldToRethink(p.descriptor, val)) {
                values.add(v);
            }
            exp = exp.contains(r.args(values));
        } else {
            exp = exp.eq(ProtoUtils.protoFieldToRethink(p.descriptor, val));
        }
        return exp;
    }

    public ReqlExpr buildGetFieldExpression(PathElem<T> p, ReqlExpr parentExpr) {
        if (!p.parent.name.isEmpty()) {
            parentExpr = buildGetFieldExpression(p.parent, parentExpr);
        }
        parentExpr = parentExpr.g(p.name);
        return parentExpr;
    }

    public ReqlFunction1 buildUpdateQuery(T object) {
        return row -> {
            MapObject p = r.hashMap();
            for (PathElem e : maskedObject.getMasks().children) {
                innerBuildUpdateQuery(row, p, e, object);
            }
            return p;
        };
    }

    private void innerBuildUpdateQuery(ReqlExpr row, Map p, PathElem<T> e, T object) {
        if (readOnlyPaths.contains(e.fullName)) {
            return;
        }

        if (e.descriptor.isRepeated()) {
            PathElem e2 = maskedObject.getPathDef(e.fullName);
            if (e2 == null) {
                p.put(e.name, ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object)));
            } else if (e2.updateType == UpdateType.REPLACE) {
                p.put(e.name, ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object)));
            } else if (e2.updateType == UpdateType.APPEND) {
                p.put(e.name, buildGetFieldExpression(e2, row).default_(r.array())
                        .setUnion(ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object))));
            } else {
                p.put(e.name, buildGetFieldExpression(e2, row).default_(r.array())
                        .setDifference(ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object))));
            }
        } else if (e.descriptor.getType() == Type.MESSAGE) {
            if (e.descriptor.isRepeated() || e.parent.name.isEmpty() || ((MessageOrBuilder) e.parent.getValue(object)).hasField(e.descriptor)) {
                if (e.descriptor.getMessageType() == Timestamp.getDescriptor()) {
                    p.put(e.name, ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object)));
                } else {
                    Map cp = r.hashMap();
                    p.put(e.name, cp);
                    e.children.forEach(c -> innerBuildUpdateQuery(row, cp, c, object));
                }
            } else {
                p.put(e.name, null);
            }
        } else {
            p.put(e.name, ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object)));
        }
    }
}
