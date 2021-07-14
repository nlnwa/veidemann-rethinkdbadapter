package no.nb.nna.veidemann.db.queryoptimizer;

import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.MapObject;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.fieldmask.Indexes.Index;
import no.nb.nna.veidemann.db.fieldmask.RethinkDbFieldMasksQueryBuilder;

import java.util.List;
import java.util.stream.Collectors;

import static com.rethinkdb.RethinkDB.r;

class GetAllSnippet<T extends MessageOrBuilder> extends Snippet<T> {

    public GetAllSnippet(RethinkDbFieldMasksQueryBuilder<T> queryBuilder, String path, List<?> values, boolean valuesPreConverted) {
        super(queryBuilder, path);
        priority = 20;
        for (Object v : values) {
            if (valuesPreConverted) {
                this.values.add(v);
            } else {
                this.values.add(ProtoUtils.protoFieldToRethink(pathDef.getDescriptor(), v));
            }
        }
    }

    public GetAllSnippet(RethinkDbFieldMasksQueryBuilder<T> queryBuilder, String path, List<?> values) {
        this(queryBuilder, path, values, false);
    }

    public GetAllSnippet(RethinkDbFieldMasksQueryBuilder<T> queryBuilder, List<?> values) {
        this(queryBuilder, queryBuilder.getPrimaryIndex().path[0], values, false);
    }

    @Override
    void optimize(List<Snippet<T>> snippets) {
        for (Snippet<T> s : snippets) {
            if (s == this) {
                continue;
            }
            List<? extends Index> matchedIndexes = findEqualIndexes(bestIndexes, s.bestIndexes);
            for (Index i : matchedIndexes) {
                canBeBehind.add(new CanBeBehindCandidate(s, i));
            }
            canBeBehind.add(new CanBeBehindCandidate(s, null));
        }
    }

    @Override
    void evaluateRenderType() {
        if (!hasPrev()) {
            if (chosenIndex == null) {
                renderType = Type.FILTER;
                return;
            } else if (chosenIndex.isPrimary()) {
                renderType = Type.GET_ALL;
                return;
            } else if (chosenIndex.path.length > 1) {
                renderType = Type.FILTER;
                return;
            } else {
                renderType = Type.GET_ALL_INDEX;
                return;
            }
        }
        if (prev.renderType == Type.FILTER || prev.renderType == Type.AND_FILTER) {
            renderType = Type.AND_FILTER;
        } else {
            renderType = Type.FILTER;
        }
    }

    @Override
    ReqlExpr render(ReqlExpr qry) {
        switch (renderType) {
            case GET_ALL:
                qry = ((Table) qry).getAll(values.toArray());
                break;
            case GET_ALL_INDEX:
                if ("configRefs".equals(chosenIndex.indexName)) {
                    values = values.stream()
                            .map(c -> r.array(((MapObject) c).get("kind"), ((MapObject) c).get("id")))
                            .collect(Collectors.toList());
                }
                qry = ((Table) qry).getAll(values.toArray()).optArg("index", chosenIndex.indexName);
                break;
            case FILTER:
                qry = qry.filter(row -> renderAndFilterSnippets(asFilter(row), next, row));
                return renderNext(qry);
            case AND_FILTER:
                if (values.size() == 1) {
                    qry = r.expr(values.get(0)).eq(queryBuilder.buildGetFieldExpression(pathDef, qry));
                } else {
                    qry = r.expr(values.toArray()).contains(queryBuilder.buildGetFieldExpression(pathDef, qry));
                }
                return qry;
            case BETWEEN_COMPOUND1_INDEX:
                if (hasNext() && next.renderType == Type.BETWEEN_COMPOUND2_INDEX) {
                    qry = qry.between(r.array(values.get(0), next.values.get(0)), r.array(values.get(0), next.values.get(1)))
                            .optArg("right_bound", "closed").optArg("index", chosenIndex.indexName);
                } else if (values.size() == 1) {
                    qry = qry.between(r.array(values.get(0), r.minval()), r.array(values.get(0), r.maxval()))
                            .optArg("right_bound", "closed").optArg("index", chosenIndex.indexName);
                } else {
                    qry = qry.between(r.array(values.get(0), r.minval()), r.array(values.get(1), r.maxval()))
                            .optArg("right_bound", "closed").optArg("index", chosenIndex.indexName);
                }
                if (next.getClass() == OrderBySnippet.class) {
                    return renderNext(qry);
                } else {
                    // If next is not an orderBy snippet, then the necessary code for rendering is already done, skip to next's next.
                    return next.renderNext(qry);
                }
            case BETWEEN_INDEX:
                if (hasNext() && chosenIndex.indexName.equals("kind_label_key") && next.getClass() == LabelSnippet.class) {
                    Object labelKeyStartSpan = ((LabelSnippet<T>) next).getKeyStartSpan();
                    Object labelKeyEndSpan = ((LabelSnippet<T>) next).getKeyEndSpan();
                    qry = qry.between(r.array(values.get(0), labelKeyStartSpan), r.array(values.get(0), labelKeyEndSpan))
                            .optArg("right_bound", "closed").optArg("index", chosenIndex.indexName);
                } else {
                    if (values.size() == 1) {
                        qry = qry.between(values.get(0), values.get(0))
                                .optArg("right_bound", "closed").optArg("index", chosenIndex.indexName);
                    } else {
                        qry = qry.between(values.get(0), values.get(1))
                                .optArg("right_bound", "closed").optArg("index", chosenIndex.indexName);
                    }
                }
                break;
            default:
                throw new RuntimeException("Render type '" + renderType + "' not implemented for '" + getClass().getSimpleName() + "'");
        }
        return renderNext(qry);
    }

    ReqlExpr asFilter(ReqlExpr row) {
        if (pathDef.getDescriptor().isRepeated()) {
            if (values.size() == 1) {
                return queryBuilder.buildGetFieldExpression(pathDef, row).contains(r.expr(values.get(0)));
            } else {
                return queryBuilder.buildGetFieldExpression(pathDef, row).coerceTo("array").contains(r.args(values));
            }
        } else {
            if (values.size() == 1) {
                Object defaultValue;
                Object value = values.get(0);
                switch(pathDef.getDescriptor().getType()) {
                    case MESSAGE:
                        defaultValue = null;
                        break;
                    case ENUM:
                        defaultValue = pathDef.getDescriptor().getDefaultValue().toString();
                        break;
                    default:
                        defaultValue = pathDef.getDescriptor().getDefaultValue();
                        break;
                }
                return r.expr(value).eq(queryBuilder.buildGetFieldExpression(pathDef, row).default_(r.expr(defaultValue)));
            } else {
                return queryBuilder.buildGetFieldExpression(pathDef, row).coerceTo("array").contains(r.args(values));
            }
        }
    }
}
