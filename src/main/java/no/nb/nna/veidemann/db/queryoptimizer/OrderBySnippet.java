package no.nb.nna.veidemann.db.queryoptimizer;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;
import com.rethinkdb.gen.ast.ReqlExpr;
import no.nb.nna.veidemann.db.fieldmask.Indexes.Index;
import no.nb.nna.veidemann.db.fieldmask.RethinkDbFieldMasksQueryBuilder;

import java.util.List;

import static com.rethinkdb.RethinkDB.r;

class OrderBySnippet<T extends MessageOrBuilder> extends Snippet<T> {
    boolean isDescending;

    public OrderBySnippet(RethinkDbFieldMasksQueryBuilder<T> queryBuilder, String path, boolean isDescending) {
        super(queryBuilder, path);
        priority = 100;
        this.path = path;
        this.isDescending = isDescending;
    }

    @Override
    void optimize(List<Snippet<T>> snippets) {
        for (Snippet<T> s : snippets) {
            if (s == this) {
                continue;
            }
            if (s.getClass() != GetAllSnippet.class) {
                for (Index i : findEqualIndexes(bestIndexes, s.bestIndexes)) {
                    canBeBehind.add(new CanBeBehindCandidate(s, i));
                }
            }
            canBeBehind.add(new CanBeBehindCandidate(s, null));
        }
    }

    @Override
    void evaluateRenderType() {
        if (!hasPrev()) {
            if (chosenIndex != null) {
                renderType = Type.ORDER_BY_INDEX;
            } else {
                renderType = Type.ORDER_BY;
            }
            return;
        }
        if ((prev.renderType == Type.BETWEEN_INDEX || prev.renderType == Type.BETWEEN_COMPOUND2_INDEX)
                && indexEqual(prev.chosenIndex, chosenIndex)) {
            renderType = Type.ORDER_BY_INDEX;
            return;
        }
        renderType = Type.ORDER_BY;
    }

    @Override
    ReqlExpr render(ReqlExpr qry) {
        switch (renderType) {
            case ORDER_BY:
                long ignoreCaseIndexCount = bestIndexes.stream()
                        .filter(i -> i.isIgnoreCase() && i.path.length == 1 && i.path[0].equals(path)
                                && pathDef.getDescriptor().getType() == FieldDescriptor.Type.STRING)
                        .count();
                if (isDescending) {
                    if (ignoreCaseIndexCount > 0) {
                        qry = qry.orderBy(r.desc(row -> queryBuilder.buildGetFieldExpression(pathDef, row).downcase()));
                    } else {
                        qry = qry.orderBy(r.desc(row -> queryBuilder.buildGetFieldExpression(pathDef, row)));
                    }
                } else {
                    if (ignoreCaseIndexCount > 0) {
                        qry = qry.orderBy(row -> queryBuilder.buildGetFieldExpression(pathDef, row).downcase());
                    } else {
                        qry = qry.orderBy(row -> queryBuilder.buildGetFieldExpression(pathDef, row));
                    }
                }
                break;
            case ORDER_BY_INDEX:
            case BETWEEN_COMPOUND2_INDEX:
                if (isDescending) {
                    qry = qry.orderBy().optArg("index", r.desc(chosenIndex.indexName));
                } else {
                    qry = qry.orderBy().optArg("index", chosenIndex.indexName);
                }
                break;
            default:
                throw new RuntimeException("Render type '" + renderType + "' not implemented for '" + getClass().getSimpleName() + "'");
        }
        return renderNext(qry);
    }
}
