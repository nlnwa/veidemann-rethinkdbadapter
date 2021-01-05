package no.nb.nna.veidemann.db.queryoptimizer;

import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import com.rethinkdb.gen.ast.Maxval;
import com.rethinkdb.gen.ast.Minval;
import com.rethinkdb.gen.ast.ReqlExpr;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.fieldmask.Indexes.Index;
import no.nb.nna.veidemann.db.fieldmask.RethinkDbFieldMasksQueryBuilder;

import java.time.OffsetDateTime;
import java.util.List;

import static com.rethinkdb.RethinkDB.r;

class BetweenSnippet<T extends MessageOrBuilder> extends Snippet<T> {
    public BetweenSnippet(RethinkDbFieldMasksQueryBuilder<T> queryBuilder, String path, Object from, Object to) {
        super(queryBuilder, path);
        priority = 20;
        if (from == null || from.equals(Timestamp.getDefaultInstance())) {
            values.add(r.minval());
        } else {
            values.add(ProtoUtils.protoFieldToRethink(pathDef.getDescriptor(), from));
        }
        if (to == null || to.equals(Timestamp.getDefaultInstance())) {
            values.add(r.maxval());
        } else {
            values.add(ProtoUtils.protoFieldToRethink(pathDef.getDescriptor(), to));
        }
    }

    @Override
    void optimize(List<Snippet<T>> snippets) {
        for (Snippet<T> s : snippets) {
            if (s == this) {
                continue;
            }
            if (s.getClass() != OrderBySnippet.class) {
                for (Index i : findEqualIndexes(bestIndexes, s.bestIndexes)) {
                    canBeBehind.add(new CanBeBehindCandidate(s, i));
                }
            }
            canBeBehind.add(new CanBeBehindCandidate(s, null));
        }
    }

    @Override
    void evaluateRenderType() {
        if (prev == null && chosenIndex != null) {
            renderType = Type.BETWEEN_INDEX;
        } else if (prev != null && (prev.renderType == Type.FILTER || prev.renderType == Type.AND_FILTER)) {
            renderType = Type.AND_FILTER;
        } else {
            renderType = Type.FILTER;
        }
    }

    @Override
    ReqlExpr render(ReqlExpr qry) {
        switch (renderType) {
            case BETWEEN_COMPOUND2_INDEX:
                return qry;
            case FILTER:
                qry = qry.filter(row -> renderAndFilterSnippets(asFilter(row), next, row));
                return renderNext(qry);
            case AND_FILTER:
                return asFilter(qry);
            case BETWEEN:
                qry = qry.between(values.get(0), values.get(1));
                break;
            case BETWEEN_INDEX:
                qry = qry.between(values.get(0), values.get(1)).optArg("index", chosenIndex.indexName);
                break;
            default:
                throw new RuntimeException("Render type '" + renderType + "' not implemented for '" + getClass().getSimpleName() + "'");
        }
        return renderNext(qry);
    }

    private ReqlExpr asFilter(ReqlExpr qry) {
        if (values.get(0) instanceof OffsetDateTime && values.get(1) instanceof OffsetDateTime) {
            qry = queryBuilder.buildGetFieldExpression(pathDef, qry).during(values.get(0), values.get(1));
        } else if (values.get(0) instanceof OffsetDateTime && values.get(1) instanceof Maxval) {
            qry = queryBuilder.buildGetFieldExpression(pathDef, qry).ge(values.get(0));
        } else if (values.get(0) instanceof Minval && values.get(1) instanceof OffsetDateTime) {
            qry = queryBuilder.buildGetFieldExpression(pathDef, qry).lt(values.get(1));
        } else {
            qry = queryBuilder.buildGetFieldExpression(pathDef, qry).ge(values.get(0))
                    .and(queryBuilder.buildGetFieldExpression(pathDef, qry).lt(values.get(1)));
        }
        return qry;
    }
}
