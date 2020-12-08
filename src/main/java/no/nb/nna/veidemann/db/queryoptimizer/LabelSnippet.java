package no.nb.nna.veidemann.db.queryoptimizer;

import com.google.protobuf.MessageOrBuilder;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import no.nb.nna.veidemann.api.config.v1.Label;
import no.nb.nna.veidemann.db.fieldmask.Indexes.Index;
import no.nb.nna.veidemann.db.fieldmask.RethinkDbFieldMasksQueryBuilder;

import java.util.List;

import static com.rethinkdb.RethinkDB.r;

class LabelSnippet<T extends MessageOrBuilder> extends Snippet<T> {
    enum MatchType {
        EXACT,
        KEY_VALUE_PREFIX,
        ANY_VALUE,
        ANY_KEY,
        ANY_KEY_VALUE_PREFIX,
    }

    MatchType matchType;

    public LabelSnippet(RethinkDbFieldMasksQueryBuilder<T> queryBuilder, Label label) {
        super(queryBuilder, "meta.label");
        priority = 10;

        Object startKey;
        Object endKey;
        Object startValue;
        Object endValue;

        if (!label.getKey().isEmpty() && !label.getValue().isEmpty() && !label.getValue().endsWith("*")) {
            // Exact match
            matchType = MatchType.EXACT;
            startKey = label.getKey();
            endKey = label.getKey();
            startValue = label.getValue();
            endValue = label.getValue();
        } else if (!label.getKey().isEmpty()) {
            // Exact match on key, value ends with '*' or is empty
            if (label.getValue().endsWith("*")) {
                matchType = MatchType.KEY_VALUE_PREFIX;
                String prefix = label.getValue().substring(0, label.getValue().length() - 1);
                startKey = label.getKey();
                endKey = label.getKey();
                startValue = prefix;
                endValue = prefix + Character.MAX_VALUE;
            } else {
                matchType = MatchType.ANY_VALUE;
                startKey = label.getKey();
                endKey = label.getKey();
                startValue = r.minval();
                endValue = r.maxval();
            }
        } else {
            // Key is empty
            if (label.getValue().endsWith("*")) {
                matchType = MatchType.ANY_KEY_VALUE_PREFIX;
                String prefix = label.getValue().toLowerCase().substring(0, label.getValue().length() - 1);
                startKey = r.minval();
                endKey = r.maxval();
                startValue = prefix;
                endValue = prefix + Character.MAX_VALUE;
            } else {
                matchType = MatchType.ANY_KEY;
                startKey = r.minval();
                endKey = r.maxval();
                startValue = label.getValue();
                endValue = label.getValue();
            }
        }
        values.add(startKey);
        values.add(startValue);
        values.add(endKey);
        values.add(endValue);
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
        if (prev == null && (matchType == MatchType.ANY_KEY || matchType == MatchType.ANY_KEY_VALUE_PREFIX)
                && path.equals("meta.label")) {
            List<Index> valIdx = queryBuilder.getBestIndexes("meta.label.value");
            if (!valIdx.isEmpty()) {
                chosenIndex = valIdx.get(0);
                renderType = Type.BETWEEN_INDEX;
                return;
            }
        }

        if (prev == null) {
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
            case GET_ALL_INDEX:
                Object v = r.array(getKeyStartSpan(), getValueStartSpan());
                qry = ((Table) qry).getAll(v).optArg("index", chosenIndex.indexName);
                break;
            case FILTER:
                qry = qry.filter(row -> renderAndFilterSnippets(asFilter(row), next, row));
                return renderNext(qry);
            case AND_FILTER:
                return asFilter(qry);
            case BETWEEN_INDEX:
                if (chosenIndex.path[0].equals("meta.label.value")) {
                    qry = qry.between(getValueStartSpan(), getValueEndSpan())
                            .optArg("right_bound", "closed").optArg("index", chosenIndex.indexName);
                } else {
                    qry = qry.between(r.array(getKeyStartSpan(), getValueStartSpan()), r.array(getKeyEndSpan(), getValueEndSpan()))
                            .optArg("right_bound", "closed").optArg("index", chosenIndex.indexName);
                }
                break;
        }
        return renderNext(qry);
    }

    private ReqlExpr asFilter(ReqlExpr qry) {
        switch (matchType) {
            case EXACT:
                Object v1 = r.hashMap("key", getKeyStartSpan()).with("value", getValueStartSpan());
                qry = queryBuilder.buildGetFieldExpression(pathDef, qry).contains(v1);
                break;
            case ANY_VALUE:
                qry = queryBuilder.buildGetFieldExpression(pathDef, qry)
                        .filter(l -> l.g("key").eq(getKeyStartSpan()));
                break;
            case KEY_VALUE_PREFIX:
                qry = queryBuilder.buildGetFieldExpression(pathDef, qry)
                        .filter(l -> l.g("key").eq(getKeyStartSpan()).and(l.g("value")
                                .between(getValueStartSpan(), getValueEndSpan()).optArg("right_bound", "closed")));
                break;
            case ANY_KEY:
                qry = queryBuilder.buildGetFieldExpression(pathDef, qry)
                        .filter(l -> l.g("value").eq(getValueStartSpan()));
                break;
            case ANY_KEY_VALUE_PREFIX:
                qry = queryBuilder.buildGetFieldExpression(pathDef, qry).filter(l -> l.g("value")
                        .between(getValueStartSpan(), getValueEndSpan()).optArg("right_bound", "closed"));
                break;
        }
        return qry;
    }

    Object getKeyStartSpan() {
        return values.get(0);
    }

    Object getKeyEndSpan() {
        return values.get(2);
    }

    Object getValueStartSpan() {
        return values.get(1);
    }

    Object getValueEndSpan() {
        return values.get(3);
    }
}
