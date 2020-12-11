package no.nb.nna.veidemann.db.queryoptimizer;

import com.google.protobuf.MessageOrBuilder;
import com.rethinkdb.gen.ast.ReqlExpr;
import no.nb.nna.veidemann.db.fieldmask.Indexes.Index;
import no.nb.nna.veidemann.db.fieldmask.PathElem;
import no.nb.nna.veidemann.db.fieldmask.RethinkDbFieldMasksQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

abstract class Snippet<T extends MessageOrBuilder> {
    enum Type {
        GET_ALL,
        GET_ALL_INDEX,
        ORDER_BY,
        ORDER_BY_INDEX,
        BETWEEN,
        BETWEEN_INDEX,
        BETWEEN_COMPOUND1_INDEX,
        BETWEEN_COMPOUND2_INDEX,
        FILTER,
        AND_FILTER,
    }

    final RethinkDbFieldMasksQueryBuilder<T> queryBuilder;
    PathElem<T> pathDef;
    String path;
    List<Object> values = new ArrayList<>();
    List<CanBeBehindCandidate> canBeBehind = new ArrayList<>();
    List<Index> bestIndexes;
    Index chosenIndex;
    Type renderType;
    Snippet<T> prev;
    Snippet<T> next;
    int priority;

    public Snippet(RethinkDbFieldMasksQueryBuilder<T> queryBuilder, String path) {
        this.queryBuilder = queryBuilder;
        this.path = path;
        pathDef = queryBuilder.getMaskedObject().getPathDef(path);
        this.bestIndexes = queryBuilder.getBestIndexes(path);
    }

    abstract ReqlExpr render(ReqlExpr qry);

    abstract void optimize(List<Snippet<T>> snippets);

    abstract void evaluateRenderType();

    public boolean hasPrev() {
        return prev != null;
    }

    public boolean hasNext() {
        return next != null;
    }

    ReqlExpr renderNext(ReqlExpr qry) {
        if (next != null) {
            return next.render(qry);
        } else {
            return qry;
        }
    }

    ReqlExpr renderAndFilterSnippets(ReqlExpr filterQry, Snippet<T> nextSnippet, ReqlExpr row) {
        while (nextSnippet != null && nextSnippet.renderType == Type.AND_FILTER) {
            filterQry = filterQry.and(nextSnippet.render(row));
            nextSnippet = nextSnippet.next;
        }
        next = nextSnippet;
        return filterQry;
    }

    class CanBeBehindCandidate {
        Class<? extends Snippet> snippetType;
        String path;
        List<Object> values;
        Index chosenIndex;

        public CanBeBehindCandidate(Snippet<T> snippet, Index chosenIndex) {
            this.snippetType = snippet.getClass();
            this.path = snippet.path;
            this.values = snippet.values;
            this.chosenIndex = chosenIndex;
        }

        public boolean isReferenceTo(Snippet<T> snippet) {
            if (snippet == null || snippetType != snippet.getClass()) return false;
            return path.equals(snippet.path) &&
                    Objects.deepEquals(values, snippet.values) &&
                    indexEqual(chosenIndex, snippet.chosenIndex);
        }

        public boolean isReferenceToIgnoringIndex(Snippet<T> snippet) {
            if (snippet == null || snippetType != snippet.getClass()) return false;
            return path.equals(snippet.path) &&
                    Objects.deepEquals(values, snippet.values);
        }

        @Override
        public String toString() {
            return "Candidate{" +
                    snippetType.getSimpleName() + "(" + path + "=" + values + ")" +
                    ", index=" + chosenIndex +
                    '}';
        }
    }

    static List<? extends Index> findEqualIndexes(List<? extends Index> x, List<? extends Index> y) {
        if (x == null || y == null) {
            return new ArrayList<>();
        }
        return x.stream()
                .filter(y::contains)
                .collect(Collectors.toList());
    }

    List<? extends Index> findEqualIndexes(Snippet<T> snippet) {
        if (bestIndexes.isEmpty() || snippet.bestIndexes.isEmpty()) {
            return new ArrayList<>();
        }
        return bestIndexes.stream()
                .filter(x -> snippet.bestIndexes.contains(x))
                .collect(Collectors.toList());
    }

    boolean isAmongBestIndexes(Index index) {
        if (index == null || bestIndexes == null || bestIndexes.isEmpty()) {
            return false;
        }
        for (Index x : bestIndexes) {
            if (indexEqual(x, index)) {
                return true;
            }
        }
        return false;
    }

    void linkNext(Snippet<T> s) {
        next = s;
        s.prev = this;
    }

    boolean indexEqual(Index x, Index y) {
        if (x == y) return true;
        if (x == null || y == null) return false;
        return x.indexName.equals(y.indexName) &&
                Objects.deepEquals(x.path, y.path);
    }

    @Override
    public String toString() {
        String indexes = bestIndexes.stream().map(i -> i.indexName).collect(Collectors.joining(", "));
        return "Snippet{" +
                getClass().getSimpleName() + "(" + path + "=" + values + ")" +
                ", renderType=" + renderType +
                ", indexes [" + indexes + "]" +
                ", chosenIndex=" + chosenIndex +
                ", prev=" + (prev == null ? "null" : prev.getClass().getSimpleName()) +
                ", next=" + (next == null ? "null" : next.getClass().getSimpleName()) +
                '}';
    }
}
