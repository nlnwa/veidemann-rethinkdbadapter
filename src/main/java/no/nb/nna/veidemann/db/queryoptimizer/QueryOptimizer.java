package no.nb.nna.veidemann.db.queryoptimizer;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.MessageOrBuilder;
import com.rethinkdb.gen.ast.ReqlExpr;
import no.nb.nna.veidemann.api.config.v1.Label;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.db.RethinkAstDecompiler;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.db.fieldmask.Indexes.Index;
import no.nb.nna.veidemann.db.fieldmask.RethinkDbFieldMasksQueryBuilder;
import no.nb.nna.veidemann.db.queryoptimizer.Snippet.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.rethinkdb.RethinkDB.r;

public class QueryOptimizer<T extends MessageOrBuilder> {
    private static final Logger LOG = LoggerFactory.getLogger(QueryOptimizer.class);

    final RethinkDbFieldMasksQueryBuilder<T> queryBuilder;
    List<Snippet<T>> snippets = new ArrayList<>();
    final Tables table;
    Snippet<T> first;

    public QueryOptimizer(RethinkDbFieldMasksQueryBuilder<T> queryBuilder, final Tables table) {
        this.queryBuilder = queryBuilder;
        this.table = table;
    }

    public void wantIdQuery(List<? extends Object> id) {
        if (id.isEmpty()) {
            return;
        }
        Snippet<T> snippet = new GetAllSnippet<>(this.queryBuilder, id);
        for (Snippet<T> s : snippets) {
            if (s.getClass() == GetAllSnippet.class && snippet.path.equals(s.path)) {
                s.values.addAll(snippet.values);
                return;
            }
        }
        snippets.add(snippet);
    }

    public void wantOrderQuery(String path, boolean isDescending) {
        snippets.add(new OrderBySnippet<>(this.queryBuilder, path, isDescending));
    }

    public void wantMaskElem(String path, List<Object> values) {
        if ("meta.label".equals(path)) {
            values.stream()
                    .map(v -> ApiTools.buildLabel(((Map<String, String>) v).get("key"), ((Map<String, String>) v).get("value")))
                    .forEach(l -> snippets.add(new LabelSnippet<>(this.queryBuilder, l)));
            return;
        }
        for (Snippet<T> s : snippets) {
            if (s.getClass() == GetAllSnippet.class && path.equals(s.path)) {
                s.values.addAll(values);
                return;
            }
        }
        snippets.add(new GetAllSnippet<>(this.queryBuilder, path, values, true));
    }

    public void wantLabelQuery(Label value) {
        if (value == null) {
            return;
        }
        snippets.add(new LabelSnippet<>(this.queryBuilder, value));
    }

    public void wantGetAllQuery(String path, List<Object> values) {
        for (Snippet<T> s : snippets) {
            if (s.getClass() == GetAllSnippet.class && path.equals(s.path)) {
                s.values.addAll(values);
                return;
            }
        }
        snippets.add(new GetAllSnippet<>(this.queryBuilder, path, values));
    }

    public void wantGetAllQuery(String path, Object value) {
        wantGetAllQuery(path, ImmutableList.of(value));
    }

    public void wantBetweenQuery(String path, Object from, Object to) {
        snippets.add(new BetweenSnippet<>(this.queryBuilder, path, from, to));
    }

    public void wantFieldMaskQuery(RethinkDbFieldMasksQueryBuilder<T> fieldMasksQueryBuilder, T template) {
        fieldMasksQueryBuilder.elems(this, template);
    }

    public void optimize() {
        if (snippets.isEmpty()) {
            return;
        }

        for (Snippet<T> s : snippets) {
            s.optimize(snippets);
        }
        snippets.sort(Comparator.comparingInt(o -> o.priority));

        // Find first snippet
        Snippet<T> p = findBestStart();

        // Find remaining snippets
        snippets.remove(p);
        while (p != null) {
            if (p.renderType == null) {
                p.evaluateRenderType();
            }
            p = findNext(p, snippets);
        }
    }

    Snippet<T> findBestStart() {
        // Find getAll for primary index
        Optional<Snippet<T>> o = snippets.stream()
                .filter(s -> s.getClass() == GetAllSnippet.class &&
                        s.bestIndexes.size() == 1 &&
                        s.bestIndexes.get(0).isPrimary())
                .map(s -> {
                    s.chosenIndex = s.bestIndexes.get(0);
                    return s;
                })
                .findAny();
        if (o.isPresent()) {
            first = o.get();
            return first;
        }

        // Find getAll with compound index and single value and another snippet using the second part of index
        for (Snippet<T> s : snippets) {
            if (s.getClass() == GetAllSnippet.class && s.values.size() == 1) {
                for (Index i : s.bestIndexes) {
                    if (i.path.length == 2 && i.path[0].equals(s.path)) {
                        for (Snippet<T> s2 : snippets) {
                            if (s2.getClass() != GetAllSnippet.class && i.path[1].equals(s2.path)) {
                                s.chosenIndex = i;
                                s.renderType = Type.BETWEEN_COMPOUND1_INDEX;
                                s.linkNext(s2);
                                first = s;
                                snippets.remove(s);
                                s2.chosenIndex = i;
                                s2.renderType = Type.BETWEEN_COMPOUND2_INDEX;
                                snippets.remove(s2);

                                // If second part is an order by, set values
                                if (s2.getClass() == OrderBySnippet.class) {
                                    s2.values.add(r.minval());
                                    s2.values.add(r.maxval());
                                } else {
                                    // Check if this compound index could be used for sorting as well
                                    for (Snippet<T> s3 : snippets) {
                                        if (s3.getClass() == OrderBySnippet.class && s3.isAmongBestIndexes(i)) {
                                            s3.chosenIndex = i;
                                            s2.linkNext(s3);
                                            snippets.remove(s3);
                                            return s3;
                                        }
                                    }
                                }
                                return s2;
                            }
                        }
                    }
                }
            }
        }

        // Find Between, GetAll or Label with index and orderBy using same index
        for (Snippet<T> s : snippets) {
            if ((s.getClass() == BetweenSnippet.class || s.getClass() == LabelSnippet.class ||
                    (s.getClass() == GetAllSnippet.class && s.values.size() == 1)) && !s.bestIndexes.isEmpty()) {
                for (Snippet<T> s2 : snippets) {
                    if (s2.getClass() == OrderBySnippet.class) {
                        List<? extends Index> indexes = s.findEqualIndexes(s2);
                        if (!indexes.isEmpty()) {
                            s.chosenIndex = indexes.get(0);
                            s2.chosenIndex = indexes.get(0);
                            s.linkNext(s2);
                            if (s.chosenIndex.path.length == 1) {
                                s.renderType = Type.BETWEEN_INDEX;
                            } else {
                                s.renderType = Type.BETWEEN_COMPOUND1_INDEX;
                            }
                            s2.renderType = Type.ORDER_BY_INDEX;
                            snippets.remove(s);
                            snippets.remove(s2);
                            first = s;
                            return s2;
                        }
                    }
                }
            }
        }

        // Find getAll or label with index
        o = snippets.stream()
                .filter(s -> (s.getClass() == GetAllSnippet.class || s.getClass() == LabelSnippet.class) &&
                        s.bestIndexes.size() > 0 && s.bestIndexes.get(0).path.length == 1)
                .map(s -> {
                    s.chosenIndex = s.bestIndexes.get(0);
                    return s;
                })
                .findAny();
        if (o.isPresent()) {
            first = o.get();
            return first;
        }

        // Find between with index
        o = snippets.stream()
                .filter(s -> s.getClass() == BetweenSnippet.class &&
                        s.bestIndexes.size() > 0 && s.bestIndexes.get(0).path.length == 1)
                .map(s -> {
                    s.chosenIndex = s.bestIndexes.get(0);
                    return s;
                })
                .findAny();
        if (o.isPresent()) {
            first = o.get();
            return first;
        }

        // Find orderBy with index
        o = snippets.stream()
                .filter(s -> s.getClass() == OrderBySnippet.class &&
                        s.bestIndexes.size() > 0 && s.bestIndexes.get(0).path.length == 1)
                .map(s -> {
                    s.chosenIndex = s.bestIndexes.get(0);
                    return s;
                })
                .findAny();
        if (o.isPresent()) {
            first = o.get();
            return first;
        }

        // Found no snippet for which we have special handling, pick the first
        first = snippets.get(0);
        return first;
    }

    Snippet<T> findNext(Snippet<T> parent, List<Snippet<T>> candidates) {
        Snippet<T> result;
        for (Snippet<T> s : candidates) {
            Optional<Snippet<T>.CanBeBehindCandidate> match = s.canBeBehind.stream().filter(c -> {
                return c.isReferenceTo(parent);
            }).findFirst();
            if (match.isPresent()) {
                result = s;
                result.chosenIndex = match.get().chosenIndex;
                parent.next = result;
                result.prev = parent;
                snippets.remove(s);
                return result;
            }
        }

        for (Snippet<T> s : candidates) {
            Optional<Snippet<T>.CanBeBehindCandidate> match = s.canBeBehind.stream()
                    .filter(c -> c.isReferenceToIgnoringIndex(parent)).findFirst();
            if (match.isPresent()) {
                result = s;
                result.chosenIndex = match.get().chosenIndex;
                parent.next = result;
                result.prev = parent;
                snippets.remove(s);
                return result;
            }
        }
        return null;
    }

    public ReqlExpr render() {
        ReqlExpr qry = r.table(table.name);

        optimize();
        if (first == null) {
            return qry;
        }

        try {
            qry = first.render(qry);
        } catch (Throwable t) {
            List<Snippet<T>> s = new ArrayList<>();
            Snippet<T> p = first;
            while (p != null) {
                s.add(p);
                p = p.next;
            }
            String sn = s.stream().map(i -> i.toString()).collect(Collectors.joining("\n    - "));
            LOG.error("Error while optimizing: {}\nCaused by optimization for: {}\n  * Resolved snippets:\n    - {}\n  * Query: {}",
                    queryBuilder.getClass().getSimpleName(), sn, new RethinkAstDecompiler(qry).toString(), t);
            throw t;
        }


        if (LOG.isDebugEnabled()) {
            List<Snippet<T>> s = new ArrayList<>();
            Snippet<T> p = first;
            while (p != null) {
                s.add(p);
                p = p.next;
            }
            String sn = s.stream().map(i -> i.toString()).collect(Collectors.joining("\n    - "));
            LOG.debug("Optimize for: {}\n  * Resolved snippets:\n    - {}\n  * Query: {}",
                    queryBuilder.getClass().getSimpleName(), sn, new RethinkAstDecompiler(qry).toString());
        }

        return qry;
    }

}
