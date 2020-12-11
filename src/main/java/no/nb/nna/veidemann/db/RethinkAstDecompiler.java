package no.nb.nna.veidemann.db;

import com.google.common.collect.ImmutableMap;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Datum;
import com.rethinkdb.gen.proto.TermType;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RethinkAstDecompiler {

    private static final Field termTypeField;
    private static final Field argsField;
    private static final Field datumField;
    private static final Field optargsField;

    private static final EnumSet<TermType> topLevelTypes = EnumSet.of(
            TermType.NOW, TermType.TIME, TermType.ISO8601, TermType.MINVAL, TermType.MAXVAL, TermType.MAKE_ARRAY,
            TermType.ARGS, TermType.ASC, TermType.DESC);

    private static final Map<String, String> funcNames = ImmutableMap.of(
            "makeArray", "array",
            "getField", "g"
    );

    private final Map<Integer, String> vars = new HashMap<>();
    private final AtomicInteger varCount = new AtomicInteger();
    private final String decompiled;

    static {
        try {
            termTypeField = ReqlAst.class.getDeclaredField("termType");
            termTypeField.setAccessible(true);
            argsField = ReqlAst.class.getDeclaredField("args");
            argsField.setAccessible(true);
            optargsField = ReqlAst.class.getDeclaredField("optargs");
            optargsField.setAccessible(true);
            datumField = Datum.class.getDeclaredField("datum");
            datumField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public RethinkAstDecompiler(ReqlAst ast) {
        List<AstElem> stack = new ArrayList<>();
        visit(stack, ast);
        decompiled = "r" + stack.stream().map(AstElem::toString).collect(Collectors.joining(""));
    }

    public static boolean isEqual(ReqlAst a1, ReqlAst a2) {
        return new RethinkAstDecompiler(a1).equals(new RethinkAstDecompiler(a2));
    }

    @Override
    public String toString() {
        return decompiled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RethinkAstDecompiler that = (RethinkAstDecompiler) o;
        return decompiled.equals(that.decompiled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(decompiled);
    }

    private class AstElem {
        List<AstElem> commandStack = new ArrayList<>();
        List<AstElem> paramStack = new ArrayList<>();
        List<AstElem> valueStack = new ArrayList<>();
        List<AstElem> optArgsStack = new ArrayList<>();
        String prefix = "";
        String infix = "";
        String postfix = "";

        public ReqlAst init(ReqlAst ast) {
            ReqlAst next = null;
            TermType termType = getTermType(ast);
            boolean toplevel = topLevelTypes.contains(termType);
            switch (getTermType(ast)) {
                case DATUM:
                    prefix = getDatumAsString((Datum) ast);
                    break;
                case VAR:
                    prefix = getVar((int) getDatum((Datum) getArguments(ast).get(0)));
                    break;
                case ARGS:
                case ASC:
                case DESC:
                    prefix = "r." + funcName(ast) + "(";
                    postfix = ")";
                    Arguments aa = getArguments(ast);
                    AstElem av = new AstElem();
                    visit(av.commandStack, aa.get(0));
                    valueStack.add(av);
                    break;
                case MAKE_OBJ:
                    prefix = "r.hashMap(";
                    infix = ")";
                    AtomicBoolean isFirst = new AtomicBoolean(true);
                    getOptArgs(ast).entrySet().stream().sorted(Entry.comparingByKey()).forEach(e -> {
                        AstElem opt = new AstElem();
                        if (isFirst.compareAndSet(true, false)) {
                            opt.prefix = "\"" + e.getKey() + "\", ";
                            visit(opt.valueStack, e.getValue());
                            paramStack.add(opt);
                        } else {
                            opt.prefix = ".with(\"" + e.getKey() + "\", ";
                            opt.postfix = ")";
                            visit(opt.valueStack, e.getValue());
                            optArgsStack.add(opt);
                        }
                    });
                    break;
                default:
                    for (ReqlAst a : getArguments(ast)) {
                        switch (getTermType(a)) {
                            case DATUM:
                            case MAKE_ARRAY:
                            case MAKE_OBJ:
                            case ARGS:
                            case FUNC:
                            case MINVAL:
                            case MAXVAL:
                            case NOW:
                            case TIME:
                            case ISO8601:
                            case CONTAINS:
                            case ASC:
                            case DESC:
                                visit(valueStack, a);
                                break;
                            case VAR:
                                if (toplevel) {
                                    visit(valueStack, a);
                                } else {
                                    visit(paramStack, a);
                                }
                                break;
                            default:
                                getOptArgs(ast).entrySet().stream().sorted(Entry.comparingByKey()).forEach(e -> {
                                    AstElem opt = new AstElem();
                                    opt.prefix = ".optArg(\"" + e.getKey() + "\", ";
                                    opt.postfix = ")";
                                    visit(opt.valueStack, e.getValue());
                                    optArgsStack.add(opt);
                                });

                                if (next != null) {
                                    throw new RuntimeException("Double next: " + next.getClass().getSimpleName() + " ==> " + a.getClass().getSimpleName());
                                }
                                next = a;
                        }
                    }
                    infix = (toplevel ? "r" : "") + '.' + funcName(ast) + "(";
                    postfix = ")";
            }
            return next;
        }

        @Override
        public String toString() {
            Collections.reverse(valueStack);
            String params = paramStack.stream().map(AstElem::toString).collect(Collectors.joining(", "));
            String values = valueStack.stream().map(AstElem::toString).collect(Collectors.joining(", "));
            String opts = optArgsStack.stream().map(AstElem::toString).collect(Collectors.joining(""));
            String commands = commandStack.stream().map(AstElem::toString).collect(Collectors.joining(""));
            return prefix + params + infix + values + commands + postfix + opts;
        }
    }

    class FuncElem extends AstElem {
        @Override
        public ReqlAst init(ReqlAst ast) {
            Arguments args = getArguments(ast);
            Arguments params = getArguments(args.get(0));
            if (params.size() > 1) {
                prefix = "(";
                infix = ")";
            }
            prefix += params.stream().map(d -> getVar((int) getDatum((Datum) d))).collect(Collectors.joining(", "));
            infix += " -> ";
            visit(commandStack, args.get(1));
            return null;
        }
    }

    class ContainsElem extends AstElem {
        @Override
        public ReqlAst init(ReqlAst ast) {
            infix = ".contains(";
            postfix = ")";
            Arguments ca = getArguments(ast);
            visit(paramStack, ca.get(0));
            for (int i = ca.size() - 1; i > 0; i--) {
                AstElem c = new AstElem();
                visit(c.commandStack, ca.get(i));
                valueStack.add(c);
            }
            return null;
        }

        @Override
        public String toString() {
            Collections.reverse(valueStack);
            String params = paramStack.stream().map(AstElem::toString).collect(Collectors.joining(""));
            String values = valueStack.stream().map(AstElem::toString).collect(Collectors.joining(", "));
            return prefix + params + infix + values + postfix;
        }
    }

    class BoolLogicElem extends AstElem {
        @Override
        public ReqlAst init(ReqlAst ast) {
            infix = "." + funcName(ast) + "(";
            postfix = ")";
            Arguments ca = getArguments(ast);
            visit(paramStack, ca.get(0));
            visit(commandStack, ca.get(1));
            return null;
        }

        @Override
        public String toString() {
            String params = paramStack.stream().map(AstElem::toString).collect(Collectors.joining(""));
            String commands = commandStack.stream().map(AstElem::toString).collect(Collectors.joining(""));
            return prefix + params + infix + commands + postfix;
        }
    }

    private void visit(List<AstElem> stack, ReqlAst ast) {
        while (ast != null) {
            AstElem e;
            switch (getTermType(ast)) {
                case CONTAINS:
                    e = new ContainsElem();
                    break;
                case FUNC:
                    e = new FuncElem();
                    break;
                case AND:
                case OR:
                    e = new BoolLogicElem();
                    break;
                default:
                    e = new AstElem();
            }
            ast = e.init(ast);
            stack.add(0, e);
        }
    }

    private String funcName(ReqlAst ast) {
        String f = ast.getClass().getSimpleName();
        f = f.substring(0, 1).toLowerCase() + f.substring(1);
        return funcNames.getOrDefault(f, f);
    }

    private String getVar(int i) {
        return vars.computeIfAbsent(i, k -> "p" + varCount.incrementAndGet());
    }

    private Arguments getArguments(ReqlAst ast) {
        try {
            return (Arguments) argsField.get(ast);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private TermType getTermType(ReqlAst ast) {
        try {
            return (TermType) termTypeField.get(ast);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private OptArgs getOptArgs(ReqlAst ast) {
        try {
            return (OptArgs) optargsField.get(ast);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Object getDatum(Datum datum) {
        try {
            return datumField.get(datum);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private String getDatumAsString(Datum datum) {
        Object d = getDatum(datum);
        if (d instanceof String) {
            return '"' + (String) d + '"';
        } else {
            return String.valueOf(d);
        }
    }
}
