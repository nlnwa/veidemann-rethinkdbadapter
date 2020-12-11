package no.nb.nna.veidemann.db;

import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Datum;
import com.rethinkdb.gen.ast.Func;
import com.rethinkdb.gen.ast.MakeObj;
import com.rethinkdb.gen.ast.Var;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RethinkAstVisualizer {
    static final String SPACES = "                                                           ";

    static final Field termTypeField;
    static final Field argsField;
    static final Field datumField;
    static final Field optargsField;

    final ReqlAst ast;

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

    public RethinkAstVisualizer(ReqlAst ast) {
        this.ast = ast;
    }

    @Override
    public String toString() {
        Map<Object, String> funcVars = new HashMap<>();
        return decompile(new StringBuilder(), funcVars, ast, "", "", 3, false).toString();
    }

    private StringBuilder decompile(StringBuilder sb, Map<Object, String> funcVars, ReqlAst ast, String indent, String subIndent, int extraIndent, boolean last) {
        Arguments args = getArguments(ast);
        if (ast instanceof Func) {
            Arguments fp = getArguments(args.get(0));
            List<Datum> anonParams = getDatumArgs(fp);
            for (Datum d : anonParams) {
                Object oldParamName = getDatum(d);
                String newParamName = "p" + (funcVars.size() + 1);
                funcVars.put(oldParamName, newParamName);
                fp.add(new Datum(newParamName));
            }
        }

        if (ast instanceof Var) {
            List<Datum> anonParams = getDatumArgs(args);
            for (Datum d : anonParams) {
                Object oldParamName = getDatum(d);
                String newParamName = funcVars.get(oldParamName);
                funcVars.put(oldParamName, newParamName);
                args.add(new Datum(newParamName));
            }
        }

        List<Datum> data = getDatumArgs(args);
        String params = String.join(",", data.stream().map(d -> getDatumAsString(d)).collect(Collectors.toList()));
        if (!"".equals(params)) {
            params = " [" + params + "]";
        }

        String objIndent = makeSubObjIndent(indent, last);
        List<String> optArgsList = getOptArgs(ast).entrySet().stream().map(e -> {

            String s = e.getKey() + ": ";

            if (e.getValue() instanceof Datum) {
                s += getDatumAsString((Datum) e.getValue());
            } else if (e.getValue() instanceof MakeObj) {
                String f = decompile(new StringBuilder(), funcVars, e.getValue(), "", "", e.getKey().length() + 2, true).toString();
                s += f.substring(0, f.length() - 1);
            } else {
                s += decompile(new StringBuilder(), funcVars, e.getValue(), "", "", 0, true);
                s = s.substring(0, s.length() - 1);
            }
            return s;
        }).collect(Collectors.toList());

        String optArgs = String.join(",\n", optArgsList);

        if (!"".equals(optArgs)) {
            int paramIndentIdx = indent.length() + extraIndent + 3;
            int endParaIdx = indent.length() + extraIndent;
            String paramIndent = (objIndent + SPACES).substring(0, paramIndentIdx);
            String endParaIndent = (objIndent + SPACES).substring(0, endParaIdx);

            optArgs = String.join(", ", optArgs.split("\n"));
            optArgs = " {" + optArgs + "}";
        }

        sb.append(indent)
                .append(ast.getClass().getSimpleName())
                .append(params)
                .append(optArgs)
                .append('\n');

        for (int i = 0; i < args.size(); i++) {
            if (i < (args.size() - 1)) {
                decompile(sb, funcVars, args.get(i), subIndent + "├──", subIndent + "│  ", 0, i >= (args.size() - 1));
            } else {
                decompile(sb, funcVars, args.get(i), subIndent + "└──", subIndent + "   ", 0, i >= (args.size() - 1));
            }
        }
        return sb;
    }

    private String makeSubObjIndent(String indent, boolean last) {
        if (indent.endsWith("└──")) {
            indent = indent.substring(0, indent.length() - 3) + "   ";
        }
        indent += (last ? "   " : "│   ") + "   ";
        return indent;
    }

    Arguments getArguments(ReqlAst ast) {
        try {
            return (Arguments) argsField.get(ast);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    OptArgs getOptArgs(ReqlAst ast) {
        try {
            return (OptArgs) optargsField.get(ast);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    Object getDatum(Datum datum) {
        try {
            return datumField.get(datum);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private String getDatumAsString(Datum datum) {
        return String.valueOf(getDatum(datum));
    }

    private List<Datum> getDatumArgs(Arguments args) {
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            if (args.get(i) instanceof Datum) {
                Datum datum = (Datum) args.get(i);
                data.add(datum);
            }
        }
        data.forEach(d -> args.remove(d));
        return data;
    }
}
