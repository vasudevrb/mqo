package batch;

import batch.data.BatchedQuery;
import batch.data.Operator;
import batch.data.Predicate;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import common.Configuration;
import common.Evaluator;
import common.QueryExecutor;
import common.Utils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static batch.data.Operator.Type.AND;
import static batch.data.Operator.Type.OR;
import static common.QueryUtils.*;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.replace;
import static org.apache.commons.lang3.StringUtils.splitByWholeSeparator;

public class QueryBatcher {

    private final Normaliser normaliser;
    private final Configuration configuration;
    private final QueryExecutor validator;
    private final Evaluator evaluator;

    //TODO Add support for 'SELECT * FROM' queries

    public QueryBatcher(Configuration configuration, QueryExecutor validator) {
        this.validator = validator;
        this.configuration = configuration;
        this.normaliser = new Normaliser();
        this.evaluator = new Evaluator();
    }

    public static <K, V> Map<V, K> inverseMap(Map<K, V> sourceMap) {
        return sourceMap.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey,
                        (a, b) -> a) //if sourceMap has duplicate values, keep only first
        );
    }

    public List<BatchedQuery> batch(List<String> queries) {
        ArrayList<BatchedQuery> batchedQueries = new ArrayList<>();
        Map<String, SqlNode> batchedQueryNodes = new HashMap<>();

        if (queries.size() == 0 || queries.size() == 1) {
            return batchedQueries;
        }

        SqlNode n1 = validator.validate(queries.get(0));
        SqlNode n2 = validator.validate(queries.get(1));
        if (canMerge(n1, n2)) {
            batchedQueries.add(new BatchedQuery(getCombinedQueryString(n1, n2), asList(0, 1), asList(n1, n2)));
        } else {
            batchedQueries.add(new BatchedQuery(getQueryString(n1), asList(0), asList(n1)));
            batchedQueries.add(new BatchedQuery(getQueryString(n2), asList(1), asList(n2)));
        }
        int k = 2;

        while (k < queries.size()) {
            boolean added = false;
            SqlNode n3 = validator.validate(queries.get(k));
            for (int l = batchedQueries.size() - 1; l >= 0; l--) {
                BatchedQuery bq = batchedQueries.get(l);

                SqlNode n4;
                if (batchedQueryNodes.containsKey(bq.sql)) {
                    n4 = batchedQueryNodes.get(bq.sql);
                } else {
                    n4 = validator.validate(bq.sql);
                    batchedQueryNodes.put(bq.sql, n4);
                }

                if (canMerge(n3, n4)) {
                    added = true;
                    batchedQueries.remove(l);
                    batchedQueryNodes.remove(bq.sql);

                    bq.sql = getCombinedQueryString(n3, n4);
                    bq.indexes.add(k);
                    bq.parts.add(n3);
                    batchedQueries.add(bq);
                    break;
                }
            }

            if (!added) {
                batchedQueries.add(new BatchedQuery(getQueryString(n3), asList(k), asList(n3)));
            }
            k++;
        }

        return batchedQueries.stream().filter(bq -> bq.indexes.size() > 1).collect(Collectors.toList());
    }

    public void unbatchResults3(BatchedQuery bq, ResultSet rs) {
        ArrayList<String> cnfs = new ArrayList<>();
        List<Map<String, String>> varMap = new ArrayList<>();
        List<List<Predicate>> preds = new ArrayList<>();
        for (SqlNode node : bq.parts) {
            Normaliser.WhereClause cnfQ1 = normaliser.getCNF(normaliser.getBooleanRepn(where(node)));
            String cnfString = cnfQ1.booleanRepn;
            cnfs.add(cnfString);
            varMap.add(cnfQ1.getCleanMap());
            preds.add(extractPredicates(cnfQ1.asString().replace("`", "")).stream()
                    .flatMap(Collection::stream)
                    .filter(pr -> !pr.isJoin)
                    .collect(Collectors.toList()));
        }


        for (int i = 0; i < varMap.size(); i++) {
            varMap.set(i, inverseMap(varMap.get(i)));
        }

        final List<String> columnNames = new ArrayList<>();
        final List<List<Object>> table = new ArrayList<>();

        try {
            columnNames.addAll(getColumnNames(rs));
            columnNames.forEach(cn -> table.add(new ArrayList<>()));
            while (rs.next()) {
                for (int i = 0; i < columnNames.size(); i++) {
                    table.get(i).add(rs.getObject(i + 1));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (columnNames.isEmpty()) {
            System.out.println("Column names is empty. Returning out of the function...");
            return;
        }

        String[] dets_all = new String[cnfs.size()];
        for (int i = 0; i < cnfs.size(); i++) {
            dets_all[i] = cnfs.get(i);
            dets_all[i] = replace(dets_all[i], "&", "&&");
            dets_all[i] = replace(dets_all[i], "|", "||");
        }

        evaluator.set(dets_all);

        List<List<String>> tabInfo = Collections.synchronizedList(new ArrayList<>());
        List<List<List<Boolean>>> tb = new ArrayList<>();
        for (List<Predicate> prs : preds) {

            List<String> colInfo = new ArrayList<>();
            for (Predicate pr : prs) {
                colInfo.add(varMap.stream().filter(x -> x.containsKey(pr.toString())).findFirst().get().get(pr.toString()));
            }
            tabInfo.add(colInfo);

            List<List<Boolean>> colBools = Collections.synchronizedList(new ArrayList<>());
            prs.parallelStream()
                    .filter(pr -> columnNames.contains(pr.getShortName()))
                    .map(pr -> table.get(columnNames.indexOf(pr.getShortName())).parallelStream().map(pr::matches).collect(Collectors.toList()))
                    .forEach(colBools::add);

            tb.add(colBools);
        }

        int[] count = new int[cnfs.size()];

        final DefaultMapContext context = new DefaultMapContext();

        for (int i = 0; i < tb.size(); i++) {
            List<String> tInfo = tabInfo.get(i);
            List<List<Boolean>> tl = tb.get(i);

            for (int j = 0; j < tl.get(0).size(); j++) {
                for (int k = 0; k < tl.size(); k++) {
                    context.set(tInfo.get(k), tl.get(k).get(j));
                }

                if (evaluator.evaluate2(i, context)) {
                    count[i] += 1;
                }
            }
        }

        System.out.println("Unbatched row count is " + Arrays.toString(count));
    }

    private List<String> getColumnNames(ResultSet rs) throws SQLException {
        List<String> names = new ArrayList<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            names.add(rs.getMetaData().getColumnName(i));
        }
        return names;
    }

    private String getQueryString(SqlNode n1) {
        String query = "SELECT " + String.join(", ", selectList(n1)) + " FROM " + getFromString(n1) + " WHERE " + where(n1);
        return replace(query, "`", "\"");
    }

    private String getCombinedQueryString(SqlNode n1, SqlNode n2) {
        List<String> select1 = selectList(n1);
        List<String> select2 = selectList(n2);

        Operator combinedWhere = build(n1, n2);

        Set<String> selectSet = new HashSet<>();
        selectSet.addAll(select1);
        selectSet.addAll(select2);
        selectSet.addAll(combinedWhere.getAllPredicateNames(s -> !isTableName(s)));
        selectSet.addAll(getWherePredicateNames(n1));
        selectSet.addAll(getWherePredicateNames(n2));

        String combinedQuery = recreateQuery(n1, combinedWhere.toString());
        return replace(combinedQuery, "`", "\"");
    }

    //NOTE: Where must be of a single query. Combined will not work because of flatMap
    private List<String> getWherePredicateNames(SqlNode n) {
        Normaliser.WhereClause cnfQ1 = normaliser.getCNF(normaliser.getBooleanRepn(where(n)));
        String cnfString = cnfQ1.asString().replace("`", "\"");
        return extractPredicates(cnfString)
                .stream()
                .flatMap(Collection::stream)
                .filter(pr -> !isTableName(pr.getValue())) //Remove join predicates
                .map(Predicate::getName)
                .collect(Collectors.toList());
    }

    public boolean canMerge(SqlNode node1, SqlNode node2) {
        List<String> from1 = from(node1);
        List<String> from2 = from(node2);
        return from1.size() == from2.size() && from1.containsAll(from2);
    }

    public Operator build(SqlNode sqlNode1, SqlNode sqlNode2) {
        String s1 = ((SqlSelect) sqlNode1).getWhere().toString();
        String s2 = ((SqlSelect) sqlNode2).getWhere().toString();

        List<List<Predicate>> pr = extractPredicates(doOR(s1, s2));
        pr = clean(pr);

        Operator opAnd = new Operator(AND);
        for (List<Predicate> preds : pr) {
            Operator opOr = new Operator(OR);
            buildCoveringPredicate2(opOr, preds);
            if (opOr.terms.size() == 1) opAnd.addTerm(opOr.terms.get(0));
            else if (opOr.terms.size() > 1) opAnd.addTerm(opOr);
        }

        return opAnd;
    }

    private List<List<Predicate>> clean(List<List<Predicate>> pr) {
        //Filter only those conditions that have the same name
        for (int i = pr.size() - 1; i >= 0; i--) {
            List<Predicate> preds = pr.get(i);
            boolean shouldRemove = preds.stream().map(Predicate::getName).collect(Collectors.toSet()).size() > 1;
            if (shouldRemove) {
                pr.remove(i);
            }
        }

        //Remove duplicates
        Set<List<Predicate>> set = new HashSet<>(pr);
        pr.clear();
        pr.addAll(set);

        //Remove lists with only 1 predicate
        return pr.stream().filter(l -> l.size() > 1).collect(Collectors.toList());
    }

    private String doOR(String s1, String s2) {
        Normaliser.WhereClause w1 = normaliser.getCNF(normaliser.getBooleanRepn(s1));
        Normaliser.WhereClause w2 = normaliser.getCNF(normaliser.getBooleanRepn(s2));

        String cnf = w1.booleanRepn;
        String c2 = w2.booleanRepn;

        StringBuilder finalStrBuilder = new StringBuilder();
        for (String part1 : splitByWholeSeparator(replace(cnf, "(", "").replace(")", ""), "&")) {
            for (String part2 : splitByWholeSeparator(replace(c2, "(", "").replace(")", ""), "&")) {
                finalStrBuilder.append("(").append(part1).append(" | ").append(part2).append(")");
                finalStrBuilder.append(" & ");
            }
        }

        String finalStr = finalStrBuilder.toString();


        finalStr = finalStr.substring(0, finalStr.length() - 3).replace("|", "OR").replace("&", "AND");
        for (String splitAnd : splitByWholeSeparator(replace(finalStr, "(", "").replace(")", ""), " AND ")) {
            String[] splitOr = splitByWholeSeparator(splitAnd, " OR ");
            for (String orp : splitOr) {
                String val = w1.map.get(orp.trim()) != null ? w1.map.get(orp.trim()) : w2.map.get(orp.trim());
                finalStr = finalStr.replace(orp.trim(), val);
            }
        }
        return finalStr;
    }

    public void buildCoveringPredicate2(Operator operator, List<Predicate> predicates) {
        if (predicates.isEmpty()) return;

        if (predicates.size() == 1) {
            operator.addTerm(predicates.get(0));
        } else if (predicates.size() == 2) {
            buildCoveringPredicate2(predicates.get(0), predicates.get(1), operator);
        } else if (predicates.size() == 3) {
            buildCoveringPredicate2(predicates.get(0), predicates.get(1), operator);
            if (operator.terms.size() > 1) {
                operator.terms.clear();
            } else if (operator.terms.size() == 1) {
                operator.terms.add(predicates.get(2));
                return;
            }

            buildCoveringPredicate2(predicates.get(0), predicates.get(2), operator);
            if (operator.terms.size() > 1) {
                operator.terms.clear();
            } else if (operator.terms.size() == 1) {
                operator.terms.add(predicates.get(1));
                return;
            }

            buildCoveringPredicate2(predicates.get(1), predicates.get(2), operator);
            if (operator.terms.size() > 1) {
                operator.terms.clear();
            } else if (operator.terms.size() == 1) {
                operator.terms.add(predicates.get(0));
                return;
            }

            operator.terms.addAll(predicates);
        } else {
            operator.terms.addAll(predicates);
        }
    }

    public void buildCoveringPredicate2(Predicate p1, Predicate p2, Operator operator) {
        //The fact that we're here means p1 and p2 have the same LHS
        //If their RHS is a column name and it's the same for p1 and p2, it's a join!
        if (isTableName(p1.getValue()) && p1.getValue().equals(p2.getValue())) {
            operator.addTerm(p1);
            return;
        }

        if (p1.isOperator(">=") || p1.isOperator(">")) {
            if (p2.isOperator(">") || p2.isOperator(">=")) {
                boolean p1Greater = p1.isOperator(">=") ? p1.compareTo(p2) >= 0 : p1.compareTo(p2) > 0;
                operator.addTerm(new Predicate(p1.getName(), p1Greater ? p1.getOperator() : p2.getOperator(),
                        p1Greater ? p2.getValue() : p1.getValue()));
            } else if (p2.isOperator("<") || p2.isOperator("<=")) {
                boolean p1Greater = p1.isOperator(">=") ? p1.compareTo(p2) >= 0 : p1.compareTo(p2) > 0;
                if (p1Greater) {
                    operator.addTerm(p1);
                    operator.addTerm(p2);
                }
            } else if (p2.isOperator("=")) {
                if (p1.compareTo(p2) == 0) {
                    operator.addTerm(p1.copyWithOp(">="));
                } else {
                    operator.addTerm(p1);
                    operator.addTerm(p2);
                }
            }
        } else if (p1.isOperator("<") || p1.isOperator("<=")) {
            if (p2.isOperator("<") || p2.isOperator("<=")) {
                boolean p1Lesser = p1.isOperator("<=") ? p1.compareTo(p2) <= 0 : p1.compareTo(p2) < 0;
                operator.addTerm(new Predicate(p1.getName(), p1Lesser ? p2.getOperator() : p1.getOperator(),
                        p1Lesser ? p2.getValue() : p1.getValue()));
            } else if (p2.isOperator(">") || p2.isOperator(">=")) {
                boolean p1Lesser = p1.isOperator("<=") ? p1.compareTo(p2) <= 0 : p1.compareTo(p2) < 0;
                if (p1Lesser) {
                    operator.addTerm(p1);
                    operator.addTerm(p2);
                }
            } else if (p2.isOperator("=")) {
                if (p1.compareTo(p2) == 0) {
                    operator.addTerm(p1.copyWithOp("<="));
                } else {
                    operator.addTerm(p1);
                    operator.addTerm(p2);
                }
            }
        } else if (p1.isOperator("=")) {
            if (p1.compareTo(p2) == 0) {
                operator.addTerm(p1.copyWithOp("="));
            } else {
                operator.addTerm(p1);
                operator.addTerm(p2);
            }
        }
    }

    public List<List<Predicate>> extractPredicates(String s) {
        List<List<Predicate>> predicates = new ArrayList<>();
        for (String splitAnd : splitByWholeSeparator(s.replace("(", "").replace(")", "").trim(), " AND ")) {
            List<Predicate> ps = new ArrayList<>();
            for (String splitOr : splitByWholeSeparator(splitAnd, " OR ")) {
                int firstOpIndex = splitOr.contains(">=") ? splitOr.indexOf(">=")
                        : splitOr.contains("<=") ? splitOr.indexOf("<=")
                        : splitOr.contains("<") ? splitOr.indexOf("<")
                        : splitOr.contains(">") ? splitOr.indexOf(">")
                        : splitOr.indexOf("=");

                if (firstOpIndex == -1) {
                    System.out.println("Can't find index of operator in " + splitOr);
                }

                List<String> operandVal = asList(splitOr.substring(0, firstOpIndex).trim(),
                        splitOr.substring(firstOpIndex, firstOpIndex + 2).trim(),
                        splitOr.substring(firstOpIndex + 2).trim());
                Predicate p = new Predicate(operandVal.get(0), operandVal.get(1), operandVal.get(2));
                p.isJoin = isTableName(splitOr.substring(firstOpIndex + 2));

                ps.add(p);
            }
            predicates.add(ps);
        }
        return predicates;
    }

    private boolean isTableName(String operand) {
        if (Utils.isDigit(operand.charAt(0))) return false;

        operand = replace(operand, "`", "");
        if (operand.contains(".")) operand = splitByWholeSeparator(operand, ".")[0];
        return configuration.tableNames.contains(operand);
    }

    private boolean isIdentifier(SqlBasicCall call, int index) {
        return call.operand(index).getKind().equals(SqlKind.IDENTIFIER);
    }

    private boolean isLiteral(SqlBasicCall call, int index) {
        return call.operand(index).getKind().equals(SqlKind.LITERAL);
    }

    public static class Normaliser {

        public WhereClause getCNF(WhereClause w) {
            Expression<String> expr2 = RuleSet.toCNF(ExprParser.parse(w.booleanRepn));
            w.booleanRepn = expr2.toString();
            return w;
        }

        public WhereClause getBooleanRepn(String w) {
            String parsed = w;

            String[] vars = StringUtils.substringsBetween(parsed, "`", "`");
            Map<String, String> map = new HashMap<>();

            for (String var : vars) {
                int indexOfVar = parsed.indexOf(var) - 1; //-1 for the quotes
                while (indexOfVar >= 0) {
                    String subw = parsed.substring(indexOfVar);

                    int firstBracIndex = subw.indexOf(")") > 0 ? subw.indexOf(")") : Integer.MAX_VALUE;
                    int firstOrIndex = subw.indexOf("OR") > 0 ? subw.indexOf("OR") - 1 : Integer.MAX_VALUE;
                    int firstAndIndex = subw.indexOf("AND") > 0 ? subw.indexOf("AND") - 1 : Integer.MAX_VALUE;
                    int firstOpIndex = Math.min(Math.min(firstBracIndex, firstOrIndex), firstAndIndex);

                    int to = firstOpIndex == Integer.MAX_VALUE ? parsed.length() : indexOfVar + firstOpIndex;
                    String part = parsed.substring(indexOfVar, to);
                    String key = Utils.randomString(4);
                    parsed = parsed.replace(part, key);
                    map.put(key, part);

                    indexOfVar = parsed.indexOf(var) - 1;
                }
            }
            return new WhereClause(map, replace(parsed, "OR", "|").replace("AND", "&"));
        }

        static class WhereClause {
            Map<String, String> map;
            String booleanRepn;

            public WhereClause(Map<String, String> map, String booleanRepn) {
                this.map = map;
                this.booleanRepn = booleanRepn;
            }

            public String asString() {
                String str = booleanRepn.replace("|", "OR").replace("&", "AND");
                Set<String> keys = map.keySet();
                for (String key : keys) {
                    str = str.replace(key, map.get(key));
                }
                return str;
            }

            public Map<String, String> getCleanMap() {
                Map<String, String> newMap = new HashMap<>();
                for (String key : map.keySet()) {
                    String newVal = replace(map.get(key), "`", "");
                    newMap.put(key, newVal);
                }
                return newMap;
            }

            @Override
            public String toString() {
                return "WhereClause{" +
                        "map=" + map +
                        ", booleanRepn='" + booleanRepn + '\'' +
                        '}';
            }
        }
    }

    public static class DefaultMapContext extends MapContext {

        @Override
        public boolean has(String name) {
            return true;
        }

        @Override
        public Object get(String name) {
            return super.get(name) != null ? super.get(name) : true;
        }
    }
}
