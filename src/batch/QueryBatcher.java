package batch;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import common.QueryValidator;
import org.apache.calcite.sql.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.*;
import java.util.stream.Collectors;

import static batch.Operator.Type.AND;
import static batch.Operator.Type.OR;

public class QueryBatcher {

    private final Normaliser normaliser;
    private final QueryValidator validator;

    public QueryBatcher(QueryValidator validator) {
        this.normaliser = new Normaliser();
        this.validator = validator;
    }

    public List<String> batch(List<String> queries) throws Exception {
        ArrayList<String> batchedQueries = new ArrayList<>();

        if (queries.size() == 1) {
            batchedQueries.add(queries.get(0));
            return batchedQueries;
        }

        SqlNode n1 = validator.validate(queries.get(0));
        SqlNode n2 = validator.validate(queries.get(1));
        if (canMerge(n1, n2)) {
            batchedQueries.add(getCombinedQueryString(n1, n2));
        } else {
            batchedQueries.add(getQueryString(n1));
            batchedQueries.add(getQueryString(n2));
        }
        int k = 2;
        //When in the batchedQueries, some new query is merged with a previous one,
        //that previous one need to be removed. This list tracks that.
        ArrayList<String> removable = new ArrayList<>();

        while (k < queries.size()) {
            boolean added = false;
            SqlNode n3 = validator.validate(queries.get(k));
            for (int l = batchedQueries.size() - 1; l >= 0; l--) {
                SqlNode n4 = validator.validate(batchedQueries.get(l));
                if (canMerge(n3, n4)) {
                    added = true;
                    batchedQueries.remove(l);
                    batchedQueries.add(getCombinedQueryString(n3, n4));
                    break;
                }
            }

            if (!added) {
                batchedQueries.add(getQueryString(n3));
            }
            k++;
        }

        return batchedQueries;
    }

    private String getQueryString(SqlNode n1) {
        String query = "SELECT " + String.join(", ", selectList(n1)) + " FROM " + from(n1) + " WHERE " + where(n1);
        return query.replaceAll("`", "\"");
    }

    private String getCombinedQueryString(SqlNode n1, SqlNode n2) {
        List<String> select1 = selectList(n1);
        List<String> select2 = selectList(n2);

        Set<String> selectSet = new HashSet<>();
        selectSet.addAll(select1);
        selectSet.addAll(select2);

        String combinedQuery = "SELECT " + String.join(", ", selectSet) + " FROM " + from(n1) + " WHERE " + build(n1, n2);
        return combinedQuery.replaceAll("`", "\"");
    }

    public boolean canMerge(SqlNode node1, SqlNode node2) {
        return from(node1).equals(from(node2));
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

    public String from(SqlNode node) {
        return ((SqlSelect) node).getFrom().toString();
    }

    public String where(SqlNode node) {
        return ((SqlSelect) node).getWhere().toString();
    }

    public List<String> selectList(SqlNode node) {
        return ((SqlSelect) node).getSelectList()
                .stream()
                .map(sl -> "\"" + sl.toString().replace(".", "\".\"") + "\"")
                .collect(Collectors.toList());
    }

    public Operator build2(SqlNode node1, SqlNode node2) {
        Operator op = new Operator(AND);

        List<Predicate> node1Preds = extractPredicates(node1);
        List<Predicate> node2Preds = extractPredicates(node2);

        for (Predicate n1p : node1Preds) {
            for (Predicate n2p : node2Preds) {
                if (!n2p.getName().equals(n1p.getName())) {
                    continue;
                }

                Operator or1 = new Operator(OR);
                buildCoveringPredicate2(n1p, n2p, or1);
                if (or1.terms.size() == 1) op.addTerm(or1.terms.get(0));
                else if (or1.terms.size() > 1) op.addTerm(or1);
            }
        }
        return op;
    }

    private List<List<Predicate>> clean(List<List<Predicate>> pr) {
        //Filter only those conditions that have the same name
        for (List<Predicate> preds : pr) {
            Iterator<Predicate> predIterator = preds.iterator();
            String predName = "";
            while (predIterator.hasNext()) {
                if (predName.isEmpty()) {
                    predName = predIterator.next().getName();
                } else if (!predName.equals(predIterator.next().getName())) {
                    predIterator.remove();
                }
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

        String finalStr = "";

        for (String part1 : cnf.replaceAll("\\(", "").replaceAll("\\)", "").split("&")) {
            for (String part2 : c2.replaceAll("\\(", "").replaceAll("\\)", "").split("&")) {
                finalStr += "(" + part1 + " | " + part2 + ")";
                finalStr += " & ";
            }
        }


        finalStr = finalStr.substring(0, finalStr.length() - 3).replaceAll("\\|", "OR").replaceAll("&", "AND");
        for (String splitAnd : finalStr.replaceAll("\\(", "").replaceAll("\\)", "").split(" AND ")) {
            String[] splitOr = splitAnd.split(" OR ");
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
        } else {
            operator.terms.addAll(predicates);
        }
    }

    private <T> boolean atLeast(int num, List<T> list, int index) {
        return list.size() > index + num + 1;
    }

    public void buildCoveringPredicate2(Predicate p1, Predicate p2, Operator operator) {
        if (p1.isOperator(">=") || p1.isOperator(">")) {
            if (p2.isOperator(">") || p2.isOperator(">=")) {
                boolean p1Greater = p1.compareTo(p2) > 0;
                operator.addTerm(new Predicate(p1.getName(), p1Greater ? p1.getOperator() : p2.getOperator(),
                        p1Greater ? p2.getValue() : p1.getValue()));
            } else if (p2.isOperator("<") || p2.isOperator("<=")) {
                boolean p1Greater = p1.compareTo(p2) > 0;
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
                boolean p1Lesser = p1.compareTo(p2) < 0;
                operator.addTerm(new Predicate(p1.getName(), p1Lesser ? p2.getOperator() : p1.getOperator(),
                        p1Lesser ? p2.getValue() : p1.getValue()));
            } else if (p2.isOperator(">") || p2.isOperator(">=")) {
                boolean p1Lesser = p1.compareTo(p2) < 0;
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
        for (String splitAnd : s.replaceAll("\\(", "").replaceAll("\\)", "").trim().split(" AND ")) {
            List<Predicate> ps = new ArrayList<>();
            for (String splitOr : splitAnd.split(" OR ")) {
                int firstOpIndex = splitOr.contains(">=") ? splitOr.indexOf(">=")
                        : splitOr.contains("<=") ? splitOr.indexOf("<=")
                        : splitOr.contains("<") ? splitOr.indexOf("<")
                        : splitOr.contains(">") ? splitOr.indexOf(">")
                        : splitOr.indexOf("=");

                if (firstOpIndex == -1) {
                    System.out.println("Can't find index of operator in " + splitOr);
                }

                List<String> operandVal = Arrays.asList(splitOr.substring(0, firstOpIndex).trim(),
                        splitOr.substring(firstOpIndex, firstOpIndex + 2).trim(),
                        splitOr.substring(firstOpIndex + 2).trim());
                ps.add(new Predicate(operandVal.get(0), operandVal.get(1), operandVal.get(2)));
            }
            predicates.add(ps);
        }
        return predicates;
    }

    public List<Predicate> extractPredicates(SqlNode node) {
        List<Predicate> predicates = new ArrayList<>();

        if (node.getKind().equals(SqlKind.ORDER_BY)) {
            node = ((SqlOrderBy) node).query;
        }

        if (node == null) return predicates;

        SqlBasicCall where = (SqlBasicCall) ((SqlSelect) node).getWhere();
        if (where != null) {
            if (isIdentifier(where, 0) && isLiteral(where, 1)) {
                predicates.add(Predicate.newPredicate(where.operand(0).toString(), "", where.operand(1).toString()));
                return predicates;
            }

            SqlBasicCall left = where.operand(0);
            SqlBasicCall right = where.operand(1);

            while (!isIdentifier(left, 0) && !isLiteral(left, 1)) {
                SqlBasicCall call = left.operand(1);
                predicates.add(Predicate.newPredicate(call.operand(0).toString(),
                        call.getOperator().getName(), call.operand(1).toString()));
                left = left.operand(0);
            }

            predicates.add(Predicate.newPredicate(left.operand(0).toString(), left.getOperator().getName(),
                    left.operand(1).toString()));
            predicates.add(Predicate.newPredicate(right.operand(0).toString(), right.getOperator().getName(),
                    right.operand(1).toString()));
        }
        return predicates;
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
                    String key = RandomStringUtils.randomAlphabetic(4);
                    parsed = parsed.replace(part, key);
                    map.put(key, part);

                    indexOfVar = parsed.indexOf(var) - 1;
                }
            }
            return new WhereClause(map, parsed.replaceAll("OR", "|").replaceAll("AND", "&"));
        }

        private String getBooleanRepn2(String w) {
            List<String> allowedSymbols = Arrays.asList(">=", "<=", ">", "<", "=", "LIKE");
            String parsed = w;

            for (String symbol : allowedSymbols) {
                int index = parsed.indexOf(symbol);
                while (index >= 0) {
                    String subw = parsed.substring(index);

                    int firstBracIndex = subw.indexOf(")") > 0 ? subw.indexOf(")") : Integer.MAX_VALUE;
                    int firstOrIndex = subw.indexOf("OR") > 0 ? subw.indexOf("OR") : Integer.MAX_VALUE;
                    int firstAndIndex = subw.indexOf("AND") > 0 ? subw.indexOf("AND") : Integer.MAX_VALUE;
                    int firstOpIndex = Math.min(Math.min(firstBracIndex, firstOrIndex), firstAndIndex);

                    int to = firstOpIndex == Integer.MAX_VALUE ? parsed.length() : index + firstOpIndex;
                    parsed = parsed.replace(parsed.substring(index, to), "");
                    index = parsed.indexOf(symbol, index + 1);
                }
            }

            return parsed.replaceAll("OR", "|").replaceAll("AND", "&");
        }

        static class WhereClause {
            Map<String, String> map;
            String booleanRepn;

            public WhereClause(Map<String, String> map, String booleanRepn) {
                this.map = map;
                this.booleanRepn = booleanRepn;
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
}
