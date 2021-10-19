package batch;

import org.apache.calcite.sql.*;

import java.util.ArrayList;
import java.util.List;

public class BatchQueryBuilder {

    public Operator build(SqlNode node1, SqlNode node2) {
        Operator op = new Operator(Operator.Type.AND);

        List<Predicate> node1Preds = extractPredicates(node1);
        List<Predicate> node2Preds = extractPredicates(node2);

        for (Predicate n1p : node1Preds) {
            for (Predicate n2p : node2Preds) {
                if (!n2p.getName().equals(n1p.getName())) {
                    continue;
                }

                Operator or1 = new Operator(Operator.Type.OR);
                buildCoveringPredicate2(n1p, n2p, or1);
                if (or1.terms.size() == 1) op.addTerm(or1.terms.get(0));
                else if (or1.terms.size() > 1) op.addTerm(or1);
            }
        }
        return op;
    }

    public void buildCoveringPredicate2(Predicate p1, Predicate p2, Operator operator) {
        if (p1.isOperator(">=") || p1.isOperator(">")) {
            if (p2.isOperator(">") || p2.isOperator(">=")) {
                boolean p1Greater = p1.compareTo(p2) > 0;
                operator.addTerm(new Predicate(p1.getName(), p1Greater ? p1.getOperator() : p2.getOperator(), p1Greater ? p2.getValue() : p1.getValue()));
            } else if (p2.isOperator("<") || p2.isOperator("<=")) {
                boolean p1Greater = p1.compareTo(p2) > 0;
                if (p1Greater) {
                    operator.addTerm(p1);
                    operator.addTerm(p2);
                }
            }
        } else if (p1.isOperator("<") || p1.isOperator("<=")) {
            if (p2.isOperator("<") || p2.isOperator("<=")) {
                boolean p1Lesser = p1.compareTo(p2) < 0;
                operator.addTerm(new Predicate(p1.getName(), p1Lesser ? p2.getOperator() : p1.getOperator(), p1Lesser ? p2.getValue() : p1.getValue()));
            } else if (p2.isOperator(">") || p2.isOperator(">=")) {
                boolean p1Lesser = p1.compareTo(p2) < 0;
                if (p1Lesser) {
                    operator.addTerm(p1);
                    operator.addTerm(p2);
                }
            }
        }
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
}
