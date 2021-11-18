package common;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;

import java.util.ArrayList;
import java.util.List;

public class Evaluator {

    private static final JexlEngine jexl = new JexlBuilder().cache(2048).strict(true).silent(false).create();

    private final List<JexlExpression> expressions = new ArrayList<>();

    public void set(String[] cnfs) {
        expressions.clear();
        for (String cnf : cnfs) {
            expressions.add(jexl.createExpression(cnf));
        }
    }

    public boolean evaluate2(int exprIndex, JexlContext context) {
        return Boolean.parseBoolean(expressions.get(exprIndex).evaluate(context).toString());
    }

    public static void main(String[] args) {
        Expression<String> expr2 = RuleSet.toCNF(ExprParser.parse("x & y"));
        System.out.println(expr2.toString());
    }
}
