package common;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleSet;

public class Evaluator {

    public static boolean evaluate(String s) {
        Expression<String> expr2 = RuleSet.simplify(ExprParser.parse(s));
        return Boolean.parseBoolean(expr2.toString());
    }

    public static void main(String[] args) {
        evaluate("a");
    }
}
