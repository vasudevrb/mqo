package common;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import org.apache.commons.jexl3.*;

import java.util.ArrayList;
import java.util.List;

public class Evaluator {

    private static final JexlEngine jexl = new JexlBuilder().cache(2048).strict(true).silent(false).create();

    private List<JexlExpression> expressions = new ArrayList<>();
    private JexlContext context = new MapContext();

    public static Evaluator initialize(String[] cnfs) {
        Evaluator evaluator = new Evaluator();
        for (String cnf : cnfs) {
            evaluator.expressions.add(jexl.createExpression(cnf));
        }
        return evaluator;
    }

    public static void main(String[] args) {
        String x = "line.klj > 4 || (b < 4 && c > 9)";
        JexlExpression e = jexl.createExpression(x);

        JexlContext context = new MapContext();
        context.set("line.klj", "5");
        context.set("b", "1");
        context.set("c", "5");
        context.set("line.klj", "2");

        System.out.println(e.evaluate(context));
    }

    public static boolean evaluate(String s) {
        Expression<String> expr2 = RuleSet.simplify(ExprParser.parse(s));
        return Boolean.parseBoolean(expr2.toString());
    }


}
