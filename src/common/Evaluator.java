package common;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import org.apache.commons.jexl3.*;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Evaluator {

    private static final JexlEngine jexl = new JexlBuilder().cache(2048).strict(true).silent(false).create();

    private List<JexlExpression> expressions = new ArrayList<>();

    public void set(String[] cnfs) {
        expressions.clear();
        for (String cnf : cnfs) {
            expressions.add(jexl.createExpression(cnf));
        }
    }

    public static void main(String[] args) {
        JexlExpression e = jexl.createExpression("100 || 241097 && 235989");
        JexlContext context = new MapContext();
    }

    public static boolean evaluate(String s) {
        Expression<String> expr2 = RuleSet.simplify(ExprParser.parse(s));
        return Boolean.parseBoolean(expr2.toString());
    }

    public boolean evaluate2(int exprIndex, JexlContext context) {
        return Boolean.parseBoolean(expressions.get(exprIndex).evaluate(context).toString());
    }


}
