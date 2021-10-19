package batch;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Normaliser {


    public static void main(String[] args) {
        String w = "x < 5 AND y > 10 OR (t >= 11 AND w < 15) OR x = 4";
        String k = "\"l_shipdate\" >= date '1994-01-01'" +
                " AND \"l_shipdate\" < date '1994-06-01'" +
                " AND (\"l_discount\" > 0.07" +
                " OR \"l_quantity\" < 25)" +
                " AND \"l_name\" LIKE 'A%'";

        WhereClause bk = getBooleanRepn2(k);
//        System.out.println(Arrays.toString(StringUtils.substringsBetween(bk, "\"", "\"")));
//        System.out.println(Arrays.toString(StringUtils.substringsBetween(k, "\"", "\"")));

        Expression<String> expr = RuleSet.toCNF(ExprParser.parse(bk.booleanRepn));
//        String cnf = expr.toString();

        String cnf = "(LgdI & ROfK & pLyL & (FlNK | JVWB))";
        String c2 = "(AqXT & oFYY & zzAA & (Bvwy | adhX))";
        String finalStr = "";

        for (String part1 : cnf.replaceAll("\\(", "").replaceAll("\\)", "").split("&")) {
            for (String part2 : c2.replaceAll("\\(", "").replaceAll("\\)", "").split("&")) {
                finalStr += "(" + part1 + "| " + part2 + ")";
                finalStr += " & ";
            }
        }

        finalStr = finalStr.substring(0, finalStr.length() - 3).replaceAll("\\|", "OR").replaceAll("&", "AND");
        for (String splitAnd: finalStr.replaceAll("\\(", "").replaceAll("\\)", "").split(" AND ")) {
            String[] splitOr = splitAnd.split(" OR ");

        }

        System.out.println(finalStr);
    }

    public static String doOr(WhereClause w1, WhereClause w2) {

        return "";
    }

    public static WhereClause getBooleanRepn2(String w) {
        String parsed = w;

        String[] vars = StringUtils.substringsBetween(parsed, "\"", "\"");
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

    public static String getBooleanRepn(String w) {
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

    private static class WhereClause {
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
