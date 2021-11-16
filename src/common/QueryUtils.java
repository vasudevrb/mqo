package common;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryUtils {

    public static final List<String> AGGREGATES = List.of("COUNT", "AVG", "SUM", "MIN", "MAX");

    public static List<String> from(SqlNode node) {
        SqlSelect selectNode = (SqlSelect) node;

        if (selectNode.getFrom() instanceof SqlJoin) {
            return getOperands((SqlJoin) selectNode.getFrom());
        }

        return List.of(((SqlSelect) node).getFrom().toString());
    }

    //Given a SqlJoin, this function returns all the operands that are not commas and stuff
    //This is needed for threeway joins, which in calcite, are represented as [SqlJoin, SqlBasicCall]
    private static List<String> getOperands(SqlJoin join) {
        ArrayList<String> calls = new ArrayList<>();

        calls.addAll(join.getOperandList().stream().filter(x -> x instanceof SqlBasicCall).map(SqlNode::toString).toList());
        join.getOperandList().stream()
                .filter(x -> x instanceof SqlJoin)
                .map(j -> getOperands((SqlJoin) j))
                .forEach(ca -> calls.addAll(ca));

        return calls;
    }

    public static String getFromString(SqlNode node) {
        String sql = node.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
        return StringUtils.trim(StringUtils.substringBetween(sql, "FROM ", "WHERE "));
    }

    public static String where(SqlNode node) {
        SqlNode n1 = ((SqlSelect) node).getWhere();
        return n1 != null ? n1.toString() : "";
    }

    public static List<String> selectList(SqlNode node) {
        return ((SqlSelect) node).getSelectList()
                .stream()
                .map(sl -> {
                    String ss = sl.toString();
                    int bracIndex = ss.indexOf("(");
                    if (bracIndex != -1 && AGGREGATES.contains(ss.substring(0, bracIndex))) {
                        return ss.replace("`", "\"");
                    }

                    return "\"" + ss.replace(".", "\".\"") + "\"";
                })
                .collect(Collectors.toList());
    }

    public static String recreateQuery(SqlNode node, String newWhere) {
        String q = "SELECT " + String.join(",", selectList(node)) +
                " FROM " + getFromString(node) +
                " WHERE " + newWhere;

        SqlNode groupBy = ((SqlSelect) node).getGroup();
        if (groupBy != null) {
            q += " GROUP BY " + groupBy.toString().replace("`", "\"");
        }

        return q;
    }

    public static int countRows(ResultSet rs) {
        int count = 0;
        try {
            while (rs.next()) {
                count++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return count;
    }

    public static RelOptCost getCost(RelNode node) {
        RelOptCluster cluster = node.getCluster();
        return cluster.getMetadataQuery().getCumulativeCost(node);
    }
}
