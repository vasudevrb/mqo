package common;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
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
        return selectList(node, true);
    }

    public static List<String> selectList(SqlNode node, boolean includeAggregates) {
        return ((SqlSelect) node).getSelectList()
                .stream()
                .filter(sl -> {
                    if (includeAggregates) {
                        return true;
                    }

                    //Return the only selects which are not aggregates
                    String ss = sl.toString();
                    int bracIndex = ss.indexOf("(");
                    var isAggregate = bracIndex != -1 && AGGREGATES.contains(ss.substring(0, bracIndex));

                    return !isAggregate;
                })
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

    public static boolean isAggregate(SqlNode node) {
        return ((SqlSelect) node).getGroup() != null;
    }

    public static List<String> groupByList(SqlNode node) {
        SqlNodeList groupBy = ((SqlSelect) node).getGroup();

        if (groupBy == null) {
            return new ArrayList<>();
        }

        return groupBy.stream()
                .map(si -> "\"" + si.toString().replace(".", "\".\"") + "\"")
                .collect(Collectors.toList());
    }

    public static String recreateQuery(SqlNode node, Collection<String> selects, String newWhere, Collection<String> groupBys) {
        String q = "SELECT " + String.join(",", selects) +
                " FROM " + getFromString(node) +
                " WHERE " + newWhere;

        if (!groupBys.isEmpty()) {
            q +=" GROUP BY " + String.join(",", groupBys);
        }

        return q;
    }

    public static String recreateQuery(SqlNode node, List<String> selects, String newWhere) {
        return recreateQuery(node, selects, newWhere, groupByList(node));
    }

    public static String recreateQuery(SqlNode node, String newWhere) {
        return recreateQuery(node, selectList(node), newWhere, groupByList(node));
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
