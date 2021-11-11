package common;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class QueryUtils {

    public static String from(SqlNode node) {
        return ((SqlSelect) node).getFrom().toString();
    }

    public static String where(SqlNode node) {
        SqlNode n1 = ((SqlSelect) node).getWhere();
        return n1 != null ? n1.toString() : "";
    }

    public static List<String> selectList(SqlNode node) {
        return ((SqlSelect) node).getSelectList()
                .stream()
                .map(sl -> "\"" + sl.toString().replace(".", "\".\"") + "\"")
                .collect(Collectors.toList());
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
