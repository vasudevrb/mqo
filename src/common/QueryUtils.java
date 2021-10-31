package common;

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
        return ((SqlSelect) node).getWhere().toString();
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
}
