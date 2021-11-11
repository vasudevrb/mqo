package batch.data;

import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BatchedQuery {
    public String sql;
    public ArrayList<Integer> indexes;
    public ArrayList<SqlNode> parts;

    public BatchedQuery(String sql, List<Integer> indexes, List<SqlNode> parts) {
        this.sql = sql;
        this.indexes = new ArrayList<>(indexes);
        this.parts = new ArrayList<>(parts);
    }

    @Override
    public String toString() {
        return "BatchQuery{" +
                "query='" + sql + '\'' +
                ", indexes=" + Arrays.toString(indexes.toArray()) +
                '}';
    }
}
