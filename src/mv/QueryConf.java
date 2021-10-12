package mv;

import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.function.Function;

public class QueryConf {
    public String schemaName;
    public List<Pair<String, String>> materializedViews;
    public String query;
    public MVHandler mvHandler;
    public Function<String, Boolean> checker;

    public QueryConf(String schemaName, List<Pair<String, String>> materializedViews, String query, MVHandler mvHandler, Function<String, Boolean> checker) {
        this.schemaName = schemaName;
        this.materializedViews = materializedViews;
        this.query = query;
        this.mvHandler = mvHandler;
        this.checker = checker;
    }
}
