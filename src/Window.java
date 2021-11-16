import batch.QueryBatcher;
import batch.data.BatchedQuery;
import common.Configuration;
import common.QueryExecutor;
import common.QueryUtils;
import common.Utils;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import test.QueryProvider;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static common.Utils.randomString;

public class Window {

    private final QueryExecutor executor;
    private final QueryProvider provider;

    private final QueryBatcher batcher;
    private final MViewOptimizer optimizer;

    public Window(Configuration configuration) {
        this.executor = new QueryExecutor(configuration);
        this.provider = new QueryProvider();

        this.batcher = new QueryBatcher(configuration, executor);
        this.optimizer = new MViewOptimizer(configuration);
    }

    public void run() throws SQLException {
        List<String> queries = provider.getAllBatches();
        Collections.shuffle(queries);

        List<BatchedQuery> batchedQueries = batcher.batch(queries);

        for (BatchedQuery bq : batchedQueries) {
            System.out.println();
            System.out.println(Utils.getPrintableSql(bq.sql));
            System.out.println();

            RelOptMaterialization materialization = optimizer.materialize(randomString(4), bq.sql);
            for (SqlNode node : bq.parts.subList(1, bq.parts.size())) {
                RelNode relNode = optimizer.substitute(materialization, executor.getLogicalPlan(node));
                if (relNode != null) {
                    executor.execute(relNode, rs -> System.out.println(QueryUtils.countRows(rs)));
                }
            }
        }
    }
}
