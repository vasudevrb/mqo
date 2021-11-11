import batch.QueryBatcher;
import batch.data.BatchedQuery;
import common.Configuration;
import common.QueryExecutor;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import test.QueryProvider;

import java.sql.SQLException;
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
        List<String> queries = provider.getBatch(0);

        long t1 = System.currentTimeMillis();
        for (String q: queries) {
            executor.execute(executor.getLogicalPlan(q), null);
        }
        System.out.println("Ind exec: " + (System.currentTimeMillis() - t1) + " ms");

        t1 = System.currentTimeMillis();
        List<BatchedQuery> batchedQueries = batcher.batch(queries);
        BatchedQuery bq = batchedQueries.get(0);


        System.out.println("Batch is " + bq.sql);
        RelOptMaterialization materialization = optimizer.materialize(randomString(4), bq.sql);
        for (SqlNode node: bq.parts) {
            RelNode relNode = optimizer.substitute(materialization, executor.getLogicalPlan(node));
            if (relNode != null) {
                System.out.println("Found substitutable");
                executor.execute(relNode, null);
            }
        }
        System.out.println("Batch exec: " + (System.currentTimeMillis() - t1) + " ms");

    }
}
