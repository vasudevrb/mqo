import batch.QueryBatcher;
import batch.data.BatchedQuery;
import common.Configuration;
import common.QueryExecutor;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import test.QueryProvider;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static common.QueryUtils.countRows;

public class Tester {

    private final QueryExecutor executor;
    private final Configuration config;
    private final MViewOptimizer optimizer;

    private final QueryProvider queryProvider;

    public Tester(Configuration config) {
        this.optimizer = new MViewOptimizer(config);
        this.executor = new QueryExecutor(config);
        this.config = config;
        this.queryProvider = new QueryProvider();
    }

    public void testMVSubstitution() throws Exception {
        List<String> matQueries = queryProvider.getMaterializable(0);

        //Regular execution
        RelNode regNode = executor.getLogicalPlan(matQueries.get(0));
        executor.execute(regNode, null);

        //MV execution
        RelOptMaterialization materialization = optimizer.materialize("mv0", matQueries.get(0));
        RelNode n = optimizer.substitute(materialization, executor.getLogicalPlan(matQueries.get(1)));
        if (n != null) {
            executor.execute(n, null);
        }
    }

    public void testBatch() throws Exception {
        QueryBatcher queryBatcher = new QueryBatcher(config, executor);

        List<String> queries = queryProvider.getBatch(0);

        System.out.println("Executing queries individually");
        long t1 = System.currentTimeMillis();
        for (String s : queries) {
            RelNode n = executor.getLogicalPlan(s);
            executor.execute(n, rs -> System.out.println("Row count: " + countRows(rs)));
        }
        long indExecTime = System.currentTimeMillis() - t1;

        System.out.println("--------------------------------");
        System.out.println("Executing queries as a batch");
        t1 = System.currentTimeMillis();
        List<BatchedQuery> combined = queryBatcher.batch(queries);
        long batchCreationTime = System.currentTimeMillis() - t1;

        long execTime = 0;
        AtomicLong unbatchTime = new AtomicLong();

        for (BatchedQuery bq : combined) {
            long t2 = System.currentTimeMillis();
            RelNode rn = executor.getLogicalPlan(bq.sql);
            executor.execute(rn, rs -> {
                long t3 = System.currentTimeMillis();
                queryBatcher.unbatchResults3(bq, rs);
                unbatchTime.addAndGet((System.currentTimeMillis() - t3));
            });
            execTime += (System.currentTimeMillis() - t2);
        }
        execTime -= unbatchTime.get();

        System.out.println("===============================");

        System.out.println("Executing queries individually took " + indExecTime + " ms");
        System.out.println("Executing queries as a batch took " + (batchCreationTime) + " + " + (execTime) + " + " + (unbatchTime.get()) + " = " + (execTime + unbatchTime.get() + (batchCreationTime)) + " ms");
        System.out.println("Combined (" + combined.size() + ") are " + Arrays.toString(combined.toArray()));

    }
}
