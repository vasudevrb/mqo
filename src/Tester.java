import batch.QueryBatcher;
import common.Configuration;
import common.QueryExecutor;
import common.QueryUtils;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.time.StopWatch;
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
        StopWatch stopWatch = new StopWatch();
        QueryBatcher queryBatcher = new QueryBatcher(config, executor);

        List<String> queries = queryProvider.getAllBatches();

        System.out.println("Executing queries individually");
        stopWatch.start();
        for (String s : queries) {
            RelNode n = executor.getLogicalPlan(s);
            executor.execute(n, rs -> System.out.println("Row count: " + countRows(rs)));
        }
        stopWatch.suspend();
        long indExecTime = stopWatch.getTime();

        System.out.println("Executing queries as a batch");
        stopWatch.resume();
        List<QueryBatcher.BatchQuery> combined = queryBatcher.batch(queries);

        long batchCreationTime = stopWatch.getTime();
        AtomicLong unbatchTimes = new AtomicLong();

        for (QueryBatcher.BatchQuery bq : combined) {
            RelNode rn = executor.getLogicalPlan(bq.query);
            executor.execute(rn, rs -> {
                long x = stopWatch.getTime();
                queryBatcher.unbatchResults3(bq, rs);
                unbatchTimes.addAndGet(stopWatch.getTime() - x);
            });
        }
        stopWatch.stop();
        long execTime = stopWatch.getTime() - unbatchTimes.get();

        System.out.println("===============================");

        System.out.println("Executing queries individually took " + indExecTime + " ms");
        System.out.println("Executing queries as a batch took " + (batchCreationTime) + " + " + (execTime) + " + " + (unbatchTimes.get()) + " = " + (execTime + unbatchTimes.get() + (batchCreationTime)) + " ms");
        System.out.println("Combined (" + combined.size() + ") are " + Arrays.toString(combined.toArray()));

    }

    public void testCost() throws Exception {
        List<String> queries = queryProvider.getBatch(2);

        RelOptCost indCost = null;
        for (String q : queries) {
            RelNode plan = executor.getLogicalPlan(q);
            if (indCost == null) {
                indCost = QueryUtils.getCost(plan);
            } else {
                indCost.plus(QueryUtils.getCost(plan));
            }
        }

        QueryBatcher batcher = new QueryBatcher(config, executor);
        QueryBatcher.BatchQuery batched = batcher.batch(queries).get(0);
        RelNode plan = executor.getLogicalPlan(batched.query);

        RelOptCost batchCost = QueryUtils.getBatchCost(plan);

        if (indCost.isLe(batchCost)) {
            System.out.println("Better to execute individually");
        } else {
            System.out.println("Better to batch");
        }
    }
}
