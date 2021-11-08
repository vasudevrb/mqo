import batch.QueryBatcher;
import common.Configuration;
import common.QueryExecutor;
import common.QueryUtils;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptCost;
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

    public void testMVSubstitution2() throws Exception {
        List<String> matQueries = queryProvider.getMaterializable(0);

        //Regular execution
        for (String q: matQueries) {
            RelNode regNode = executor.getLogicalPlan(q);
            executor.execute(regNode, null);
        }

        //MV execution
//        RelOptMaterialization materialization = optimizer.materialize("mv0", matQueries.get(0));
//        executor.execute(materialization.queryRel, null);
//        for (String q: matQueries.subList(1, matQueries.size())) {
//            RelNode n = optimizer.substitute(materialization, executor.getLogicalPlan(q));
//            if (n != null) {
//                executor.execute(n, null);
//            }
//        }

    }

    public void testBatchSimple() throws Exception {
        QueryBatcher queryBatcher = new QueryBatcher(config, executor);
        List<String> queries = queryProvider.getAllBatches();

        for (String s : queries) {
            RelNode n = executor.getLogicalPlan(s);
            executor.execute(n, rs -> System.out.println("Row count: " + countRows(rs)));
        }

        List<QueryBatcher.BatchQuery> combined = queryBatcher.batch(queries);
        for (QueryBatcher.BatchQuery bq : combined) {
            RelNode rn = executor.getLogicalPlan(bq.query);
            executor.execute(rn, rs -> queryBatcher.unbatchResults3(bq, rs));
        }
    }

    public void testBatch() throws Exception {
        QueryBatcher queryBatcher = new QueryBatcher(config, executor);

        List<String> queries = queryProvider.getBatch(1);

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
        List<QueryBatcher.BatchQuery> combined = queryBatcher.batch(queries);
        long batchCreationTime = System.currentTimeMillis() - t1;

        long execTime = 0;
        AtomicLong unbatchTime = new AtomicLong();

        for (QueryBatcher.BatchQuery bq : combined) {
            long t2 = System.currentTimeMillis();
            RelNode rn = executor.getLogicalPlan(bq.query);
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

    public void testCost() throws Exception {
        List<String> queries = queryProvider.getBatch(2);

        RelOptCost sumIndCost = null;
        for (String q : queries) {
            RelNode plan = executor.getLogicalPlan(q);
            if (sumIndCost == null) {
                sumIndCost = QueryUtils.getCost(plan);
            } else {
                sumIndCost.plus(QueryUtils.getCost(plan));
            }
        }

        QueryBatcher batcher = new QueryBatcher(config, executor);
        QueryBatcher.BatchQuery batched = batcher.batch(queries).get(0);
        RelNode plan = executor.getLogicalPlan(batched.query);

        RelOptCost batchCost = QueryUtils.getBatchCost(plan);

        if (sumIndCost.isLe(batchCost)) {
            System.out.println("Better to execute individually");
        } else {
            System.out.println("Better to batch");
        }
    }

    public void testListenerThreads() {
        queryProvider.listen(s -> System.out.println("Got " + s));
    }
}
