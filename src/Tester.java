import batch.QueryBatcher;
import common.Configuration;
import common.QueryExecutor;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import test.QueryProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
        //Regular execution
        RelNode regNode = executor.getLogicalPlan(Queries.q1);
        executor.execute(regNode, null);

        //MV execution
        RelOptMaterialization materialization = optimizer.materialize("mv0", Queries.mv1);
        RelNode n = optimizer.substitute(materialization, executor.getLogicalPlan(Queries.q1));
        if (n != null) {
            executor.execute(n, null);
        }

    }

    public void testBatch() throws Exception {
        List<String> queries = queryProvider.getAllBatches();

        QueryBatcher queryBatcher = new QueryBatcher(config, executor);

        long t3 = System.currentTimeMillis();
        for (String s : queries) {
            RelNode n = executor.getLogicalPlan(s);
//            executor.execute(n, rs -> System.out.println("Row count: " + countRows(rs)));
        }
        long t4 = System.currentTimeMillis();


        long t1 = System.currentTimeMillis();
        List<QueryBatcher.BatchQuery> combined = queryBatcher.batch(queries);
        long t2 = System.currentTimeMillis();

        System.out.println("Creating a batch took " + (t2 - t1) + " ms");

        List<List<Long>> times = new ArrayList<>();

        for (QueryBatcher.BatchQuery bq : combined) {
            long t5 = System.currentTimeMillis();

            AtomicLong t7 = new AtomicLong();
            AtomicLong t8 = new AtomicLong();

            RelNode rn = executor.getLogicalPlan(bq.query);

            executor.execute(rn, rs -> {
                t7.set(System.currentTimeMillis());
                queryBatcher.unbatchResults3(bq, rs);
                t8.set(System.currentTimeMillis());
            });
            long t6 = System.currentTimeMillis();

            times.add(Arrays.asList(t6 - t5, t8.get() - t7.get()));
        }


        long exec_times = times.get(0).get(0) + times.get(1).get(0);
        long unbatch_times = times.get(0).get(1) + times.get(1).get(1);
//        long exec_times = times.get(0).get(0);
//        long unbatch_times = times.get(0).get(1);
//        long exec_times = 0;
//        long unbatch_times = 0;

        System.out.println("===============================");

        System.out.println("Executing queries individually took " + (t4 - t3) + " ms");
        System.out.println("Executing queries as a batch took " + (t2 - t1) + " + " + (exec_times) + " + " + (unbatch_times) + " = " + (exec_times + unbatch_times + (t2 - t1)) + " ms");
        System.out.println("Combined (" + combined.size() + ") are " + Arrays.toString(combined.toArray()));

    }

}
