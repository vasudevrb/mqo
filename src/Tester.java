import batch.QueryBatcher;
import common.Configuration;
import common.QueryExecutor;
import common.QueryUtils;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Tester {

    private final QueryExecutor executor;
    private final Configuration config;
    private final MViewOptimizer optimizer;

    public Tester(Configuration config) {
        this.optimizer = new MViewOptimizer(config);
        this.executor = new QueryExecutor(config);
        this.config = config;
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
        String q1 = "SELECT \"l_extendedprice\", \"l_quantity\"" +
                " FROM \"public\".\"lineitem\"" +
                " WHERE" +
                " \"l_shipdate\" >= date '1994-01-01'" +
                " AND \"l_shipdate\" < date '1994-09-02'" +
                " AND \"l_discount\" > 0.07" +
                " AND \"l_quantity\" > 45";

        String q2 = "SELECT \"l_extendedprice\"" +
                " FROM \"public\".\"lineitem\"" +
                " WHERE" +
                " \"l_shipdate\" < date '1994-06-02'" +
                " AND \"l_shipdate\" > date '1994-01-01'" +
                " AND \"l_quantity\" > 25";

        String q3 = "SELECT \"s_name\", \"s_suppkey\"" +
                " FROM \"public\".\"supplier\", \"public\".\"nation\"" +
                " WHERE \"s_nationkey\" = \"n_nationkey\"" +
                " AND \"s_suppkey\" < 800";

        String q4 = "SELECT \"s_name\", \"n_name\", \"r_name\"" +
                " FROM \"public\".\"supplier\", \"public\".\"nation\", \"public\".\"region\"" +
                " WHERE \"s_nationkey\" = \"n_nationkey\"" +
                " AND \"n_regionkey\" = \"r_regionkey\"" +
                " AND \"s_suppkey\" < 1200";

        String q5 = "SELECT \"s_name\", \"n_name\", \"r_name\"" +
                " FROM \"public\".\"supplier\", \"public\".\"nation\", \"public\".\"region\"" +
                " WHERE \"s_nationkey\" = \"n_nationkey\"" +
                " AND \"n_regionkey\" = \"r_regionkey\"" +
                " AND \"s_suppkey\" < 1500";

        QueryBatcher queryBatcher = new QueryBatcher(config, executor);

        long t3 = System.currentTimeMillis();
        for (String s : Arrays.asList(q1, q2, q4, q5)) {
            executor.execute(executor.getLogicalPlan(s), rs -> System.out.println("Count is " + QueryUtils.countRows(rs)));
        }
        long t4 = System.currentTimeMillis();


        long t1 = System.currentTimeMillis();
        List<QueryBatcher.BatchQuery> combined = queryBatcher.batch(Arrays.asList(q1, q2, q3, q4, q5));
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
