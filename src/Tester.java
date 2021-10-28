import batch.QueryBatcher;
import common.Configuration;
import common.QueryValidator;
import mv.Optimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Tester {
    private Configuration config;
    private Optimizer optimizer;
    private QueryValidator validator;

    public Tester(Configuration config) throws SQLException {
        this.optimizer = new Optimizer(config);
        this.validator = new QueryValidator(config);
        this.config = config;
    }

    public void testMVSubstitution() throws Exception {
        //Regular execution
        RelNode regNode = validator.getLogicalPlan(Queries.q3);
        RelNode physicalPlan = optimizer.getPhysicalPlan(regNode);
        optimizer.execute(physicalPlan);

        //MV execution
        Pair<RelNode, List<RelOptMaterialization>> m = optimizer.getMaterializations("MV0", Queries.mv0, Queries.q0);
        RelNode node = optimizer.getPhysicalPlan(optimizer.optimize(m).get(0), m.right);
        node.explain();
        optimizer.execute(node);
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

        String q3 = "SELECT \"s_name\", \"s_suppkey\""  +
                " FROM \"public\".\"supplier\", \"public\".\"nation\"" +
                " WHERE \"s_nationkey\" = \"n_nationkey\"" +
                " AND \"s_suppkey\" < 800";

        String q4 = "SELECT \"s_name\", \"n_name\""  +
                " FROM \"public\".\"supplier\", \"public\".\"nation\"" +
                " WHERE \"s_nationkey\" = \"n_nationkey\"" +
                " AND \"s_suppkey\" < 1200";

        String q5 = "SELECT \"s_name\", \"n_name\""  +
                " FROM \"public\".\"supplier\", \"public\".\"nation\"" +
                " WHERE \"s_nationkey\" = \"n_nationkey\"" +
                " AND \"s_suppkey\" < 1500";

        QueryBatcher queryBatcher = new QueryBatcher(config, validator);

        long t3 = System.currentTimeMillis();
        for (String s : Arrays.asList(q1, q2, q3, q4, q5)) {
            optimizer.execute((validator.getLogicalPlan(s)));
        }
        long t4 = System.currentTimeMillis();


        long t1 = System.currentTimeMillis();
        List<QueryBatcher.BatchQuery> combined = queryBatcher.batch(Arrays.asList(q1, q2, q3, q4, q5));
        long t2 = System.currentTimeMillis();

        System.out.println("Creating a batch took " + (t2 - t1) + " ms");

        List<List<Long>> times = new ArrayList<>();

        //TODO: Find out why queries won't execute in serial
        for (QueryBatcher.BatchQuery bq : combined) {
            System.out.println("EXEC ");
            long t5 = System.currentTimeMillis();
            RelNode rn = validator.getLogicalPlan(bq.query);
            ResultSet rs = optimizer.executeAndGetResult(rn);
            long t6 = System.currentTimeMillis();

            System.out.println("UNBA ");

            long t7 = System.currentTimeMillis();
            queryBatcher.unbatchResults3(bq, rs);
            long t8 = System.currentTimeMillis();

            times.add(Arrays.asList(t6 - t5, t8 - t7));
        }


        long exec_times = times.get(0).get(0) + times.get(1).get(0);
        long unbatch_times = times.get(0).get(1) + times.get(1).get(1);
//        long exec_times = times.get(0).get(0);
//        long unbatch_times = times.get(0).get(1);

        System.out.println("===============================");

        System.out.println("Executing queries individually took " + (t4 - t3) + " ms");
        System.out.println("Executing queries as a batch took " + (t2 - t1) + " + " + (exec_times) + " + " + (unbatch_times) + " = " + (exec_times + unbatch_times + (t2 - t1)) + " ms");
        System.out.println("Combined (" + combined.size() + ") are " + Arrays.toString(combined.toArray()));

    }

}
