import batch.QueryBatcher;
import common.Configuration;
import common.QueryValidator;
import mv.Optimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
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

        String q3 = "SELECT \"s_suppkey\", \"s_name\" FROM \"public\".\"supplier\" WHERE \"s_suppkey\" < 1000";

        String q4 = "SELECT \"s_name\" FROM \"public\".\"supplier\" WHERE \"s_suppkey\" < 1200";

        String q5 = "SELECT \"s_suppkey\" FROM \"public\".\"supplier\" WHERE \"s_suppkey\" < 1500";

        SqlNode sqlNode = validator.validate(q1);
        SqlNode sqlNode2 = validator.validate(q2);

        QueryBatcher queryBatcher = new QueryBatcher(validator);

//        Operator op = batchQueryBuilder.build(sqlNode, sqlNode2);
//        System.out.println(op);

        long t1 = System.currentTimeMillis();
        List<QueryBatcher.BatchQuery> combined = queryBatcher.batch(Arrays.asList(q1, q2, q3, q4, q5));
        long t2 = System.currentTimeMillis();

        long t3 = System.currentTimeMillis();
        for (String s: Arrays.asList(q2)) {
            optimizer.execute(validator.getLogicalPlan(s));
        }
        long t4 = System.currentTimeMillis();



        long t5 = System.currentTimeMillis();
        RelNode rn = validator.getLogicalPlan(combined.get(0).query);

        ResultSet rs = optimizer.executeAndGetResult(rn);
        long t6 = System.currentTimeMillis();

        long t7 = System.currentTimeMillis();
        queryBatcher.unbatchResults(combined.get(0), rs);
        long t8 = System.currentTimeMillis();

        System.out.println("===============================");

        System.out.println("Executing queries individually took " + (t4 - t3) + " ms");
        System.out.println("Executing queries as a batch took " + (t2 - t1) + " + " + (t6 - t5)+" + " + (t8 - t7) + " = " + ((t8 - t7) + (t6 - t5) + (t2 - t1)) + " ms");
        System.out.println("Combined (" + combined.size() + ") are " + Arrays.toString(combined.toArray()));
        System.out.println("Combining took " + (t2 - t1) + " ms");

    }

}
