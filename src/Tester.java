import batch.QueryBatcher;
import batch.Operator;
import common.Configuration;
import common.QueryValidator;
import mv.Optimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import java.sql.SQLException;
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
        String q1 = "SELECT \"l_extendedprice\"" +
                " FROM \"public\".\"lineitem\"" +
                " WHERE" +
                " \"l_shipdate\" >= date '2018-09-09'" +
                " AND \"l_shipdate\" < date '2018-10-09'" +
                " AND \"l_discount\" > 0.07" +
                " AND \"l_quantity\" > 25";

        String q2 = "SELECT \"l_extendedprice\"" +
                " FROM \"public\".\"lineitem\"" +
                " WHERE" +
                " \"l_shipdate\" > date '2019-10-01'" +
                " AND \"l_quantity\" > 30";

        SqlNode sqlNode = validator.validate(q1);
        SqlNode sqlNode2 = validator.validate(q2);

        QueryBatcher queryBatcher = new QueryBatcher();

//        Operator op = batchQueryBuilder.build(sqlNode, sqlNode2);
//        System.out.println(op);

        long t1 = System.currentTimeMillis();
        Operator op = queryBatcher.build(sqlNode, sqlNode2);
        long t2 = System.currentTimeMillis();

        System.out.println("Combining took " + (t2 - t1) + " ms");

    }

}
