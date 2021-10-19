import batch.BatchQueryBuilder;
import batch.Operator;
import mv.Optimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import java.sql.SQLException;
import java.util.List;

public class Tester {
    private Optimizer optimizer;

    public Tester() throws SQLException {
        this.optimizer = Optimizer.create();
    }

    public void testMVSubstitution() throws Exception {
        //Regular execution
        RelNode regNode = optimizer.convert(optimizer.validate(optimizer.parse(Queries.q3)));
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
                " \"l_shipdate\" >= date '1994-01-01'" +
                " AND \"l_shipdate\" < date '1994-06-01'" +
                " AND \"l_discount\" > 0.07" +
                " AND \"l_quantity\" < 25";

        String q2 = "SELECT \"l_extendedprice\"" +
                " FROM \"public\".\"lineitem\"" +
                " WHERE" +
                " \"l_shipdate\" > date '1994-02-01'" +
                " AND \"l_quantity\" <= 30" +
                " AND \"l_discount\" < 0.06";

        SqlNode sqlNode = optimizer.validate(optimizer.parse(q1));
        SqlNode sqlNode2 = optimizer.validate(optimizer.parse(q2));

        BatchQueryBuilder batchQueryBuilder = new BatchQueryBuilder();

        Operator op = batchQueryBuilder.build(sqlNode, sqlNode2);
        System.out.println(op);
    }

}
