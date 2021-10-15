import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.util.Pair;

import java.io.PrintWriter;
import java.util.List;

import static org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES;

public class Main {

    public static void main(String[] args) throws Exception {
        Optimizer optimizer = Optimizer.create();

        RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), ALL_ATTRIBUTES, false);

//        optimizer.executeRelNode(optimizer.convert(optimizer.validate(optimizer.parse(Queries.q3))));


//        optimizer.optimize(optimizer.getMaterializations("MV0", Queries.mv0, Queries.q0)).get(0).explain(relWriter);
//        optimizer.executeRelNode(optimizer.optimize(optimizer.getMaterializations("MV0", Queries.mv0, Queries.q0)).get(0));

        Pair<RelNode, List<RelOptMaterialization>> m = optimizer.getMaterializations("MV2", Queries.mv2, Queries.q2);
        RelNode rr = optimizer.runrun(optimizer.optimize(m).get(0), m.right);
        rr.explain(relWriter);
        optimizer.executeRelNode(rr);
    }
}
