package mv;

import common.QueryUtils;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;

public class Materialization {
    public RelNode queryRel;
    public RelNode tableRel;
    public RelNode queryRelCanonical;

    public Materialization(RelOptMaterialization mat) {
        this.queryRel = mat.queryRel;
        this.tableRel = mat.tableRel;
        this.queryRelCanonical = QueryUtils.canonicalize(mat.queryRel);
    }
}
