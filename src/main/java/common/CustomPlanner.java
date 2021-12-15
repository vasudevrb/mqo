package common;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CustomPlanner extends VolcanoPlanner {

    public static long diff = 0;

    public CustomPlanner(@Nullable RelOptCostFactory costFactory, @Nullable Context externalContext) {
        super(costFactory, externalContext);
    }

    @Override
    public @Nullable RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
        if (rel instanceof EnumerableRel) {
//            long t1 = System.currentTimeMillis();
            RelOptCost cost = super.getCost(rel, mq);
//            diff += (System.currentTimeMillis() - t1);
            return cost;
        }

        return super.getCost(rel, mq);
    }
}
