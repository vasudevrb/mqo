package common;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.*;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class CostHelper {

    private static Map<String, Integer> cardinalities;

    static {
        cardinalities = new HashMap<>();
        cardinalities.put("lineitem", 6_000_000);
        cardinalities.put("orders", 1_500_000);
        cardinalities.put("customer", 150_000);
        cardinalities.put("partsupp", 800_000);
        cardinalities.put("supplier", 10_000);
        cardinalities.put("part", 200_000);
        cardinalities.put("nation", 25);
        cardinalities.put("region", 5);
    }

    public static RelOptCost getCost(RelNode node, boolean isBatch) {
        RelOptCluster cl = node.getCluster();
        final ImmutableList<RelMetadataProvider> list = ImmutableList.of(SWCostImpl.SOURCE, cl.getMetadataProvider());
        cl.setMetadataProvider(ChainedRelMetadataProvider.of(list));
        cl.setMetadataQuerySupplier(SWMetadataQuery::new);

        cl.invalidateMetadataQuery();

        final SWMetadataQuery mq = (SWMetadataQuery) node.getCluster().getMetadataQuery();

        return mq.getSWCost(node, isBatch);
    }

    public interface SWCost extends Metadata {
        Method METHOD = Types.lookupMethod(SWCost.class, "getSWCost", boolean.class);

        MetadataDef<SWCost> DEF = MetadataDef.of(SWCost.class, SWCost.Handler.class, METHOD);

        RelOptCost getSWCost(boolean isBatch);

        interface Handler extends MetadataHandler<SWCost> {
            RelOptCost getSWCost(RelNode r, RelMetadataQuery mq, boolean isBatch);
        }

    }

    public static class SWCostImpl implements MetadataHandler<SWCost> {
        public static final RelMetadataProvider SOURCE =
                ReflectiveRelMetadataProvider.reflectiveSource(new SWCostImpl(), SWCost.Handler.class);

        @Override
        public MetadataDef<SWCost> getDef() {
            return SWCost.DEF;
        }

        public RelOptCost getSWCost(RelNode rel, RelMetadataQuery mq, boolean isBatch) {
            if (!isBatch) {
                return mq.getCumulativeCost(rel);
            }

            return mq.getCumulativeCost(rel).multiplyBy(0.9);
        }
    }

    private static class SWMetadataQuery extends RelMetadataQuery {
        private SWCost.Handler swCostHandler;

        SWMetadataQuery() {
            swCostHandler = initialHandler(SWCost.Handler.class);
        }

        public RelOptCost getSWCost(RelNode rel, boolean isBatch) {
            for (; ; ) {
                try {
                    return swCostHandler.getSWCost(rel, this, isBatch);
                } catch (JaninoRelMetadataProvider.NoHandler e) {
                    swCostHandler = revise(SWCost.Handler.class);
                }
            }

        }
    }
}
