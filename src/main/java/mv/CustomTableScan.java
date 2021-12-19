package mv;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Random;

public class CustomTableScan extends EnumerableTableScan {

    public CustomTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, Class elementType) {
        super(cluster, traitSet, table, elementType);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return RelOptCostImpl.FACTORY.makeCost(new Random().nextDouble(200, 10000), 2, 2);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        //TODO: Proper row counts
        return new Random().nextDouble(200, 10000);
    }

    public static CustomTableScan create(RelOptCluster cluster,
                                         RelOptTable relOptTable) {
        final Table table = relOptTable.unwrap(Table.class);
        Class elementType = CustomTableScan.deduceElementType(table);
        final RelTraitSet traitSet =
                cluster.traitSetOf(EnumerableConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                            if (table != null) {
                                return table.getStatistic().getCollations();
                            }
                            return ImmutableList.of();
                        });
        return new CustomTableScan(cluster, traitSet, relOptTable, elementType);
    }
}
