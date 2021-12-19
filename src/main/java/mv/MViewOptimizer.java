package mv;

import common.Configuration;
import common.QueryExecutor;
import common.Utils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.MaterializationService.DefaultTableFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

import static org.apache.calcite.rel.core.RelFactories.LOGICAL_BUILDER;

public class MViewOptimizer {
    private final SchemaPlus rootSchema;
    private final SchemaPlus schema;

    private final QueryExecutor validator;

    private final Prepare.CatalogReader catalogReader;
    private final RelOptCluster cluster;

    public MViewOptimizer(Configuration programConfig) {
        this.rootSchema = programConfig.rootSchema;
        this.schema = programConfig.schema;
        this.catalogReader = programConfig.catalogReader;
        this.cluster = programConfig.cluster;

        this.validator = new QueryExecutor(programConfig);
    }

    public RelOptMaterialization materialize(String mViewQuery, RelNode node) {
        String mViewName = Utils.randomString(7);
        long t1 = System.currentTimeMillis();
        //Create a table with the given materialized view query
        DefaultTableFactory tableFactory = new DefaultTableFactory();
        Table table = tableFactory.createTable(CalciteSchema.from(rootSchema), mViewQuery, List.of(schema.getName()));
        schema.add(mViewName, table);

        //Create a logical scan on the table and convert it to enumerable (physical) scan
        RelBuilder builder = LOGICAL_BUILDER.create(cluster, catalogReader);
        builder.scan(schema.getName(), mViewName);
        LogicalTableScan logicalTableScan = (LogicalTableScan) builder.build();

        //RelOptMaterialization records that this table represents this query. Required for substitution
        RelOptMaterialization m = new RelOptMaterialization(logicalTableScan, node, null, List.of(schema.getName(), mViewName));

        long t2 = System.currentTimeMillis();
        System.out.println("Materializing view took " + (t2 - t1) + " ms");

        return m;
    }

    public RelNode substitute(RelOptMaterialization materialization, RelNode query) {
        //TODO: Return substitutes that are cheapest to execute
        long t1 = System.currentTimeMillis();
        //TODO: Profile canonicalize. maybe this can be extracted out.
        List<RelNode> substitutes = new SubstitutionVisitor(canonicalize(materialization.queryRel), canonicalize(query))
                .go(materialization.tableRel);
        RelNode node = substitutes.stream().findFirst().map(this::uncanonicalize).orElse(null);

        if (node == null) {
            return null;
        }


        long t2 = System.currentTimeMillis();
        System.out.println("Matching MV with query took " + (t2 - t1) + " ms");
        return node;
    }

    private RelNode canonicalize(RelNode rel) {
        HepProgram program =
                new HepProgramBuilder()
                        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
                        .addRuleInstance(CoreRules.FILTER_MERGE)
                        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                        .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
                        .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
                        .addRuleInstance(CoreRules.PROJECT_MERGE)
                        .addRuleInstance(CoreRules.PROJECT_REMOVE)
                        .addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE)
                        .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
                        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS)
                        .addRuleInstance(CoreRules.FILTER_TO_CALC)
                        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
                        .addRuleInstance(CoreRules.FILTER_CALC_MERGE)
                        .addRuleInstance(CoreRules.PROJECT_CALC_MERGE)
                        .addRuleInstance(CoreRules.CALC_MERGE)
                        .build();

        final HepPlanner hepPlanner = new HepPlanner(program);
        hepPlanner.setRoot(rel);
        return hepPlanner.findBestExp();
    }

    private RelNode uncanonicalize(RelNode rel) {
        HepProgram program =
                new HepProgramBuilder()
                        .addRuleInstance(CoreRules.CALC_SPLIT)
                        .addRuleInstance(CoreRules.CALC_REMOVE)
                        .build();

        final HepPlanner hepPlanner = new HepPlanner(program);
        hepPlanner.setRoot(rel);
        return hepPlanner.findBestExp();
    }
}
