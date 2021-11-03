package mv;

import com.google.common.collect.ImmutableList;
import common.Configuration;
import common.QueryExecutor;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

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

    public Pair<RelNode, List<RelOptMaterialization>> getMaterializations(String mv_name, String mv, String q) {
        List<RelOptMaterialization> materializations = new ArrayList<>();
        final RelBuilder builder = RelFactories.LOGICAL_BUILDER.create(cluster, catalogReader);
        final MaterializationService.DefaultTableFactory tableFactory = new MaterializationService.DefaultTableFactory();

        final RelNode mvRel = validator.getLogicalPlan(mv);
        long t1 = System.currentTimeMillis();
        final Table table = tableFactory.createTable(CalciteSchema.from(rootSchema), mv, ImmutableList.of(schema.getName()));
        long t2 = System.currentTimeMillis();

        System.out.println("Time taken to create table: " + (t2 - t1) + " ms");
        schema.add(mv_name, table);
        builder.scan(schema.getName(), mv_name);

        final LogicalTableScan logicalTableScan = (LogicalTableScan) builder.build();
        final EnumerableTableScan replacement = EnumerableTableScan.create(cluster, logicalTableScan.getTable());
        materializations.add(new RelOptMaterialization(replacement, mvRel, null, ImmutableList.of(schema.getName(), mv_name)));

        return new Pair<>(validator.getLogicalPlan(q), materializations);
    }

    public List<RelNode> optimize(Pair<RelNode, List<RelOptMaterialization>> mvData) {
        RelNode queryRel = mvData.left;
        RelOptMaterialization materialization = mvData.right.get(0);
        List<RelNode> substitutes =
                new SubstitutionVisitor(canonicalize(materialization.queryRel), canonicalize(queryRel))
                        .go(materialization.tableRel);
        return substitutes.stream().map(this::uncanonicalize).toList();
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
