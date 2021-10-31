package mv;

import com.google.common.collect.ImmutableList;
import common.Configuration;
import common.QueryValidator;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Pair;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class Optimizer {
    private final SchemaPlus rootSchema;
    private final SchemaPlus schema;

    private final CalciteConnection connection;
    private final QueryValidator validator;

    private final Prepare.CatalogReader catalogReader;
    private final VolcanoPlanner planner;
    private final RelOptCluster cluster;

    public Optimizer(Configuration programConfig) {
        this.rootSchema = programConfig.rootSchema;
        this.schema = programConfig.schema;
        this.connection = programConfig.connection;
        this.catalogReader = programConfig.catalogReader;
        this.planner = programConfig.planner;
        this.cluster = programConfig.cluster;

        this.validator = new QueryValidator(programConfig);
    }

    public Pair<RelNode, List<RelOptMaterialization>> getMaterializations(String mv_name, String mv, String q) throws Exception {
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
        return substitutes;
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

    public void executeAndGetResult(RelNode relNode, Consumer<ResultSet> consumer) throws SQLException {
        RelOptCluster cl = relNode.getCluster();
        RelTraitSet desired = cl.traitSet().replace(EnumerableConvention.INSTANCE);
        VolcanoPlanner pl = (VolcanoPlanner) cl.getPlanner();
        RelNode newRoot = pl.changeTraits(relNode, desired);
        pl.setRoot(newRoot);
        RelNode n2 = pl.findBestExp();

        RelRunner runner = connection.unwrap(RelRunner.class);
        long t1 = System.nanoTime();
        PreparedStatement run = runner.prepareStatement(n2);
        long t2 = System.nanoTime();

        long t3 = System.nanoTime();
        run.execute();
        long t4 = System.nanoTime();


        System.out.println("Executed query. Compile: " + (t2 - t1) / 1000000 + " ms, Execute: " + (t4 - t3) / 1000000 + " ms");
        consumer.accept(run.getResultSet());
        run.close();
    }

    public RelNode getPhysicalPlan(RelNode relNode, List<RelOptMaterialization> materializations) {
        RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_FILTER_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE
        );

        Program program = Programs.of(rules);
        return program.run(planner, relNode,
                relNode.getTraitSet().plus(EnumerableConvention.INSTANCE), materializations, Collections.emptyList());
    }

    public RelNode getPhysicalPlan(RelNode node) {
        return getPhysicalPlan(node, Collections.emptyList());
    }
}
