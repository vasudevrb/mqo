package mv;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class MVHandler {

    private CalciteConnection calciteConnection;
    private FrameworkConfig config;

    public MVHandler(CalciteConnection calciteConnection, FrameworkConfig config) {
        this.calciteConnection = calciteConnection;
        this.config = config;
    }

    public boolean isDerivable(String mvId, String mv, String query) {
        return getDerivedPlan(mvId, mv, query) != null;
    }

    public RelNode getDerivedPlan(String mvId, String mv, String query) {
        QueryConf conf = new QueryConf("schema", ImmutableList.of(Pair.of(mv, mvId)), query);
        final TestConfig testConfig = build(conf);

        final List<RelNode> substitutes = optimize(testConfig);
        return substitutes.isEmpty() ? null : substitutes.get(0);
    }

    private List<RelNode> optimize(TestConfig testConfig) {
        RelNode queryRel = testConfig.queryRel;
        RelOptMaterialization materialization = testConfig.materializations.get(0);
        return new SubstitutionVisitor(canonicalize(materialization.queryRel), canonicalize(queryRel))
                .go(materialization.tableRel);
    }

    private List<RelNode> optimize2(TestConfig testConfig) {
        RelNode queryRel = testConfig.queryRel;
        RelOptPlanner planner = queryRel.getCluster().getPlanner();
        RelTraitSet traitSet = queryRel.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
        RelOptUtil.registerDefaultRules(planner, true, false);
        return ImmutableList.of(
                Programs.standard().run(planner, queryRel, traitSet, testConfig.materializations, ImmutableList.of()));
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

    private TestConfig build(QueryConf sql) {
        return Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
            cluster.getPlanner().setExecutor(new RexExecutorImpl(DataContexts.EMPTY));
            try {
                DataSource dataSource = JdbcSchema.dataSource("jdbc:postgresql:tpch_test", "org.postgresql.Driver", "postgres", "vasu");
                SchemaPlus defaultSchema = rootSchema.add("public", JdbcSchema.create(rootSchema, "public", dataSource, null, null));

                final RelNode queryRel = toRel(cluster, rootSchema, defaultSchema, sql.query);

                final List<RelOptMaterialization> mvs = new ArrayList<>();
                final RelBuilder relBuilder =
                        RelFactories.LOGICAL_BUILDER.create(cluster, relOptSchema);
                final MaterializationService.DefaultTableFactory tableFactory =
                        new MaterializationService.DefaultTableFactory();
                for (Pair<String, String> pair : sql.materializedViews) {
                    final RelNode mvRel = toRel(cluster, rootSchema, defaultSchema, pair.left);
                    final Table table = tableFactory.createTable(CalciteSchema.from(rootSchema),
                            pair.left, ImmutableList.of(defaultSchema.getName()));
                    defaultSchema.add(pair.right, table);
                    relBuilder.scan(defaultSchema.getName(), pair.right);
                    final LogicalTableScan logicalScan = (LogicalTableScan) relBuilder.build();
                    final EnumerableTableScan replacement =
                            EnumerableTableScan.create(cluster, logicalScan.getTable());
                    mvs.add(
                            new RelOptMaterialization(replacement, mvRel, null,
                                    ImmutableList.of(defaultSchema.getName(), pair.right)));
                }
                return new TestConfig(defaultSchema.getName(), queryRel, mvs);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }, config);
    }

    public <E extends Throwable> RuntimeException rethrow(Throwable e) throws E {
        if (e instanceof InvocationTargetException) {
            e = e.getCause();
        }
        throw (E) e;
    }

    private RelNode toRel(RelOptCluster cluster, SchemaPlus rootSchema,
                          SchemaPlus defaultSchema, String sql) throws SqlParseException, SQLException {
        final SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
        final SqlNode parsed = parser.parseStmt();

        final CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(defaultSchema).path(null),
                new JavaTypeFactoryImpl(),
                CalciteConnectionConfig.DEFAULT);

        final SqlValidator validator = new ValidatorForTest(SqlStdOperatorTable.instance(),
                catalogReader, new JavaTypeFactoryImpl(), SqlConformanceEnum.DEFAULT);
        final SqlNode validated = validator.validate(parsed);
        final SqlToRelConverter.Config config = SqlToRelConverter.config()
                .withTrimUnusedFields(false)
                .withExpand(true)
                .withDecorrelationEnabled(true);
        final SqlToRelConverter converter = new SqlToRelConverter(
                (rowType, queryString, schemaPath, viewPath) -> {
                    throw new UnsupportedOperationException("cannot expand view");
                }, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, config);


        return converter.convertQuery(validated, false, true).rel;
    }

    private static class ValidatorForTest extends SqlValidatorImpl {
        ValidatorForTest(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
                         RelDataTypeFactory typeFactory, SqlConformance conformance) {
            super(opTab, catalogReader, typeFactory, Config.DEFAULT.withSqlConformance(conformance));
        }
    }

    protected static class TestConfig {
        public final String defaultSchema;
        public final RelNode queryRel;
        public final List<RelOptMaterialization> materializations;

        public TestConfig(String defaultSchema, RelNode queryRel, List<RelOptMaterialization> materializations) {
            this.defaultSchema = defaultSchema;
            this.queryRel = queryRel;
            this.materializations = materializations;
        }
    }

    static class QueryConf {
        public String schemaName;
        public List<Pair<String, String>> materializedViews;
        public String query;

        public QueryConf(String schemaName, List<Pair<String, String>> materializedViews, String query) {
            this.schemaName = schemaName;
            this.materializedViews = materializedViews;
            this.query = query;
        }
    }
}
