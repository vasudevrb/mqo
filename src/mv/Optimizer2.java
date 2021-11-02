package mv;

import com.google.common.collect.ImmutableList;
import common.Configuration;
import common.QueryExecutor;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
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
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class Optimizer2 {

    private final SchemaPlus rootSchema;
    private final SchemaPlus schema;

    private final QueryExecutor validator;

    private final Prepare.CatalogReader catalogReader;
    private final VolcanoPlanner planner;
    private final RelOptCluster cluster;


    public Optimizer2(Configuration programConfig) {
        this.rootSchema = programConfig.rootSchema;
        this.schema = programConfig.schema;
        this.catalogReader = programConfig.catalogReader;
        this.planner = programConfig.planner;
        this.cluster = programConfig.cluster;

        this.validator = new QueryExecutor(programConfig);
    }

    public List<RelNode> optimize(String mvName, String mv, String q) throws Exception {
//        TestConfig testConfig = build(new Sql(q, List.of(new Pair<>(mvName, mv))));
        TestConfig testConfig = getMaterializations(new Sql(q, List.of(new Pair<>(mvName, mv))));
        RelNode queryRel = testConfig.queryRel;
        RelOptPlanner planner = queryRel.getCluster().getPlanner();
        RelTraitSet traitSet = queryRel.getCluster().traitSet()
                .replace(BindableConvention.INSTANCE);
        RelOptUtil.registerDefaultRules(planner, true, false);
        return ImmutableList.of(
                Programs.standard().run(
                        planner, queryRel, traitSet, testConfig.materializations, ImmutableList.of()));
    }

    public TestConfig getMaterializations(Sql sql) throws Exception {
        List<RelOptMaterialization> materializations = new ArrayList<>();
        final RelBuilder builder = RelFactories.LOGICAL_BUILDER.create(cluster, catalogReader);
        final MaterializationService.DefaultTableFactory tableFactory = new MaterializationService.DefaultTableFactory();

        final RelNode mvRel = validator.getLogicalPlan(sql.materializations.get(0).right);
        long t1 = System.currentTimeMillis();
        final Table table = tableFactory.createTable(CalciteSchema.from(rootSchema), sql.materializations.get(0).right, List.of(schema.getName()));
        long t2 = System.currentTimeMillis();

        System.out.println("Time taken to create table: " + (t2 - t1) + " ms");
        schema.add(sql.materializations.get(0).left, table);
        builder.scan(schema.getName(), sql.materializations.get(0).left);

        final LogicalTableScan logicalTableScan = (LogicalTableScan) builder.build();
        final EnumerableTableScan replacement = EnumerableTableScan.create(cluster, logicalTableScan.getTable());
        materializations.add(new RelOptMaterialization(replacement, mvRel, null, ImmutableList.of(schema.getName(), sql.materializations.get(0).left)));

        return new TestConfig(schema.getName(),toRel(cluster, rootSchema, schema, sql.query), materializations);
    }

    private TestConfig build(Sql sql) {
        assert sql != null;
        return Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
            cluster.getPlanner().setExecutor(new RexExecutorImpl(DataContexts.EMPTY));
            try {
//                final SchemaPlus defaultSchema;
//                if (sql.getDefaultSchemaSpec() == null) {
//                    defaultSchema = rootSchema.add("hr",
//                            new ReflectiveSchema(new MaterializationTest.HrFKUKSchema()));
//                } else {
//                    defaultSchema = CalciteAssert.addSchema(rootSchema, sql.getDefaultSchemaSpec());
//                }
                final RelNode queryRel = toRel(cluster, rootSchema, schema, sql.getQuery());
                final List<RelOptMaterialization> mvs = new ArrayList<>();
                final RelBuilder relBuilder =
                        RelFactories.LOGICAL_BUILDER.create(cluster, relOptSchema);
                final MaterializationService.DefaultTableFactory tableFactory =
                        new MaterializationService.DefaultTableFactory();
                for (Pair<String, String> pair : sql.getMaterializations()) {
                    final RelNode mvRel = toRel(cluster, rootSchema, schema, pair.left);
                    final Table table = tableFactory.createTable(CalciteSchema.from(rootSchema),
                            pair.left, ImmutableList.of(schema.getName()));
                    schema.add(pair.right, table);
                    relBuilder.scan(schema.getName(), pair.right);
                    final LogicalTableScan logicalScan = (LogicalTableScan) relBuilder.build();
                    final EnumerableTableScan replacement =
                            EnumerableTableScan.create(cluster, logicalScan.getTable());
                    mvs.add(
                            new RelOptMaterialization(replacement, mvRel, null,
                                    ImmutableList.of(schema.getName(), pair.right)));
                }
                return new TestConfig(schema.getName(), queryRel, mvs);
            } catch (Exception e) {
                throw rethrow(e);
            }
        });
    }

    public <E extends Throwable> RuntimeException rethrow(Throwable e) throws E {
        if (e instanceof InvocationTargetException) {
            e = e.getCause();
        }
        throw (E) e;
    }

    private RelNode toRel(RelOptCluster cluster, SchemaPlus rootSchema,
                          SchemaPlus defaultSchema, String sql) throws SqlParseException {
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
                .withTrimUnusedFields(true)
                .withExpand(true)
                .withDecorrelationEnabled(true);
        final SqlToRelConverter converter = new SqlToRelConverter(
                (rowType, queryString, schemaPath, viewPath) -> {
                    throw new UnsupportedOperationException("cannot expand view");
                }, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, config);
        return converter.convertQuery(validated, false, true).rel;
    }

    private static class TestConfig {
        public final String defaultSchema;
        public final RelNode queryRel;
        public final List<RelOptMaterialization> materializations;

        public TestConfig(String defaultSchema, RelNode queryRel,
                          List<RelOptMaterialization> materializations) {
            this.defaultSchema = defaultSchema;
            this.queryRel = queryRel;
            this.materializations = materializations;
        }
    }

    private static class ValidatorForTest extends SqlValidatorImpl {
        ValidatorForTest(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
                         RelDataTypeFactory typeFactory, SqlConformance conformance) {
            super(opTab, catalogReader, typeFactory, Config.DEFAULT.withSqlConformance(conformance));
        }
    }

    private static class Sql {
        private String query;
        private List<Pair<String, String>> materializations;

        public Sql(String query, List<Pair<String, String>> materializations) {
            this.query = query;
            this.materializations = materializations;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public List<Pair<String, String>> getMaterializations() {
            return materializations;
        }

        public void setMaterializations(List<Pair<String, String>> materializations) {
            this.materializations = materializations;
        }
    }

}
