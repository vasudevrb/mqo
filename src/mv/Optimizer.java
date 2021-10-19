package mv;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Pair;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Optimizer {
    private final SchemaPlus rootSchema;
    private final SchemaPlus schema;

    private final CalciteConnectionConfig config;
    private final CalciteConnection connection;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;

    private final Prepare.CatalogReader catalogReader;
    private final VolcanoPlanner planner;
    private final RelOptCluster cluster;

    //TODO reorganize this class
    public Optimizer(SchemaPlus rootSchema, SchemaPlus schema, CalciteConnectionConfig config,
                     CalciteConnection connection, SqlValidator validator, SqlToRelConverter converter,
                     Prepare.CatalogReader catalogReader, VolcanoPlanner planner, RelOptCluster cluster) {
        this.rootSchema = rootSchema;
        this.schema = schema;
        this.config = config;
        this.connection = connection;
        this.validator = validator;
        this.converter = converter;
        this.catalogReader = catalogReader;
        this.planner = planner;
        this.cluster = cluster;
    }

    public static Optimizer create() throws SQLException {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        DataSource dataSource = JdbcSchema.dataSource("jdbc:postgresql:tpch_test", "org.postgresql.Driver", "postgres", "vasu");
        SchemaPlus defaultSchema = rootSchema.add("public", JdbcSchema.create(rootSchema, "public", dataSource, null, null));

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema.from(rootSchema),
                Collections.singletonList(defaultSchema.getName()), typeFactory, config);

        SqlOperatorTable operatorTable = new ChainedSqlOperatorTable(ImmutableList.of(SqlStdOperatorTable.instance()));

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withSqlConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);

        SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);

        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(true)
                .withExpand(false) // https://issues.apache.org/jira/browse/CALCITE-1045
                .build();

        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );

        return new Optimizer(rootSchema, defaultSchema, config, calciteConnection,
                validator, converter, catalogReader, planner, cluster);
    }

    public SqlNode parse(String sql) throws Exception {
        SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();
        parserConfig.setCaseSensitive(config.caseSensitive());
        parserConfig.setUnquotedCasing(config.unquotedCasing());
        parserConfig.setQuotedCasing(config.quotedCasing());
        parserConfig.setConformance(config.conformance());

        SqlParser parser = SqlParser.create(sql, parserConfig.build());

        return parser.parseStmt();
    }

    public SqlNode validate(SqlNode node) {
        return validator.validate(node);
    }

    public RelNode convert(SqlNode node) {
        RelRoot root = converter.convertQuery(node, false, true);
        return root.rel;
    }

    public Pair<RelNode, List<RelOptMaterialization>> getMaterializations(String mv_name, String mv, String q) throws Exception {
        List<RelOptMaterialization> materializations = new ArrayList<>();
        final RelBuilder builder = RelFactories.LOGICAL_BUILDER.create(cluster, catalogReader);
        final MaterializationService.DefaultTableFactory tableFactory = new MaterializationService.DefaultTableFactory();

        final RelNode mvRel = convert(validate(parse(mv)));
        long t1 = System.currentTimeMillis();
        final Table table = tableFactory.createTable(CalciteSchema.from(rootSchema), mv, ImmutableList.of(schema.getName()));
        long t2 = System.currentTimeMillis();

        System.out.println("Time taken to create table: " + (t2 - t1) + " ms");
        schema.add(mv_name, table);
        builder.scan(schema.getName(), mv_name);

        final LogicalTableScan logicalTableScan = (LogicalTableScan) builder.build();
        final EnumerableTableScan replacement = EnumerableTableScan.create(cluster, logicalTableScan.getTable());
        materializations.add(new RelOptMaterialization(replacement, mvRel, null, ImmutableList.of(schema.getName(), mv_name)));

        return new Pair<>(convert(validate(parse(q))), materializations);
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

    public void execute(RelNode relNode) throws SQLException {
        RelRunner runner = connection.unwrap(RelRunner.class);
        long t3 = System.currentTimeMillis();
        PreparedStatement run = runner.prepareStatement(relNode);
        long t4 = System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        run.execute();
        long t2 = System.currentTimeMillis();
        ResultSet rs = run.getResultSet();

        int count = 0;
        while (rs.next()) {
//            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
//                System.out.print(rs.getObject(i) + ", ");
//            }
//            System.out.println();
            count++;
        }

        System.out.println("Compiling query took " + (t4 -t3) + " ms");
        System.out.println("Successfully executed query! Row count: " + count + " Time: " + (t2 - t1) + "ms");
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
