package common;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Configuration {
    public SchemaPlus rootSchema;
    public SchemaPlus schema;

    public CalciteConnectionConfig config;
    public CalciteConnection connection;
    public SqlValidator validator;
    public SqlToRelConverter converter;

    public Prepare.CatalogReader catalogReader;
    public VolcanoPlanner planner;
    public RelOptCluster cluster;

    public List<String> tableNames;

    private Configuration(SchemaPlus rootSchema, SchemaPlus schema, CalciteConnectionConfig config,
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

    public static Configuration initialize() throws SQLException {
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

        List<String> tabNames = new ArrayList<>();
        ResultSet rs = connection.getMetaData().getTables("tpch_test", null, "%", new String[]{"TABLE"});
        while (rs.next()) {
            tabNames.add(rs.getString(3));
        }

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

        planner.addRule(PruneEmptyRules.PROJECT_INSTANCE);
//        planner.addRule(Bindables.BINDABLE_SORT_RULE);
//        planner.addRule(Bindables.BINDABLE_VALUES_RULE);
//        planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
//        planner.addRule(Bindables.BINDABLE_FILTER_RULE);
//        planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
//        planner.addRule(Bindables.BINDABLE_JOIN_RULE);
//        planner.addRule(Bindables.FROM_NONE_RULE);
        planner.addRule(EnumerableRules.TO_BINDABLE);
//
        EnumerableRules.rules().stream()
                .filter(rule -> rule != EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
                .forEach(planner::addRule);
//        planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE); //TODO Find out why individual join queries won't work with this rule
//        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false); // https://issues.apache.org/jira/browse/CALCITE-1045

        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );

        Configuration configuration = new Configuration(rootSchema, defaultSchema, config, calciteConnection,
                validator, converter, catalogReader, planner, cluster);
        configuration.tableNames = tabNames;

        return configuration;

    }
}
