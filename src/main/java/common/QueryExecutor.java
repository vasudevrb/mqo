package common;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelRunner;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static common.QueryUtils.*;

public class QueryExecutor {

    private final CalciteConnectionConfig config;
    private final CalciteConnection connection;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;

    private final Pattern betweenPattern = Pattern.compile("(\\S*) BETWEEN( \\S*)? (\\S*) \\S* (\\S*)");

    public QueryExecutor(Configuration configuration) {
        this.config = configuration.config;
        this.connection = configuration.connection;
        this.validator = configuration.validator;
        this.converter = configuration.converter;
    }

    public SqlNode parse(String sql) {
        SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();
        parserConfig.setCaseSensitive(config.caseSensitive());
        parserConfig.setUnquotedCasing(config.unquotedCasing());
        parserConfig.setQuotedCasing(config.quotedCasing());
        parserConfig.setConformance(config.conformance());

        SqlParser parser = SqlParser.create(sql, parserConfig.build());

        try {
            return parser.parseStmt();
        } catch (SqlParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    public SqlNode validate(SqlNode node, boolean allowAggregations) {
        if (hasBetween(node)) {
            String where = replaceBetween(node).replace("`", "\"");
            String query = recreateQuery(node, where);
            return this.validate(query);
        }

        if (!allowAggregations && isAggregate(node)) {
            return this.validate(deAggregateQuery(node));
        }

        return validator.validate(node);
    }

    public SqlNode validate(SqlNode node) {
        return validate(node, true);
    }

    public SqlNode validate(String q, boolean allowAggregations) {
        return validate(parse(q), allowAggregations);
    }

    public SqlNode validate(String q) {
        return validate(q, true);
    }

    public RelNode getLogicalPlan(SqlNode node) {
        RelRoot root = converter.convertQuery(node, false, true);
        return root.rel;
    }

    public RelNode getLogicalPlan(String q) {
        return getLogicalPlan(validate(q));
    }

    public RelNode getPhysicalPlan(RelNode node) {
        RelOptCluster cluster = node.getCluster();
        VolcanoPlanner planner = (VolcanoPlanner) cluster.getPlanner();

        // Is already a physical node
        if (!planner.isLogical(node)) {
            return node;
        }

        RelNode newRoot = planner.changeTraits(node, cluster.traitSet().replace(EnumerableConvention.INSTANCE));
        planner.setRoot(newRoot);
        return planner.findBestExp();
    }

    public void execute(SqlNode node, Consumer<ResultSet> consumer) {
        execute(getLogicalPlan(node), consumer);
    }

    public void execute(RelNode relNode, Consumer<ResultSet> consumer) {
        try {
            _execute(relNode, consumer);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void _execute(RelNode relNode, Consumer<ResultSet> consumer) throws SQLException {
        long t1 = System.currentTimeMillis();
        RelNode physicalNode = getPhysicalPlan(relNode);
        long physicalTime = System.currentTimeMillis() - t1;

        connection.setAutoCommit(false);
        RelRunner runner = connection.unwrap(RelRunner.class);
        t1 = System.currentTimeMillis();
        PreparedStatement run = runner.prepareStatement(physicalNode);
        run.setFetchSize(10000);
        long compileTime = System.currentTimeMillis() - t1;

        t1 = System.currentTimeMillis();
        run.execute();
        long execTime = System.currentTimeMillis() - t1;

        System.out.printf("\nPlan: %dms, Compile: %dms, Exec: %dms\n", physicalTime, compileTime, execTime);

        ResultSet rs = run.getResultSet();
        if (consumer != null) consumer.accept(rs);
        run.close();
        rs.close();
    }

    private boolean hasBetween(SqlNode node) {
        return where(node).contains(" BETWEEN ");
    }

    private String replaceBetween(SqlNode node) {
        String where = where(node);
        Matcher matcher = betweenPattern.matcher(where);
        while (matcher.find()) {
            String predName = matcher.group(1);
            String predLow = matcher.group(3);
            String predHigh = matcher.group(4);

            where = matcher.replaceAll(matchResult -> matchResult.group(1) + " >= " + predLow +
                    " AND " + predName + " <= " + predHigh);
        }
        return where;
    }

    public String deAggregateQuery(SqlNode node) {
        return recreateQuery(node, selectList(node, false), where(node).replace("`", "\""), Collections.emptyList());
    }
}
