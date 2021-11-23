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
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static common.QueryUtils.recreateQuery;
import static common.QueryUtils.where;

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

    public SqlNode validate(SqlNode node) {
        if (hasBetween(node)) {
            String where = replaceBetween(node).replace("`", "\"");
            String query = recreateQuery(node, where);
            return this.validate(query);
        }
        return validator.validate(node);
    }

    public SqlNode validate(String q) {
        return validate(parse(q));
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

    public RelNode getPhysicalPlan(String q) {
        return getPhysicalPlan(getLogicalPlan(q));
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
        RelNode physicalNode = getPhysicalPlan(relNode);

        //TODO: Try making runner global
        RelRunner runner = connection.unwrap(RelRunner.class);
        long t1 = System.nanoTime();
        PreparedStatement run = runner.prepareStatement(physicalNode);
        long t2 = System.nanoTime();

        long t3 = System.nanoTime();
        run.execute();
        long t4 = System.nanoTime();

        System.out.println("Executed query. Compile: " + (t2 - t1) / 1000000 + " ms, Execute: " + (t4 - t3) / 1000000 + " ms");

        if (consumer != null) consumer.accept(run.getResultSet());
        run.close();
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
}
