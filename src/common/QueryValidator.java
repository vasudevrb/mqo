package common;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static common.QueryUtils.*;

public class QueryValidator {

    private final CalciteConnectionConfig config;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;

    private Pattern betweenPattern = Pattern.compile("([`a-zA-Z0-9_\"@$#]+) BETWEEN (ASYMMETRIC )?(\\d+) AND (\\d+)");

    public QueryValidator(Configuration configuration) {
        this.config = configuration.config;
        this.validator = configuration.validator;
        this.converter = configuration.converter;
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

    public SqlNode validate(SqlNode node) throws Exception {
        if (hasBetween(node)) {
            String where = replaceBetween(node).replace("`", "\"");
            String query = "SELECT " + String.join(",", selectList(node)) + " FROM " + from(node) + " WHERE " + where ;
            return this.validate(query);
        }
        return validator.validate(node);
    }

    public SqlNode validate(String q) throws Exception {
        return validate(parse(q));
    }

    public RelNode getLogicalPlan(SqlNode node) {
        RelRoot root = converter.convertQuery(node, false, true);
        return root.rel;
    }

    public RelNode getLogicalPlan(String q) throws Exception {
        return getLogicalPlan(validate(parse(q)));
    }

    public boolean hasBetween(SqlNode node) {
        return where(node).contains(" BETWEEN ");
    }

    public String replaceBetween(SqlNode node) {
        String where = where(node);
        Matcher matcher = betweenPattern.matcher(where);
        while (matcher.find()) {
            String predName = matcher.group(1);
            String predLow = matcher.group(3);
            String predHigh = matcher.group(4);

            where = matcher.replaceAll(matchResult -> matchResult.group(1) + " >= " + predLow + " AND " + predName + " <= " + predHigh);
        }
        return where;
    }
}
