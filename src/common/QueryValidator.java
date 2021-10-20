package common;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class QueryValidator {

    private final CalciteConnectionConfig config;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;

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

    public SqlNode validate(SqlNode node) {
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
}
