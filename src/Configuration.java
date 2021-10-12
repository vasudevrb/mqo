import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

public class Configuration {

    private final FrameworkConfig frameworkConfig;

    public Configuration(SchemaPlus rootSchema) {
        this.frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(getParserConfig())
                .sqlToRelConverterConfig(getSqlToRelConverterConfig())
                .defaultSchema(rootSchema)
                .build();
    }

    public FrameworkConfig getFrameworkConfig() {
        return frameworkConfig;
    }

    public Planner getPlanner() {
        return Frameworks.getPlanner(frameworkConfig);
    }

    private SqlParser.Config getParserConfig() {
        return SqlParser.configBuilder()
                .setCaseSensitive(false)
                .build();
    }

    private SqlToRelConverter.Config getSqlToRelConverterConfig() {
        return SqlToRelConverter.config()
                .withTrimUnusedFields(false)
                .withExpand(true)
                .withDecorrelationEnabled(true);
    }
}
