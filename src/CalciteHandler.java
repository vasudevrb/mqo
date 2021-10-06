import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.*;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;

public class CalciteHandler {

    public void parseAndValidateSql() throws SQLException, SqlParseException, ValidationException, RelConversionException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        DataSource dataSource = JdbcSchema.dataSource("jdbc:postgresql:tpch_test", "org.postgresql.Driver", "postgres", "vasu");
        rootSchema.add("public", JdbcSchema.create(rootSchema, "public", dataSource, null, null));

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

        String sql = "SELECT * FROM \"public\".\"region\"";

        Planner planner = Frameworks.getPlanner(config);
        SqlNode node = planner.parse(sql);
        SqlNode validated = planner.validate(node);

        RelRoot relRoot = planner.rel(validated);
        RelNode relNode = relRoot.project();
        final RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES, false);
        relNode.explain(relWriter);


        PreparedStatement run = RelRunners.run(relNode);
        ResultSet rs = connection.prepareStatement(sql).executeQuery();

        System.out.println("Result is");
        while (rs.next()) {
            for (int i = 1; i < rs.getMetaData().getColumnCount(); i++) {
                System.out.print(rs.getObject(i) + ", ");
            }
            System.out.println();
        }

        SqlWriter writer = new SqlPrettyWriter();
        validated.unparse(writer, 0, 0);

        System.out.println(ImmutableList.of(writer.toSqlString().getSql()));
    }
}
