import mv.MVHandler;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {

    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        DataSource dataSource = JdbcSchema.dataSource("jdbc:postgresql:tpch_test", "org.postgresql.Driver", "postgres", "vasu");
        rootSchema.add("public", JdbcSchema.create(rootSchema, "public", dataSource, null, null));

        Configuration configuration = new Configuration(rootSchema);
        QueryExecutor executor = new QueryExecutor(calciteConnection, configuration.getPlanner());

        try {
            executor.executeSql(Queries.q0);
            executor.executeRelNode(new MVHandler(calciteConnection).getDerivedPlan("MV0", Queries.mv0, Queries.q0));
        } catch (SqlParseException | ValidationException | RelConversionException e) {
            e.printStackTrace();
        }
    }
}
