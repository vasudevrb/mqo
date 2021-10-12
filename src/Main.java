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

    public static void main(String[] args) throws SQLException, ValidationException, SqlParseException, RelConversionException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        DataSource dataSource = JdbcSchema.dataSource("jdbc:postgresql:tpch_test", "org.postgresql.Driver", "postgres", "vasu");
        rootSchema.add("public", JdbcSchema.create(rootSchema, "public", dataSource, null, null));

        Configuration configuration = new Configuration(rootSchema);
        QueryExecutor executor = new QueryExecutor(calciteConnection, configuration.getPlanner());
        executor.printPlan = true;
//        executor.printOutput = true;

        MVHandler mvHandler = new MVHandler(calciteConnection, configuration.getFrameworkConfig());

        long t1 = System.currentTimeMillis();
        executor.executeSql(Queries.q2);
        long t2 = System.currentTimeMillis();

        long t3 = System.currentTimeMillis();
        executor.executeRelNode(mvHandler.getDerivedPlan("MV2", Queries.mv2, Queries.q2));
        long t4 = System.currentTimeMillis();

        System.out.println("Normal exec: " + (t2 - t1) + "ms, MV exec: " + (t4 - t3) + " ms");
    }
}
