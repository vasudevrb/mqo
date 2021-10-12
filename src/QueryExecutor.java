import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.tools.ValidationException;

import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES;

public class QueryExecutor {

    private final Planner planner;
    private final CalciteConnection connection;
    public boolean printPlan;
    public boolean printOutput;

    public QueryExecutor(CalciteConnection connection, Planner planner) {
        this.connection = connection;
        this.planner = planner;
    }

    public void executeSql(String sql) throws SqlParseException, ValidationException, RelConversionException, SQLException {
        //Parse and validate SQL first. If successful, we have an AST
        SqlNode node = planner.parse(sql);
        SqlNode validated = planner.validate(node);

        //Convert the AST to relational tree and execute
        RelRoot relRoot = planner.rel(validated);
        RelNode relNode = relRoot.project();

        if (printPlan) {
            RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), ALL_ATTRIBUTES, false);
            relNode.explain(relWriter);
        }

        executeRelNode(relNode);
    }

    public void executeRelNode(RelNode relNode) throws SQLException {
        RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement run = runner.prepare(relNode);
        run.execute();
        ResultSet rs = run.getResultSet();

        int count = 0;
        while (rs.next()) {
            count++;
            if (printOutput) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    System.out.print(rs.getObject(i) + ", ");
                }
                System.out.println();
            }
        }

        System.out.println("Successfully executed query! Row count: " + count);
    }
}
