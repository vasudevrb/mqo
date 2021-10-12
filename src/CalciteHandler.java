import com.google.common.collect.ImmutableList;
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

import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CalciteHandler {

    public void parseAndValidateSql(String sql, SchemaPlus rootSchema, CalciteConnection connection) throws SQLException, SqlParseException, ValidationException, RelConversionException {

        //TODO See parserconfig to set lowercase
        // https://datacadamia.com/db/calcite/getting_started
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

//        String sql = "SELECT sum(\"l_extendedprice\" * \"l_discount\") as revenue" +
//                " FROM \"public\".\"lineitem\"" +
//                " WHERE" +
//                " \"l_shipdate\" >= date '1994-01-01'" +
//                " AND \"l_shipdate\" < date '1994-01-01' + interval '1' year" +
//                " AND \"l_discount\" between 0.06 - 0.01 AND 0.06 + 0.01" +
//                " AND \"l_quantity\" < 24";

//        String sql = "SELECT \"l_extendedprice\", \"l_discount\"" +
//                " FROM \"public\".\"lineitem\"" +
//                " WHERE" +
////                " \"l_shipdate\" >= date '1994-01-01'" +
////                " AND \"l_shipdate\" < date '1995-01-01'" +
//                " \"l_discount\" between 0.06 - 0.01 AND 0.06 + 0.01" +
//                " AND \"l_quantity\" < 24";


        Planner planner = Frameworks.getPlanner(config);
        SqlNode node = planner.parse(sql);
        SqlNode validated = planner.validate(node);

        RelRoot relRoot = planner.rel(validated);
        RelNode relNode = relRoot.project();
        final RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES, false);
        relNode.explain(relWriter);


        RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement run = runner.prepare(relNode);
        run.execute();
        ResultSet rs = run.getResultSet();

        System.out.println("Result is");
        int num = 0;
        while (rs.next()) {
            num += 1;
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
//                System.out.print(rs.getObject(i) + ", ");
            }
//            System.out.println();
        }

        System.out.println("Size : " + num);
        SqlWriter writer = new SqlPrettyWriter();
        validated.unparse(writer, 0, 0);

//        System.out.println(ImmutableList.of(writer.toSqlString().getSql()));
    }
}
