import mv.MVHandler;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.tools.ValidationException;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Collections;

public class Main {

    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        DataSource dataSource = JdbcSchema.dataSource("jdbc:postgresql:tpch_test", "org.postgresql.Driver", "postgres", "vasu");
        rootSchema.add("public", JdbcSchema.create(rootSchema, "public", dataSource, null, null));

//        try {
//            new CalciteHandler().parseAndValidateSql(rootSchema, calciteConnection);
//        } catch (SqlParseException | ValidationException | RelConversionException e) {
//            e.printStackTrace();
//        }

        String tpch6_orig = "SELECT \"l_extendedprice\", \"l_shipdate\", \"l_discount\", \"l_quantity\"" +
                " FROM \"public\".\"lineitem\"" +
                " WHERE" +
                " \"l_shipdate\" >= date '1994-01-01'" +
                " AND \"l_shipdate\" < date '1995-01-01'" +
                " AND \"l_discount\" BETWEEN 0.05 AND 0.08" +
                " AND \"l_quantity\" < 24";

        String tpch6_mod = "SELECT \"l_extendedprice\"" +
                " FROM \"public\".\"lineitem\"" +
                " WHERE" +
                " \"l_shipdate\" >= date '1994-01-01'" +
                " AND \"l_shipdate\" < date '1994-06-01'" +
                " AND \"l_discount\" between 0.06 AND 0.07" +
                " AND \"l_quantity\" < 14";

        String mv = "SELECT \"r_name\" FROM \"public\".\"region\"";
        String q = "SELECT \"r_name\" FROM \"public\".\"region\" WHERE \"r_name\" LIKE 'A%'";
        String qmv = "SELECT * FROM \"public\".\"MV0\" WHERE 1 = 0";

        String mv2 = "SELECT \"l_discount\", \"l_quantity\" FROM \"public\".\"lineitem\" WHERE \"l_quantity\" < 24";
        String q2 = "SELECT \"l_discount\" FROM \"public\".\"lineitem\" WHERE \"l_quantity\" < 4";
        String q2_tt = "SELECT \"l_discount\" FROM \"public\".\"MV2\" WHERE \"l_quantity\" < 4";

        String mv3 = "SELECT \"s_suppkey\", \"s_name\" FROM \"supplier\" WHERE \"s_suppkey\" < 1000";
        String q3 = "SELECT \"s_suppkey\", \"s_name\" FROM \"supplier\" WHERE \"s_suppkey\" < 100";
        String q3_tt = "SELECT \"s_suppkey\", \"s_name\" FROM \"public\".\"MV3\" WHERE \"s_suppkey\" < 100";


        System.out.println("ORIGINAL QUERY ----->");
        try {
            new CalciteHandler().parseAndValidateSql(q, rootSchema, calciteConnection);
        } catch (SqlParseException | ValidationException | RelConversionException e) {
            e.printStackTrace();
        }
        System.out.println("<-------------");





        System.out.println("DERIVED PLAN ----->");
//        System.out.println("Is derivable? " + new MVHandler(calciteConnection).isDerivable("MV2", mv, q));
        RelNode derivedPlan = new MVHandler(calciteConnection).getDerivedPlan("MV0", mv, q);

        final RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES, false);
        derivedPlan.explain(relWriter);
//
        RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement run = runner.prepare(derivedPlan);
        run.execute();
        ResultSet rs = run.getResultSet();

        System.out.println("Result is");
        int num = 0;
        while (rs.next()) {
            num += 1;
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                System.out.print(rs.getObject(i) + ", ");
            }
            System.out.println();
        }
        System.out.println("Num rows: " + num);
        System.out.println("<-------------");

//        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
//
//        CalciteSchema cs = CalciteSchema.createRootSchema(false, false);
//        cs.add(defSchema.getName(), defSchema);
//
//        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(cs,
//                Collections.singletonList("public"), typeFactory, calciteConnection.config());
//
//        SqlValidator validator = SqlValidatorUtil.newValidator();
//        validator.
    }
}
