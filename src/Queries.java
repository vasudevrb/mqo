public class Queries {

    public static final String mv1 = "SELECT \"l_extendedprice\", \"l_shipdate\", \"l_discount\", \"l_quantity\"" +
            " FROM \"public\".\"lineitem\"" +
            " WHERE" +
            " \"l_shipdate\" >= date '1994-01-01'" +
            " AND \"l_shipdate\" < date '1995-01-01'" +
            " AND \"l_discount\" BETWEEN 0.05 AND 0.08" +
            " AND \"l_quantity\" < 24";

    public static final String q1 = "SELECT \"l_extendedprice\"" +
            " FROM \"public\".\"lineitem\"" +
            " WHERE" +
            " \"l_shipdate\" >= date '1994-01-01'" +
            " AND \"l_shipdate\" < date '1994-06-01'" +
            " AND \"l_discount\" between 0.06 AND 0.07" +
            " AND \"l_quantity\" < 14";

    public static final String mv0 = "SELECT \"r_name\" FROM \"public\".\"region\"";
    public static final String q0 = "SELECT \"r_name\" FROM \"public\".\"region\" WHERE \"r_name\" LIKE 'A%'";

    public static final String mv2 = "SELECT \"l_discount\", \"l_quantity\" FROM \"lineitem\" WHERE \"l_quantity\" < 24";
    public static final String q2 = "SELECT \"l_discount\" FROM \"public\".\"lineitem\" WHERE \"l_quantity\" < 4";

    public static final String mv3 = "SELECT \"s_suppkey\", \"s_name\" FROM \"supplier\" WHERE \"s_suppkey\" < 1000";
    public static final String q3 = "SELECT \"s_name\" FROM \"public\".\"supplier\" WHERE \"s_suppkey\" < 100";
}
