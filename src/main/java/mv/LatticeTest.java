package mv;

import common.Configuration;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.materialize.LatticeRootNode;
import org.apache.calcite.materialize.LatticeSuggester;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.statistic.MapSqlStatisticProvider;
import org.apache.calcite.tools.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.function.UnaryOperator;

public class LatticeTest {

    private SchemaPlus defaultSchema;

    public LatticeTest(SchemaPlus defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public static void main(String[] args) throws SQLException, ValidationException, SqlParseException, RelConversionException {
        Configuration configuration = Configuration.initialize();
        LatticeTest test = new LatticeTest(configuration.schema);
        test.test();
    }

    public void test() throws ValidationException, SqlParseException, RelConversionException {
        Tester tester = new Tester(defaultSchema).withEvolve(true);
        List<Lattice> lattices = new ArrayList<>();
        lattices = (tester.addQuery("SELECT * FROM \"region\" WHERE \"r_name\" LIKE 'A%'"));
        lattices = (tester.addQuery("SELECT count(*), sum(\"s_suppkey\") FROM \"supplier\" WHERE \"s_suppkey\" > 1000"));
        lattices = (tester.addQuery("SELECT \"s_suppkey\" FROM \"supplier\" WHERE \"s_suppkey\" > 100"));

        System.out.println(Arrays.toString(lattices.toArray()));
    }

    private static class Tester {
        final LatticeSuggester s;
        private final FrameworkConfig config;

        Tester(SchemaPlus schemaPlus) {
            this(Frameworks.newConfigBuilder()
                    .defaultSchema(schemaPlus)
                    .statisticProvider(MapSqlStatisticProvider.INSTANCE)
                    .build());
        }

        private Tester(FrameworkConfig config) {
            this.config = config;
            s = new LatticeSuggester(config);
        }

        Tester withConfig(FrameworkConfig config) {
            return new Tester(config);
        }

        private Frameworks.ConfigBuilder builder() {
            return Frameworks.newConfigBuilder(config);
        }

        List<Lattice> addQuery(String q) throws SqlParseException,
                ValidationException, RelConversionException {
            final Planner planner = new PlannerImpl(config);
            final SqlNode node = planner.parse(q);
            final SqlNode node2 = planner.validate(node);
            final RelRoot root = planner.rel(node2);
            return s.addQuery(root.project());
        }

        /**
         * Parses a query returns its graph.
         */
        LatticeRootNode node(String q) throws SqlParseException,
                ValidationException, RelConversionException {
            final List<Lattice> list = addQuery(q);
            if (list.size() > 1) {
                System.out.println("WARNING: Lattice list size is > 1");
            }
            return list.get(0).rootNode;
        }

        Tester withEvolve(boolean evolve) {
            return withConfig(builder().evolveLattice(evolve).build());
        }

        private Tester withParser(UnaryOperator<SqlParser.Config> transform) {
            return withConfig(
                    builder()
                            .parserConfig(transform.apply(config.getParserConfig()))
                            .build());
        }

        Tester withDialect(SqlDialect dialect) {
            return withParser(dialect::configureParser);
        }

        Tester withLibrary(SqlLibrary library) {
            SqlOperatorTable opTab = SqlLibraryOperatorTableFactory.INSTANCE
                    .getOperatorTable(EnumSet.of(SqlLibrary.STANDARD, library));
            return withConfig(builder().operatorTable(opTab).build());
        }
    }
}
