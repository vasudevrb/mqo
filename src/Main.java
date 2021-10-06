import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.sql.SQLException;

public class Main {

    public static void main(String[] args) {
        try {
            new CalciteHandler().parseAndValidateSql();
        } catch (SQLException | SqlParseException | ValidationException | RelConversionException e) {
            e.printStackTrace();
        }
    }
}
