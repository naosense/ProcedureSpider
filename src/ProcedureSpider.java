import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ProcedureSpider {
    public static void main(String[] args) {
        System.out.println("//////////////////////////////////////////\r\n" +
                        "               ¨†¨„¨‡¡¡¡¡¨…\r\n" +
                        "               ¨…¨…¨‡¡¡¨‡¨†\r\n" +
                        "               ¡¡¨†¨ƒ¨€¨ƒ¨†\r\n" +
                        "               ¡¡¨‡¨„¨‚¨„¨‡\r\n" +
                        "¡¡               ¡¡¨…¨†¨†\r\n" +
                        "¡¡               ¡¡¨‡¨‡¨‡\r\n" +
                        "//////////////////////////////////////////");
        Connection conn = DBUtil.getConnection();
        scan(conn);
        DBUtil.closeIfOpen(conn);
    }

    private static void scan(Connection conn) {
        PreparedStatement statement = null;
        ResultSet result = null;

        String sql =
                "SELECT TABLE_NAME\n" +
                "  FROM (SELECT SUBSTR(UNIT_NAME, 3, LENGTH(UNIT_NAME) - 2) TABLE_NAME\n" +
                "          FROM J1_DW.ETL_META_UNIT\n" +
                "        UNION\n" +
                "        SELECT SUBSTR(UNIT_NAME, 3, LENGTH(UNIT_NAME) - 2) TABLE_NAME\n" +
                "          FROM J1_LDCX.ETL_META_UNIT\n" +
                "        UNION\n" +
                "        SELECT SUBSTR(UNIT_NAME, 3, LENGTH(UNIT_NAME) - 2) TABLE_NAME\n" +
                "          FROM J1_CXTJ.ETL_META_UNIT)\n" +
                "  ORDER BY TABLE_NAME";

        try {
            statement = conn.prepareStatement(sql);
            result = statement.executeQuery();

            while (result.next()) {
                String t = result.getString("TABLE_NAME");
                ProcedureManager pm = new ProcedureManager(conn);
                pm.print(t);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBUtil.closeIfOpen(result);
            DBUtil.closeIfOpen(statement);
        }
    }
}
