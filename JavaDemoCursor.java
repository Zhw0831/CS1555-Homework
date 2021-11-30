import java.sql.*;
import java.util.Properties;

public class JavaDemoCursor {
    public static void main(String args[]) throws
            SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://class3.cs.pitt.edu:5432/";
        Properties props = new Properties();
        props.setProperty("user", "<your_pitt_user>>");
        props.setProperty("password", "<your_pitt_pass>");

        Connection dbcon =
                DriverManager.getConnection(url, props);

        Statement st = dbcon.createStatement();
        ResultSet resultSet = st.executeQuery("SELECT * FROM STUDENT");
        int pos = resultSet.getRow();      // Get cursor position, pos = 0
        boolean b = resultSet.isBeforeFirst();    // true
        int rid;
        String rname, rmajor;
        resultSet.next();                  // Move cursor to the first row
        pos = resultSet.getRow();          // Get cursor position, pos = 1

        System.out.println(pos);
        rid = resultSet.getInt("SID");
        rname = resultSet.getString("Name");
        rmajor = resultSet.getString(3);
        System.out.println(rid + " " + rname + " " + rmajor);

        b = resultSet.isFirst();    // true
        resultSet.last();           // Move cursor to the last row
        pos = resultSet.getRow();   // If table has 10 rows, pos = 10

        System.out.println(pos);
        rid = resultSet.getInt("SID");
        rname = resultSet.getString("Name");
        rmajor = resultSet.getString(3);
        System.out.println(rid + " " + rname + " " + rmajor);

        b = resultSet.isLast();     // true
        resultSet.afterLast();      // Move cursor past last row
        pos = resultSet.getRow();   // If table has 10 rows,
        // value would be 11
        b = resultSet.isAfterLast();   // true

    }
}