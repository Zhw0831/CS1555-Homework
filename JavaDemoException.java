

import java.sql.*;
import java.util.Properties;

public class JavaDemoException {
    public static void main(String args[]) throws
            SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost:5432/"; //use for local
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "Sirshri1");

        Connection conn =
                DriverManager.getConnection(url, props);

        Statement st = conn.createStatement();
        try {
            ResultSet res3 = st.executeQuery("SELECT * FROM NOTATABLE");
        }
        catch (SQLException e1) {
            System.out.println("SQL Error");
            while (e1 != null) {
                System.out.println("Message = " + e1.getMessage());
                System.out.println("SQLState = "+ e1.getSQLState());
                System.out.println("SQL Code = "+ e1.getErrorCode());
                e1 = e1.getNextException();
            }
        }

    }
}