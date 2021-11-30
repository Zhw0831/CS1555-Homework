

import java.sql.*;
import java.util.Properties;

public class JavaDemoTransaction {
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
            conn.setAutoCommit(false);
            st.executeUpdate("INSERT INTO FOREST VALUES ('2','Pennsylvania Forest',2700,0.74,40,70,20,110);");
            st.executeUpdate("INSERT INTO FOREST VALUES ('3','Stone Valley',5000,0.56,60,160,30,80);");
            conn.commit();
        } catch (SQLException e1) {
            try {
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        }


    }
}