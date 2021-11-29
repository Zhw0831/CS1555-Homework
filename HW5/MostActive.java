import java.util.Properties;
import java.util.*;
import java.sql.*;

public class MostActive {
    public static void main(String args[]) throws
            SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost:5432/";
        //String url = "jdbc:postgresql://class3.cs.pitt.edu:5432/";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "aaa496140768");
        Connection conn =
                DriverManager.getConnection(url, props);

        Statement st = conn.createStatement();

        String query = "SELECT sensor_id, RANK() OVER(ORDER BY COUNT(report_time)) AS rank FROM REPORT GROUP BY sensor_id";
        ResultSet res = st.executeQuery(query);

        int id, rank;
        System.out.format("%15s%15s%n", "sensor id", "active rank");
        while(res.next()){
            id = res.getInt("sensor_id");
            rank = res.getInt("rank");
            System.out.format("%15s%15s%n", id, rank);
        }
    }
}