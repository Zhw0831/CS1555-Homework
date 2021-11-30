import java.util.Properties;
import java.util.*;
import java.sql.*;

public class FindTopK {
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

        Scanner inScan = new Scanner(System.in);
        int k = 0;

        while(k <= 0) {
            System.out.print("Please enter the number of top busy workers: ");
            k = inScan.nextInt();
        }


        String name;
        int rank, count = 0;

        String query = "    SELECT name, RANK() OVER(ORDER BY sensor_num_maintain DESC) as rank\n" +
                "    FROM\n" +
                "         (SELECT w.name, COUNT(sensor_id) AS sensor_num\n" +
                "          FROM WORKER w JOIN SENSOR s on w.ssn = s.maintainer\n" +
                "          WHERE s.energy <= 2\n" +
                "          GROUP BY w.name) sensor_num_maintain;";
        ResultSet res = st.executeQuery(query);

        if(!res.next())
            System.out.print("There is no busy workers.");
        else{
            System.out.format("%15s%15s%n", "worker name", "busy rank");
            do{
                name = res.getString("name");
                rank = res.getInt("rank");
                System.out.format("%15s%15s%n", name, rank);
                count++;

                if(count==k)
                    break;
            } while (res.next());
        }

    }
}