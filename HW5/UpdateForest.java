import java.util.Properties;
import java.util.*;
import java.sql.*;

public class UpdateForest {
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
        String forestName = "", state = "";
        double newArea = 0;
        int forestNum = 0;

        try{
            boolean validName = false;
            while (!validName) {
                System.out.print("Please enter the forest name: ");
                forestName = inScan.nextLine();
                //forestName = forestName.toUpperCase();

                String compareName = "SELECT name, forest_no FROM FOREST WHERE name = '"+forestName+"'";
                ResultSet res1 = st.executeQuery(compareName);

                if (res1.next()) {
                    validName = true;
                    forestNum = res1.getInt("forest_no");
                }
                else
                    System.out.print("The forest name doesn't exist. ");
            }

            System.out.print("Please enter the new area to update: ");
            newArea = inScan.nextDouble();

            inScan.nextLine();

            boolean validState = false;
            while (!validState) {
                System.out.print("Please enter the state abbreviation: ");
                state = inScan.nextLine();
                state = state.toUpperCase();

                String compareState = "SELECT state FROM COVERAGE WHERE forest_no = '"+forestNum+"' AND state = '"+state+"'";
                ResultSet res2 = st.executeQuery(compareState);

                if (res2.next())
                    validState = true;
                else
                    System.out.print("The forest does not cover area in this state. ");
            }

            inScan.close();

            conn.setAutoCommit(false);
            PreparedStatement updateCoverage = conn.prepareStatement("UPDATE COVERAGE SET area = ? WHERE forest_no = ? AND state = ?");
            updateCoverage.setDouble(1, newArea);
            updateCoverage.setInt(2, forestNum);
            updateCoverage.setString(3, state);

            updateCoverage.executeUpdate();
            conn.commit();

            System.out.print("Your update is successful.");
        }
        catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The current update failed.");
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        }
        catch (Exception e){
            System.out.println("Then input is invalid.");
            System.exit(0);
        }
    }
}
