import java.util.Properties;
import java.util.*;
import java.sql.*;

public class AddForest {
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

        // Task#1: Add forest
        //Ask the user to supply all the necessary fields for the new forest: forest name, area, 
        //acid level, and the minimum boundary rectangle coordinates (xmin, xmax, ymin, ymax).

        Scanner kbd = new Scanner(System.in);
        String forestName = "";
        double area, acidLevel = 0, xmin = 0, xmax = 0, ymin = 0, ymax = 0;
 
        try {
            boolean validName = false;
            while (!validName) {
                System.out.print("Please enter the forest name: ");
                forestName = kbd.nextLine();
                forestName = forestName.toUpperCase();

                String compareName = "SELECT name FROM FOREST WHERE name = '"+forestName+"'";
                ResultSet res1 = st.executeQuery(compareName);

                if (!res1.next())
                    validName = true;
                else
                    System.out.print("The forest name already exists. ");
                }

            System.out.print("Please enter the forest area: ");
            area = kbd.nextDouble();

            boolean validAcid = false;
            while (!validAcid) {
                System.out.print("Please enter the forest's acid level: ");
                acidLevel = kbd.nextDouble();

                if (acidLevel >= 0 && acidLevel <= 1)
                    validAcid = true;
                else
                    System.out.print("Acid level must between 0 and 1. ");
            }

            boolean validBound = false;

            while(!validBound) {
                System.out.print("Please enter the min x boundary: ");
                xmin = kbd.nextDouble();

                System.out.print("Please enter the max x boundary: ");
                xmax = kbd.nextDouble();

                System.out.print("Please enter the min y boundary: ");
                ymin = kbd.nextDouble();

                System.out.print("Please enter the max y boundary: ");
                ymax = kbd.nextDouble();

                String compareBound = "SELECT mbr_xmin, mbr_xmax, mbr_ymin, mbr_ymax FROM FOREST WHERE mbr_xmin = '" + xmin + "' AND mbr_xmax = '" + xmax + "' AND mbr_ymin = '" + ymin + "' AND mbr_ymax = '" + ymax + "'";
                ResultSet res2 = st.executeQuery(compareBound);

                if (!res2.next())
                    validBound = true;
                else
                    System.out.print("The boundaries already exist. ");
            }

            kbd.close();

            conn.setAutoCommit(false);
            PreparedStatement insertForest = conn.prepareStatement("INSERT INTO FOREST (forest_no, name, area, acid_level, mbr_xmin, mbr_xmax, mbr_ymin, mbr_ymax) VALUES (DEFAULT, ?, ?, ?, ?, ?, ?, ?);");
            insertForest.setString(1, forestName);
            insertForest.setDouble(2, area);
            insertForest.setDouble(3, acidLevel);
            insertForest.setDouble(4, xmin);
            insertForest.setDouble(5, xmax);
            insertForest.setDouble(6, ymin);
            insertForest.setDouble(7, ymax);

            insertForest.executeUpdate();
            conn.commit();

            PreparedStatement query2 = conn.prepareStatement("SELECT forest_no FROM FOREST WHERE name = ?");
            query2.setString(1, forestName);
            ResultSet res2 = query2.executeQuery();
            while(res2.next()) {
                int confirmNum = res2.getInt("forest_no");
                System.out.printf("\nYour insertion is successful with forest number %d\n", confirmNum);
            }
        }
        catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The current insertion failed.");
                String query2 = "SELECT setval('FOREST_forest_no_seq', MAX(forest_no), true) FROM FOREST;";
                st.executeQuery(query2);
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        }
        catch (Exception e){
            System.out.println(e.toString());
            System.out.println("Then input is invalid.");
            System.exit(0);
        }

    }
}