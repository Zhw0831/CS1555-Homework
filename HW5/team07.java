package HW5;

import java.sql.*;
import java.util.Properties;
import java.util.*;
import java.io.*;

public class team07 {
    static Scanner inScan = new Scanner(System.in);

    public static void main(String args[]) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost:5432/"; //use for local
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "Sirshri1");
        Connection conn = DriverManager.getConnection(url, props);

        while(true)
        {
            switch (team07.menu())
            {
                case 1:
                    team07.insertForest(conn);
                    continue;
                case 2:
                    team07.insertWorker(conn);
                    continue;
                case 3:
                    team07.insertSensor(conn);
                    continue;
                case 4:
                    team07.switchWorkers(conn);
                    continue;
                case 5:
                    team07.updateSensorStatus(conn);
                    continue;
                case 6:
                    team07.updateForestArea(conn);
            }
        }
    }

    private static int menu()
    {
        System.out.println("Welcome to the US Forest Registry. " +
                "In order to make a selection, enter the number corresponding to the menu option.");
        System.out.println("1: Add Forest");
        System.out.println("2: Add Worker");
        System.out.println("3: Add Sensor");
        System.out.println("4: Switch Workers Duties");
        System.out.println("5: Update Sensor Status");
        System.out.println("6: Update Forest Area Covered");


        return inScan.nextInt();
    }

    private static void insertForest(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();
        String forestName = "";
        double area, acidLevel = 0, xmin = 0, xmax = 0, ymin = 0, ymax = 0;
        boolean validName = false;

        try{
            ///getting the input from the user and checking integrity statements
            while (!validName) {
                System.out.print("Please enter the forest name: ");
                forestName = inScan.nextLine();
                forestName = forestName.toUpperCase();

                String compareName = "SELECT name FROM FOREST WHERE name = '"+forestName+"'";
                ResultSet res1 = st.executeQuery(compareName);

                if (!res1.next())
                    validName = true;
                else
                    System.out.print("The forest name already exists. ");
            }

            System.out.print("Please enter the forest area: ");
            area = inScan.nextDouble();

            boolean validAcid = false;
            while (!validAcid) {
                System.out.print("Please enter the forest's acid level: ");
                acidLevel = inScan.nextDouble();

                if (acidLevel >= 0 && acidLevel <= 1)
                    validAcid = true;
                else
                    System.out.print("Acid level must between 0 and 1. ");
            }

            boolean validBound = false;

            while(!validBound) {
                System.out.print("Please enter the min x boundary: ");
                xmin = inScan.nextDouble();

                System.out.print("Please enter the max x boundary: ");
                xmax = inScan.nextDouble();

                System.out.print("Please enter the min y boundary: ");
                ymin = inScan.nextDouble();

                System.out.print("Please enter the max y boundary: ");
                ymax = inScan.nextDouble();

                String compareBound = "SELECT mbr_xmin, mbr_xmax, mbr_ymin, mbr_ymax FROM FOREST WHERE mbr_xmin = '" +
                        xmin + "' AND mbr_xmax = '" + xmax + "' AND mbr_ymin = '" + ymin + "' AND mbr_ymax = '" + ymax + "'";
                ResultSet res2 = st.executeQuery(compareBound);

                if (!res2.next())
                    validBound = true;
                else
                    System.out.print("The boundaries already exist. ");
            }

            conn.setAutoCommit(false);
            String insert = "INSERT INTO FOREST (name,area,acid_level,mbr_xmin,mbr_xmax,mbr_ymin,mbr_ymax) " +
                    "VALUES ('"+forestName+"',"+area+", "+acidLevel+","+xmin+","+xmax+","+ymin+","+ymax+");";
            PreparedStatement insertForest = conn.prepareStatement(insert);
            insertForest.executeUpdate();
            conn.commit();

            PreparedStatement query2 = conn.prepareStatement("SELECT forest_no FROM FOREST WHERE name = ?");
            query2.setString(1, forestName);
            ResultSet res2 = query2.executeQuery();
            while(res2.next()) {
                int confirmNum = res2.getInt("forest_no");
                System.out.printf("\nYour insertion is successful with forest number %d\n", confirmNum);
            }

            }catch (SQLException e1) 
            {
                try{
                    System.out.print(e1.toString());
                    System.out.println("The current insertion failed.");
                    String query2 = "SELECT setval('FOREST_forest_no_seq', MAX(forest_no), true) FROM FOREST;";
                    st.executeQuery(query2);
                    conn.rollback();
                } catch (SQLException e2) 
                {
                    System.out.println(e2.toString());
                }
            }catch (Exception e)
            {
                System.out.println(e.toString());
                System.out.println("Then input is invalid.");
                System.exit(0);
            }   
    }   

    private static void insertWorker(Connection conn) throws SQLException
    {
        int rank;
        String name = "", ssn = "", employingState;
        Statement st = conn.createStatement();

        try {  
            boolean validSsn = false;
            while (!validSsn) 
            {
                System.out.print("Enter the Social Security Number (SSN) of the worker:");
                ssn = inScan.nextLine();

                String query1 = "SELECT ssn FROM WORKER WHERE ssn = '" + ssn + "'";
                ResultSet res1 = st.executeQuery(query1);

                if (!res1.next())
                    validSsn = true;
                else
                System.out.print("The ssn already exists. ");
            }

            boolean validName = false;
            while (!validName) 
            {
                System.out.print("Please enter the worker's name: ");
                name = inScan.nextLine();
                name = name.toUpperCase();

                String query2 = "SELECT name FROM WORKER WHERE name = '" + name + "'";
                ResultSet res2 = st.executeQuery(query2);

                if (!res2.next())
                    validName = true;
                else
                    System.out.print("The name already exists. ");
            }
            System.out.print("Please enter the worker's rank: ");
            rank = inScan.nextInt();

            inScan.nextLine();

            System.out.print("Please enter the worker's employing state: ");
            employingState = inScan.nextLine();

            //    conn.setAutoCommit(true);
            //add a method that checks if the state exists and if it does not
            //insert it into the table
            String insert = "INSERT INTO WORKER VALUES ('" + ssn + "', '" + name + "', " + rank + ", '" + employingState + "');";
            PreparedStatement insertWorker = conn.prepareStatement(insert);

            insertWorker.executeUpdate();
            conn.commit();

            PreparedStatement query2 = conn.prepareStatement("SELECT name FROM WORKER WHERE ssn = ?");
            query2.setString(1, ssn);
            ResultSet res2 = query2.executeQuery();
            while (res2.next()) 
            {
                String confirmName = res2.getString("name");
                System.out.printf("\nYour insertion is successful with worker's name %s\n", confirmName);
            }
        }catch (SQLException e1) 
        {
            try {
                System.out.print(e1.toString());
                System.out.println("The current insertion failed.");
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        }catch (Exception e)
        {
            System.out.println("Then input is invalid.");
            System.exit(0);
        }
    }

}
