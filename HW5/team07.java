//package HW5;

import java.sql.*;
import java.util.Properties;
import java.util.*;
import java.io.*;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

public class team07 {
    static Scanner inScan = new Scanner(System.in);

    public static void main(String args[]) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost:5432/"; //use for local
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "aaa496140768");
        Connection conn = DriverManager.getConnection(url, props);

        while (true) {
            switch (team07.menu()) {
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

    private static int menu() {
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

    private static void insertForest(Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        String forestName = "";
        double area, acidLevel = 0, xmin = 0, xmax = 0, ymin = 0, ymax = 0;
        boolean validName = false;
        inScan.nextLine();
        try {
            ///getting the input from the user and checking integrity statements
            while (!validName) {
                System.out.print("Please enter the forest name: ");
                forestName = inScan.nextLine();

                String compareName = "SELECT name FROM FOREST WHERE name = '" + forestName + "'";
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

            while (!validBound) {
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
                    "VALUES ('" + forestName + "'," + area + ", " + acidLevel + "," + xmin + "," + xmax + "," + ymin + "," + ymax + ");";
            PreparedStatement insertForest = conn.prepareStatement(insert);
            insertForest.executeUpdate();
            conn.commit();

            PreparedStatement query2 = conn.prepareStatement("SELECT forest_no FROM FOREST WHERE name = ?");
            query2.setString(1, forestName);
            ResultSet res2 = query2.executeQuery();
            while (res2.next()) {
                int confirmNum = res2.getInt("forest_no");
                System.out.printf("\nYour insertion is successful with forest number %d\n", confirmNum);
            }

        } catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The current insertion failed.");
                String query2 = "SELECT setval('FOREST_forest_no_seq', MAX(forest_no), true) FROM FOREST;";
                st.executeQuery(query2);
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        } catch (Exception e) {
            System.out.println(e.toString());
            System.out.println("Then input is invalid.");
            System.exit(0);
        }
    }

    private static void insertWorker(Connection conn) throws SQLException {
        int rank;
        String name = "", ssn = "", employingState = " ";
        Statement st = conn.createStatement();
        inScan.nextLine();
        try {
            boolean validSsn = false;
            while (!validSsn) {
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
            while (!validName) {
                System.out.print("Please enter the worker's name: ");
                name = inScan.nextLine();

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
            boolean validState = false;
            while (!validState) {
                System.out.print("Please enter the worker's employing state: ");
                employingState = inScan.nextLine();

                if (employingState.length() == 2) {
                    validState = true;
                } else {
                    System.out.println("State abbreviation invalid. Please enter the state abbreviation (PA, OH).");
                }
            }

            conn.setAutoCommit(false);
            //add a method that checks if the state exists and if it does not
            //insert it into the table
            String insert = "INSERT INTO WORKER VALUES ('" + ssn + "', '" + name + "', " + rank + ", '" + employingState + "');";
            PreparedStatement insertWorker = conn.prepareStatement(insert);

            insertWorker.executeUpdate();
            conn.commit();

            PreparedStatement query2 = conn.prepareStatement("SELECT name FROM WORKER WHERE ssn = ?");
            query2.setString(1, ssn);
            ResultSet res2 = query2.executeQuery();
            while (res2.next()) {
                String confirmName = res2.getString("name");
                System.out.printf("\nYour insertion is successful with worker's name %s\n", confirmName);
            }
        } catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The current insertion failed.");
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        } catch (Exception e) {
            System.out.println("Then input is invalid.");
            System.exit(0);
        }
    }

    private static void insertSensor(Connection conn) throws SQLException {
        double x = 0, y = 0;
        String maintainer = "";
        Statement st = conn.createStatement();
        inScan.nextLine();
        try {
            boolean validCord = false;
            while (!validCord) {
                System.out.print("Please enter the x coordinate of the sensor: ");
                x = inScan.nextDouble();
                System.out.print("Please enter the y coordinate of the sensor: ");
                y = inScan.nextDouble();

                String query1 = "SELECT x,y FROM SENSOR WHERE x = '" + x + "' AND y = '" + y + "'";
                ResultSet res1 = st.executeQuery(query1);

                if (!res1.next())
                    validCord = true;
                else
                    System.out.print("The x, y coordinates already exist. ");
            }
            inScan.nextLine();
            boolean validName = false;
            while (!validName) {
                System.out.print("Please enter the maintainer's Social Security Number: ");
                maintainer = inScan.nextLine();

                String query3 = "SELECT ssn FROM WORKER WHERE ssn = '" + maintainer + "'";
                ResultSet res3 = st.executeQuery(query3);

                if (res3.next())
                    validName = true;
                else
                    System.out.print("The ssn is not in the worker's table. ");
            }

            System.out.println("Enter the time the sensor was last charged:");
            System.out.println("Enter the month");
            String lc_month = inScan.next();
            System.out.println("Enter the day");
            String lc_day = inScan.next();
            System.out.println("Enter the year");
            String lc_year = inScan.next();
            System.out.println("Enter the time (hh24:mi)");
            String lc_time = inScan.next();

            System.out.println("Enter the time the sensor was last read:");
            System.out.println("Enter the month");
            String lr_month = inScan.next();
            System.out.println("Enter the day");
            String lr_day = inScan.next();
            System.out.println("Enter the year");
            String lr_year = inScan.next();
            System.out.println("Enter the time (hh24:mi)");
            String lr_time = inScan.next();

            System.out.println("Enter the energy level of the sensor:");
            int energy_level = inScan.nextInt();

            String last_charged = lc_month + "/" + lc_day + "/" + lc_year + " " + lc_time;
            String last_read = lr_month + "/" + lr_day + "/" + lr_year + " " + lr_time;

            String insert = String.format("INSERT INTO SENSOR(x,y,last_charged,maintainer, last_read,energy) VALUES (%f,%f,TO_TIMESTAMP('%s', 'mm/dd/yyyy hh24:mi'),\'" +
                    "%s',TO_TIMESTAMP('%s', 'mm/dd/yyyy hh24:mi'),%d);", x, y, last_charged, maintainer, last_read, energy_level);
            conn.setAutoCommit(false);
            PreparedStatement insertWorker = conn.prepareStatement(insert);
            conn.commit();

            PreparedStatement query2 = conn.prepareStatement("SELECT sensor_id FROM SENSOR WHERE x = ? AND y = ?");
            query2.setDouble(1, x);
            query2.setDouble(2, y);
            ResultSet res2 = query2.executeQuery();
            while (res2.next()) {
                int confirmNum = res2.getInt("sensor_id");
                System.out.printf("\nYour insertion is successful with sensor id %d\n", confirmNum);
            }
        } catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The current insertion failed.");
                String query2 = "SELECT setval('SENSOR_sensor_id_seq', MAX(sensor_id), true) FROM SENSOR;";
                st.executeQuery(query2);
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        } catch (Exception e) {
            System.out.println(e.toString());
            System.out.println("Then input is invalid.");
            System.exit(0);
        }
    }

    private static void switchWorkers(Connection conn) throws SQLException {
        String nameA = "", nameB = "";
        String ssnA = "", ssnB = "";
        Statement st = conn.createStatement();
        inScan.nextLine();
        boolean validNameA = false;
        while (!validNameA) {
            System.out.print("Enter the name of worker A: ");
            nameA = inScan.nextLine();

            String query1 = "SELECT ssn, name FROM WORKER WHERE name = '" + nameA + "'";
            ResultSet res1 = st.executeQuery(query1);

            if (res1.next()) {
                validNameA = true;
                ssnA = res1.getString(1);
            } else
                System.out.print("Worker A's name is not in the worker table. ");
        }
        boolean validNameB = false;
        while (!validNameB) {
            System.out.print("Enter the name of worker B: ");
            nameB = inScan.nextLine();

            String query2 = "SELECT ssn,name FROM WORKER WHERE name = '" + nameB + "'";
            ResultSet res2 = st.executeQuery(query2);

            if (res2.next()) {
                validNameB = true;
                ssnB = res2.getString(1);
            } else
                System.out.print("Worker B's name is not in the worker table. ");
        }

        Boolean rReturn;
        CallableStatement properCase = conn.prepareCall("{ ? = call checkState( ?, ? ) }");
        properCase.registerOutParameter(1, Types.BIT);
        properCase.setString(2, nameA);
        properCase.setString(3, nameB);
        properCase.execute();
        rReturn = properCase.getBoolean(1);
        properCase.close();
        if (!rReturn) {
            System.out.println("The workers are not eligible to work in the others state.");
            return;
        }

        try {
            conn.setAutoCommit(false);
            String insertHolder = "INSERT INTO WORKER VALUES ('xxxxxxxxx','x',0,'PA');";
            st.executeUpdate(insertHolder);
            conn.commit();

            PreparedStatement update1 = conn.prepareStatement("UPDATE SENSOR SET maintainer = 'xxxxxxxxx' WHERE maintainer = ? ;");
            update1.setString(1, ssnA);
            update1.executeUpdate();
            conn.commit();

            PreparedStatement update2 = conn.prepareStatement("UPDATE SENSOR SET maintainer = ? WHERE maintainer = ? ;");
            update2.setString(1, ssnA);
            update2.setString(2, ssnB);
            update2.executeUpdate();
            conn.commit();

            PreparedStatement update3 = conn.prepareStatement("UPDATE SENSOR SET maintainer = ? WHERE maintainer = 'xxxxxxxxx' ;");
            update3.setString(1, ssnB);
            update3.executeUpdate();
            conn.commit();

            String deleteHolder = "DELETE FROM WORKER WHERE ssn='xxxxxxxxx';";
            st.executeUpdate(deleteHolder);
            conn.commit();

            System.out.println("Your switch is successful.");
        } catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The switch failed.");
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        } catch (Exception e) {
            System.out.println("Then input is invalid.");
            System.exit(0);
        }
    }

    private static void updateSensorStatus(Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        Scanner inScan = new Scanner(System.in);
        double x = 0, y = 0, temperature = 0;

        int energy_level = 0, sensor_id = 0;

        try {
            boolean validCord = false;
            while (!validCord) {
                System.out.print("Please enter the x coordinate of the sensor: ");
                x = inScan.nextDouble();
                System.out.print("Please enter the y coordinate of the sensor: ");
                y = inScan.nextDouble();

                String query1 = "SELECT x,y,sensor_id FROM SENSOR WHERE x = '" + x + "' AND y = '" + y + "'";
                ResultSet res1 = st.executeQuery(query1);

                if (res1.next()) {
                    validCord = true;
                    sensor_id = res1.getInt("sensor_id");
                } else
                    System.out.print("The sensor is not in the records. ");
            }

            inScan.nextLine();

            System.out.println("Enter the energy level of the sensor:");
            energy_level = inScan.nextInt();

            System.out.println("Enter the time the sensor was last charged:");
            System.out.println("Enter the month");
            String lc_month = inScan.next();
            System.out.println("Enter the day");
            String lc_day = inScan.next();
            System.out.println("Enter the year");
            String lc_year = inScan.next();
            System.out.println("Enter the time (hh24:mi)");
            String lc_time = inScan.next();

            String last_read = lc_month + "/" + lc_day + "/" + lc_year + " " + lc_time;

            //update the sensor table to change the last charged and energy.
            //insert the tuple
            String insert = String.format("UPDATE SENSOR S SET S.last_read = TO_TIMESTAMP('%s', 'mm/dd/yyyy hh24:mi')" +
                    "WHERE S.x = %f AND S.y = %f;", last_read, x, y);
            String query = String.format("UPDATE SENSOR S SET S.energy= %d" +
                    "WHERE S.x = %f AND S.y = %f;", energy_level, x, y);

            try {
                conn.setAutoCommit(false);
                st.executeUpdate(insert);
                st.executeUpdate(query);
                conn.commit();
            } catch (SQLException e1) {
                try {
                    conn.rollback();
                } catch (SQLException e2) {
                    System.out.println(e2.toString());
                }
            }
            System.out.println("Enter the recorded temperature:");
            temperature = inScan.nextInt();

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();
            Timestamp timestamp = Timestamp.valueOf(now);

            conn.setAutoCommit(false);
            String insertReport = String.format("INSERT INTO REPORT VALUES (%d, %s, %f)", sensor_id, timestamp, temperature);
            PreparedStatement report = conn.prepareStatement(insertReport);

            report.executeUpdate();
            conn.commit();

            PreparedStatement query2 = conn.prepareStatement("SELECT sensor_id FROM EMERGENCY WHERE sensor_id = ?");
            query2.setInt(1, sensor_id);
            ResultSet res2 = query2.executeQuery();
            if (res2.next()) {
                System.out.print("An emergency was reported after the sensor status was updated.");
            }

        } catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The current insertion failed.");
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        } catch (Exception e) {
            System.out.println(e.toString());
            System.out.println("Then input is invalid.");
            System.exit(0);
        }
    }

    private static void updateForestArea(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();

        Scanner inScan = new Scanner(System.in);
        String forestName = "", inputState = "";
        double newArea = 0;
        int forestNum = 0;

        try{
            boolean validName = false;
            while (!validName) {
                System.out.print("Please enter the forest name: ");
                forestName = inScan.nextLine();

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
                inputState = inScan.nextLine();
                inputState = inputState.toUpperCase();

                String compareState = "SELECT state FROM COVERAGE WHERE forest_no = '"+forestNum+"' AND state = '"+inputState+"'";
                ResultSet res2 = st.executeQuery(compareState);

                if (res2.next())
                    validState = true;
                else
                    System.out.print("The forest does not cover area in this state. ");
            }

            inScan.close();

            conn.setAutoCommit(false);
            String update = String.format("UPDATE COVERAGE SET area = %f WHERE forest_no = %d AND state = %s", newArea, forestNum, inputState);
            PreparedStatement updateCoverage = conn.prepareStatement(update);

            updateCoverage.executeUpdate();
            conn.commit();

            System.out.print("Your update is successful.");
        }catch (SQLException e1) {
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
