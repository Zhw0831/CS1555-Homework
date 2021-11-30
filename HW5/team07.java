
package HW5;

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
        props.setProperty("password", "Sirshri1");
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
                    continue;
                case 7:
                    team07.findTopK(conn);
                    continue;
                case 8:
                    team07.mostActive(conn);
                    continue;
                case 9:
                    team07.addState(conn);
                    continue;
                case 10:
                    team07.showRegistry(conn);
                    continue;
            }
            System.out.println("-----------------------------------------");
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
        System.out.println("7: Find Top K Busy Workers");
        System.out.println("8: Find Most Active Sensors");
        System.out.println("9: Add State");
        System.out.println("10: View Registry");
        return inScan.nextInt();

    }

    private static void showRegistry(Connection conn) throws SQLException
    {
        System.out.println("Select the regsitry you want to view:");
        System.out.println("1: Forest");
        System.out.println("2: Worker");
        System.out.println("3: Sensor");
        System.out.println("4: State ");
        System.out.println("5: Coverage");
        System.out.println("6: Report");
        System.out.println("7: Emergency");

        switch (inScan.nextInt())
        {
            case 1:
                showTables.forest(conn);
        }

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
                System.out.print("Enter the name of the forest: ");
                forestName = inScan.nextLine();
                String compareName = String.format("SELECT name FROM FOREST WHERE name = %s", forestName);
                ResultSet res1 = st.executeQuery(compareName);

                if (!res1.next())
                    validName = true;
                else
                    System.out.println("This forest name already exists. Please try again.");
            }

            System.out.print("Enter the area of the forest: ");
            area = inScan.nextDouble();

            boolean validAcid = false;
            while (!validAcid) {
                System.out.print("Enter the acid level of the forest");
                acidLevel = inScan.nextDouble();

                if (acidLevel >= 0 && acidLevel <= 1)
                    validAcid = true;
                else
                    System.out.println("The acid level you entered is invalid. It must be between 0 and 1 inclusive. ");
            }

            boolean validBound = false;

            while (!validBound) {
                System.out.print("Enter the minimum x boundary: ");
                xmin = inScan.nextDouble();

                System.out.print("Enter the maximum x boundary: ");
                xmax = inScan.nextDouble();

                System.out.print("Enter the minimum y boundary: ");
                ymin = inScan.nextDouble();

                System.out.print("Enter the maximum y boundary: ");
                ymax = inScan.nextDouble();

                String compareBound = "SELECT mbr_xmin, mbr_xmax, mbr_ymin, mbr_ymax FROM FOREST WHERE mbr_xmin = '" +
                        xmin + "' AND mbr_xmax = '" + xmax + "' AND mbr_ymin = '" + ymin + "' AND mbr_ymax = '" + ymax + "'";
                ResultSet res2 = st.executeQuery(compareBound);

                if (!res2.next())
                    validBound = true;
                else
                    System.out.println("These x - y boundaries already exist. Please try again.");
            }

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
            while (res2.next()) {
                int confirmNum = res2.getInt("forest_no");
                System.out.println(String.format("Forest %s successfully inserted with the forest number %d!", forestName, confirmNum));
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

            String query2 = "SELECT abbreviation FROM STATE";
            ResultSet res2 = st.executeQuery(query2);
            ArrayList<String> states = new ArrayList<String>();
            while (res2.next()) {
                states.add(res2.getString(1));
            }
            boolean validState = false;
            while (!validState) {
                System.out.print("Please enter the worker's employing state: ");
                employingState = inScan.nextLine();

                if (employingState.length() == 2) {
                    validState = true;
                } else {
                    System.out.println("State abbreviation invalid. Please enter the state abbreviation (PA, OH).");
                }

                if (!states.contains(employingState)) {
                    System.out.println("This state does not exist. Please add it.");
                    addState(conn);
                }
            }

            conn.setAutoCommit(false);
            //add a method that checks if the state exists and if it does not
            //insert it into the table
            String insert = "INSERT INTO WORKER VALUES ('" + ssn + "', '" + name + "', " + rank + ", '" + employingState + "');";
            PreparedStatement insertWorker = conn.prepareStatement(insert);

            insertWorker.executeUpdate();
            conn.commit();

            PreparedStatement query3 = conn.prepareStatement("SELECT name FROM WORKER WHERE ssn = ?");
            query3.setString(1, ssn);
            ResultSet res3 = query3.executeQuery();
            while (res3.next()) {
                String confirmName = res3.getString("name");
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

        System.out.println("--------------------------------------");
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

        System.out.print("--------------------------------------");
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

        System.out.print("--------------------------------------");
    }

    private static void updateSensorStatus(Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        inScan.nextLine();
        double x = 0, y = 0, temperature = 0;

        int energy = 0, sensor_id = 0;

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

            System.out.print("Please enter the energy level: ");
            energy = inScan.nextInt();

            inScan.nextLine();

            System.out.println("Enter the time the sensor was last charged:");
            System.out.println("Enter the month");
            String lc_month = inScan.next();
            System.out.println("Enter the day");
            String lc_day = inScan.next();
            System.out.println("Enter the year");
            String lc_year = inScan.next();
            System.out.println("Enter the time (hh24:mi)");
            String lc_time = inScan.next();

            String last_charged = lc_month + "/" + lc_day + "/" + lc_year + " " + lc_time;

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();
            Timestamp timestamp = Timestamp.valueOf(now);

            System.out.print("Please enter the temperature: ");
            temperature = inScan.nextDouble();


            conn.setAutoCommit(false);
            PreparedStatement upDateSensor = conn.prepareStatement("UPDATE SENSOR SET energy = ? , last_charged = TO_TIMESTAMP(?, 'mm/dd/yyyy hh24:mi') WHERE x = ? AND y = ?");
            upDateSensor.setInt(1, energy);
            upDateSensor.setString(2, last_charged);
            upDateSensor.setDouble(3, x);
            upDateSensor.setDouble(4, y);

            upDateSensor.executeUpdate();
            conn.commit();

            conn.setAutoCommit(false);
            PreparedStatement insertReport = conn.prepareStatement("INSERT INTO REPORT VALUES (?, ?, ?)");
            insertReport.setInt(1, sensor_id);
            insertReport.setTimestamp(2, timestamp);
            insertReport.setDouble(3, temperature);

            insertReport.executeUpdate();
            conn.commit();

            PreparedStatement query2 = conn.prepareStatement("SELECT sensor_id FROM EMERGENCY WHERE sensor_id = ?");
            query2.setInt(1, sensor_id);
            ResultSet res2 = query2.executeQuery();
            if (res2.next()) {
                System.out.println("An emergency was reported after the sensor status was updated.");
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

        System.out.print("--------------------------------------");
    }

    private static void updateForestArea(Connection conn) throws SQLException {
        Statement st = conn.createStatement();

        inScan.nextLine();
        String forestName = "", state = "";
        double newArea = 0;
        int forestNum = 0;

        try {
            boolean validName = false;
            while (!validName) {
                System.out.print("Please enter the forest name: ");
                forestName = inScan.nextLine();
                //forestName = forestName.toUpperCase();

                String compareName = "SELECT name, forest_no FROM FOREST WHERE name = '" + forestName + "'";
                ResultSet res1 = st.executeQuery(compareName);

                if (res1.next()) {
                    validName = true;
                    forestNum = res1.getInt("forest_no");
                } else
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

                String compareState = "SELECT state FROM COVERAGE WHERE forest_no = '" + forestNum + "' AND state = '" + state + "'";
                ResultSet res2 = st.executeQuery(compareState);

                if (res2.next())
                    validState = true;
                else
                    System.out.print("The forest does not cover area in this state. ");
            }

            conn.setAutoCommit(false);
            PreparedStatement updateCoverage = conn.prepareStatement("UPDATE COVERAGE SET area = ? WHERE forest_no = ? AND state = ?");
            updateCoverage.setDouble(1, newArea);
            updateCoverage.setInt(2, forestNum);
            updateCoverage.setString(3, state);

            updateCoverage.executeUpdate();
            conn.commit();

            System.out.println("Your update is successful.");
        } catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The current update failed.");
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        } catch (Exception e) {
            System.out.println("Then input is invalid.");
            System.exit(0);
        }

        System.out.print("--------------------------------------");
    }

    private static void findTopK(Connection conn) throws SQLException {
        inScan.nextLine();

        String q = "SELECT count(*) FROM WORKER";
        Statement st = conn.createStatement();
        ResultSet res1 = st.executeQuery(q);
        int numWorkers = 0;
        while (res1.next()) {
            numWorkers = res1.getInt(1);
        }


        Boolean validateK = false;
        int k = 0;
        while (!validateK) {
            System.out.println("Enter a top k value: ");
            k = inScan.nextInt();
            if (k > numWorkers) {
                System.out.println("There are only " + numWorkers + " workers in the registry.");
            } else
                validateK = true;
        }

        String query = String.format("SELECT A.name, count(sensor_id) FROM (SELECT name, sensor_id FROM WORKER JOIN sensor ON worker.ssn = sensor.maintainer WHERE energy <= 2) AS A\n" +
                "GROUP BY A.name ORDER BY count(sensor_id) DESC FETCH FIRST %d ROWS ONLY;", k);
        res1 = st.executeQuery(query);
        String name;
        while (res1.next()) {
            name = res1.getString(1);
            System.out.println(name);
        }

        System.out.print("--------------------------------------");
    }

    private static void mostActive(Connection conn) throws SQLException {
        Statement st = conn.createStatement();

        String query = "SELECT sensor_id, RANK() OVER(ORDER BY COUNT(report_time)) AS rank FROM REPORT GROUP BY sensor_id";


        ResultSet res = st.executeQuery(query);

        int id, rank;
        System.out.format("%15s%15s%n", "sensor id", "active rank");
        while (res.next()) {
            id = res.getInt("sensor_id");
            rank = res.getInt(2);
            System.out.format("%15s%15s%n", id, rank);
        }

        System.out.print("--------------------------------------");
    }

    private static void addState(Connection conn) throws SQLException
    {
        String state = "", abbreviation = "";
        double area = 0;
        int population = 0;


        try {
            System.out.print("Enter the name of the state: ");
            state = inScan.next();

            boolean validState = false;
            while (!validState) {
                System.out.print("Enter the two letter abbreviation of the state: ");
                abbreviation = inScan.next();
                if (abbreviation.length() == 2) {
                    validState = true;
                } else {
                    System.out.println("State abbreviation invalid. Please enter the state abbreviation (PA, OH).");
                }

            }

            System.out.print("Enter the area of the state: ");
            area = inScan.nextDouble();
            System.out.print("Enter the population of the state: ");
            population = inScan.nextInt();

            conn.setAutoCommit(false);
            PreparedStatement query2 = conn.prepareStatement("INSERT INTO STATE (name, abbreviation, area, population) VALUES (?, ?, ?, ?);");
            query2.setString(1, state);
            query2.setString(2, abbreviation);
            query2.setDouble(3, area);
            query2.setInt(4, population);

            query2.executeUpdate();
            conn.commit();

            query2 = conn.prepareStatement("SELECT name FROM STATE WHERE abbreviation = ?");
            query2.setString(1, abbreviation);
            ResultSet res2 = query2.executeQuery();
            while (res2.next()) {
                String confirmName = res2.getString("name");
                System.out.println(String.format("%s successfully inserted into state with the abbreviation %s", state, abbreviation));
            }

        }catch(SQLException e1)
        {
            try {
            System.out.print(e1.toString());
            System.out.println("The current insertion failed.");
            conn.rollback();
            } catch (SQLException e2) {
            System.out.println(e2.toString());
            }
        } catch(Exception e)
        {
            System.out.println("Then input is invalid.");
            System.exit(0);
        }

        System.out.print("--------------------------------------");
    }
}

