package HW5;

import java.sql.*;
import java.util.Properties;
import java.util.*;
import java.io.*;

public class queries {
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
            switch (queries.menu())
            {
                case 1:
                    queries.insertForest(conn);
                    continue;
                case 2:
                    queries.insertWorker(conn);
                    continue;
                case 3:
                    queries.insertSensor(conn);
                    continue;
                case 4:
                    queries.switchWorkers(conn);
                    continue;
                case 5:
                    queries.updateSensorStatus(conn);
                    continue;
                case 6:
                    queries.updateForestArea(conn);
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
        inScan.nextLine();
        System.out.println("Enter the name of the forest:");
        String name = inScan.nextLine();

        ///check if the name exists already
        String check = "SELECT name FROM FOREST WHERE name = '"+name+"'";
        Statement st2 = conn.createStatement();
        ResultSet res1 = st2.executeQuery(check);
        String fname = "";
        if (res1.next()) {
            fname = res1.getString("name");
            System.out.println(fname + ": This forest already exists, please try again.");
            System.out.println("------------------------------------");
            return;
        }

        System.out.println("Enter the area of the forest:");
        int area = inScan.nextInt();
        System.out.println("Enter the acid level of the forest:");
        double acidlvl = inScan.nextDouble();
        System.out.println("Enter the xmin boundary requirement:");
        double xmin = inScan.nextDouble();
        System.out.println("Enter the xmax boundary requirement:");
        double xmax = inScan.nextDouble();
        System.out.println("Enter the ymin boundary requirement:");
        double ymin = inScan.nextDouble();
        System.out.println("Enter the ymax boundary requirement:");
        double ymax = inScan.nextDouble();


        //insert the tuple
        String insert = "INSERT INTO FOREST (name,area,acid_level,mbr_xmin,mbr_xmax,mbr_ymin,mbr_ymax) " +
                "VALUES ('"+name+"',"+area+", "+acidlvl+","+xmin+","+xmax+","+ymin+","+ymax+");";
        Statement st = conn.createStatement();
        try {
            conn.setAutoCommit(false);
            st.executeUpdate(insert);
            conn.commit();
        } catch (SQLException e1) {
            try {
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        }

        //display the registry
        System.out.println("------------------------------------");
        System.out.println("Forest Registry");
        st2 = conn.createStatement();
        String query1 =
                "SELECT forest_no, name, area, acid_level FROM FOREST";
        res1 = st2.executeQuery(query1);
        String rid, rname, rarea, racid;
        while (res1.next()) {
            rid = res1.getString("forest_no");
            rname = res1.getString("name");
            rarea = res1.getString(3);
            racid = res1.getString(4);
            System.out.println(rid + " " + rname + " " + rarea+ " " + racid);
        }
        System.out.println("------------------------------------");


    }

    private static void insertWorker(Connection conn) throws SQLException
    {
        inScan.nextLine();
        System.out.println("Enter the name of the worker:");
        String name = inScan.nextLine();
        System.out.println("Enter the Social Security Number (SSN) of the worker:");
        String ssn = inScan.next();
        System.out.println("Enter the rank of the worker:");
        int rank = inScan.nextInt();
        System.out.println("Enter the worker's employing state as an abbreviation:");
        String estate = inScan.next();


        //check if its possbile
        String checkName = "SELECT name, ssn FROM WORKER WHERE name = '"+name+"'OR ssn = '"+ssn+"'";
        Statement st2 = conn.createStatement();
        ResultSet res1 = st2.executeQuery(checkName);
        String fname = "";
        if (res1.next()) {
            fname = res1.getString("name") + " " + res1.getString("ssn");
            System.out.println(fname + ": This worker already exists, please try again.");
            System.out.println("------------------------------------");
            return;
        }

        //insert the tuple
        String insert = "INSERT INTO WORKER VALUES ('"+ssn+"', '"+name+"', "+rank+", '"+estate+"');";
        Statement st = conn.createStatement();
        try {
            conn.setAutoCommit(false);
            st.executeUpdate(insert);
            conn.commit();
        } catch (SQLException e1) {
            try {
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        }


        //display the workers
        System.out.println("------------------------------------");
        System.out.println("Worker Registry");
        st2 = conn.createStatement();
        String query1 =
                "SELECT ssn, name, rank, employing_state FROM WORKER";
        res1 = st2.executeQuery(query1);
        String rid, rname, rrank, rstate;
        while (res1.next()) {
            rid = res1.getString(1);
            rname = res1.getString(2);
            rrank = res1.getString(3);
            rstate = res1.getString(4);
            System.out.println(rid + " " + rname + " " + rrank+ " " + rstate);
        }
        System.out.println("------------------------------------");

    }

    private static void insertSensor(Connection conn) throws SQLException
    {
        inScan.nextLine();
        System.out.println("Enter the x coordinate of the sensor:");
        int x = inScan.nextInt();
        System.out.println("Enter the y coordinate of the sensor:");
        int y = inScan.nextInt();

        //check if these coordinates already exist
        String checkLocation = "SELECT x, y FROM SENSOR WHERE x = " + x + " AND y = " + y;
        Statement st2 = conn.createStatement();
        ResultSet res1 = st2.executeQuery(checkLocation);
        String coordinates = "";
        if (res1.next()) {
            coordinates = res1.getString("x") + " " + res1.getString("y");
            System.out.println(coordinates+ ": A sensor already exists at this location. Please try again.");
            System.out.println("------------------------------------");
            return;
        }

        System.out.println("Enter the time the sensor was last charged:");
        System.out.println("Enter the month");
        String lc_month= inScan.next();
        System.out.println("Enter the day");
        String lc_day= inScan.next();
        System.out.println("Enter the year");
        String lc_year= inScan.next();
        System.out.println("Enter the time (hh24:mi)");
        String lc_time= inScan.next();

        System.out.println("Enter the maintainer of the sensor:");
        String ssn = inScan.next();
        System.out.println("Enter the time the sensor was last read:");
        System.out.println("Enter the month");
        String lr_month= inScan.next();
        System.out.println("Enter the day");
        String lr_day= inScan.next();
        System.out.println("Enter the year");
        String lr_year= inScan.next();
        System.out.println("Enter the time (hh24:mi)");
        String lr_time= inScan.next();
        System.out.println("Enter the energy level of the sensor:");
        int energy_level = inScan.nextInt();

        String last_charged = lc_month + "/" + lc_day + "/" + lc_year + " " + lc_time;
        String last_read = lr_month + "/" + lr_day + "/" + lr_year + " " + lr_time;

        //insert the tuple
        String insert = String.format("INSERT INTO SENSOR(x,y,last_charged,maintainer, last_read,energy) VALUES (%d,%d,TO_TIMESTAMP('%s', 'mm/dd/yyyy hh24:mi'),\'" +
                "%s',TO_TIMESTAMP('%s', 'mm/dd/yyyy hh24:mi'),%d);", x,y,last_charged,ssn, last_read, energy_level);
        String query = insert;
        Statement st = conn.createStatement();
        try {
            conn.setAutoCommit(false);
            st.executeUpdate(query);
            conn.commit();
        } catch (SQLException e1) {
            try {
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        }


        //display the workers
        System.out.println("------------------------------------");
        System.out.println("Sensor List");
        st2 = conn.createStatement();
        String query1 =
                "SELECT sensor_id, x, y FROM SENSOR";
        res1 = st2.executeQuery(query1);
        String rid, rx, ry;
        while (res1.next()) {
            rid = res1.getString(1);
            rx = res1.getString(2);
            ry = res1.getString(3);
            System.out.println(rid + " " + rx + " " + ry);
        }
        System.out.println("------------------------------------");
    }

    private static void switchWorkers(Connection conn) throws SQLException
    {
        inScan.nextLine();
        System.out.println("Enter name of the first employee:");
        String name1 = inScan.nextLine();
        System.out.println("Enter name of the second employee:");
        String name2 = inScan.nextLine();

        //check if they can swap.
        //calling a function with return value
        Boolean rReturn;
        CallableStatement properCase = conn.prepareCall("{ ? = call checkState( ?, ? ) }");
        properCase.registerOutParameter(1, Types.BIT);
        properCase.setString(2, name1);
        properCase.setString(3, name2);
        properCase.execute();
        rReturn = properCase.getBoolean(1);
        properCase.close();
        if(!rReturn)
        {
            System.out.println("The workers are not eligible to work in the others state.");
            return;
        }

        String q = "BEGIN;\n" +
                "INSERT INTO WORKER VALUES ('xxxxxxxxx','x',0,'PA');\n" +
                "COMMIT;\n" +
                "BEGIN;\n" +
                "UPDATE SENSOR\n" +
                "SET maintainer = 'xxxxxxxxx'\n" +
                "WHERE maintainer = (SELECT w.ssn\n" +
                "                  FROM WORKER w\n" +
                "                  WHERE w.name = '" + name1 + "');\n" +
                "COMMIT;\n" +
                "BEGIN;\n" +
                "UPDATE SENSOR\n" +
                "SET maintainer = (SELECT w.ssn\n" +
                "                  FROM WORKER w\n" +
                "                  WHERE w.name = '" + name1 + "')\n" +
                "WHERE maintainer = (SELECT w.ssn\n" +
                "                  FROM WORKER w\n" +
                "                  WHERE w.name = '" + name2 + "');\n" +
                "COMMIT;\n" +
                "BEGIN;\n" +
                "UPDATE SENSOR\n" +
                "SET maintainer = (SELECT w.ssn\n" +
                "                  FROM WORKER w\n" +
                "                  WHERE w.name = '" + name2 + "')\n" +
                "WHERE maintainer = 'xxxxxxxxx';\n" +
                "COMMIT;\n" +
                "BEGIN;\n" +
                "DELETE FROM WORKER WHERE ssn='xxxxxxxxx';\n" +
                "COMMIT;";

        Statement st = conn.createStatement();
        try {
            conn.setAutoCommit(false);
            st.executeUpdate(q);
            conn.commit();
        } catch (SQLException e1) {
            try {
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        }
        System.out.println(String.format("Successfully swapped duties for %s and %s", name1, name2));

    }

    private static void updateSensorStatus(Connection conn) throws SQLException
    {
        inScan.nextLine();
        System.out.println("Enter the x coordinate of the sensor:");
        int x = inScan.nextInt();
        System.out.println("Enter the y coordinate of the sensor:");
        int y = inScan.nextInt();

        Boolean rReturn;
        CallableStatement properCase = conn.prepareCall("{ ? = call checkCoordinates( ?, ? ) }");
        properCase.registerOutParameter(1, Types.BIT);
        properCase.setInt(2, x);
        properCase.setInt(3, y);
        properCase.execute();
        rReturn = properCase.getBoolean(1);
        properCase.close();
        if(!rReturn)
        {
            System.out.println("There is no sensor at these coordinates!");
            return;
        }

        System.out.println("Enter the energy level of the sensor:");
        int energy_level = inScan.nextInt();

        System.out.println("Enter the time the sensor was last charged:");
        System.out.println("Enter the month");
        String lc_month= inScan.next();
        System.out.println("Enter the day");
        String lc_day= inScan.next();
        System.out.println("Enter the year");
        String lc_year= inScan.next();
        System.out.println("Enter the time (hh24:mi)");
        String lc_time= inScan.next();

        String last_read = lc_month + "/" + lc_day + "/" + lc_year + " " + lc_time;
        //update the sensor table to change the last charged and energy.
        //insert the tuple
        String insert = String.format("UPDATE SENSOR S SET S.last_read = TO_TIMESTAMP('%s', 'mm/dd/yyyy hh24:mi')" +
                        "WHERE S.x = %d AND S.y = %d;", last_read, x, y);
        String query = String.format("UPDATE SENSOR S SET S.energy= %d" +
                "WHERE S.x = %d AND S.y = %d;", energy_level, x, y);
        Statement st = conn.createStatement();
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
        int temperature = inScan.nextInt();

        //we are inserting into the report table which should trigger the insertion into emergency
        st = conn.createStatement();
        String getID = String.format("SELECT S.sensor_id FROM SENSOR S\n" +
                        "WHERE S.x = %d AND S.y = %d;", x, y);
        ResultSet res1 = st.executeQuery(getID);
        int sensorId = res1.getInt(1);

        //i inserted the report time as the last charged.
        query = String.format("INSERT INTO REPORT VALUES(%d, TO_TIMESTAMP('%s', 'mm/dd/yyyy hh24:mi')), %d",
                sensorId, last_read, temperature);
        st = conn.createStatement();
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
        if(temperature > 100)//then we are gonna query the emergency table to make sure it exists.
        {
            st = conn.createStatement();
            String getEm = String.format("SELECT * FROM EMERGENCY R\n" +
                    "WHERE E.sensor_id = %d", sensorId);
            res1 = st.executeQuery(getEm);
            int a, c;
            String b;
            while (res1.next()) {
                a = res1.getInt(1);
                b = res1. getString(2);
                c = res1.getInt(3);

                System.out.println(a + " " + b + " " + c);
            }
        }
    }

    private static void updateForestArea(Connection conn) throws SQLException
    {
        inScan.nextLine();
        System.out.println("Enter the name of the Forest:");
        String fName = inScan.nextLine();
        System.out.println("Enter the new area to be updated");
        int area = inScan.nextInt();
        System.out.println("Enter the abbreviation of the state that the forest spans:");
        String state = inScan.next();

        String q = String.format("UPDATE FOREST F SET F.area = %d WHERE F.name = %s", area, fName);

        String g = String.format("SELECT F.forest_no FROM FOREST F WHERE F.name = %s", fName);

        Statement st = conn.createStatement();
        try {
            conn.setAutoCommit(false);
            st.executeUpdate(q);
            conn.commit();
        } catch (SQLException e1) {
            try {
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println(e2.toString());
            }
        }

        ResultSet res1 = st.executeQuery(g);
        int n;
        n = res1. getInt(1);
        //i litterly do not know what to do
    }

    private static void getTopK(Connection conn) throws SQLException
    {
        inScan.nextLine();
        System.out.println("Enter a top k value: ");
        int k = inScan.nextInt();

        String q = String.format("SELECT A.name, count(sensor_id) FROM (SELECT name, sensor_id FROM WORKER JOIN sensor ON worker.ssn = sensor.maintainer WHERE energy <= 2) AS A\n" +
                "GROUP BY A.name FETCH FIRST %d ROWS ONLY ORDER BY count(sensor_id) DESC;", k);
        Statement st = conn.createStatement();
        ResultSet res1 = st.executeQuery(q);
        String name;
        while(res1.next())
        {
            name = res1.getString(1);
            System.out.println(name);
        }
    }

    private static void displaySensorRankings(Connection conn) throws SQLException
    {
        String q = "SELECT sensor_id, count(report_time) FROM REPORT GROUP BY sensor_id\n" +
                "HAVING count(report_time) IS NOT NULL ORDER BY count(report_time) DESC";

        Statement st = conn.createStatement();
        ResultSet res1 = st.executeQuery(q);
        String name;
        int count;
        while(res1.next())
        {
            count = res1.getInt(2);
            name = res1.getString(1);
            System.out.println(count + " " + name);
        }
    }
}



