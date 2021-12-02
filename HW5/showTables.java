package HW5;

import java.sql.*;

public class showTables {

    public static void forest(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();
        String getForest = "SELECT * FROM FOREST";
        ResultSet res2 = st.executeQuery(getForest);
        int forestNum;
        String forestName;
        double area, acid_level, xmin, xmax, ymin, ymax;
        System.out.println(String.format("%15s%30s%15s%15s%15s%15s%15s%15s",
                "|forest no|", "|name|", "|area|", "|acid_level|", "|x-min|", "|x-max|", "|y-min|", "|y-max|"));
        while (res2.next()) {
            forestNum = res2.getInt("forest_no");
            forestName = res2.getString("name");
            area = res2.getDouble("area");
            acid_level = res2.getDouble("acid_level");
            xmin = res2.getDouble("mbr_xmin");
            xmax = res2.getDouble("mbr_xmax");
            ymin = res2.getDouble("mbr_ymin");
            ymax = res2.getDouble("mbr_ymax");
            System.out.println(String.format("%15s%30s%15s%15s%15s%15s%15s%15s",
                    forestNum, forestName, area, acid_level,xmin,xmax,ymin,ymax));
        }
        System.out.println("-----------------------------------------");
    }

    public static void worker(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();
        String getForest = "SELECT * FROM WORKER";
        ResultSet res2 = st.executeQuery(getForest);
        String ssn = "", workerName = "", state = "";
        int rank;
        System.out.println(String.format("%15s%15s%15s%15s",
                "|ssn|", "|name|", "|rank|", "|employing state|"));
        while (res2.next()) {
            workerName = res2.getString("name");
            ssn = res2.getString("ssn");
            state = res2.getString("employing_state");
            rank = res2.getInt("rank");
            System.out.println(String.format("%15s%15s%15s%15d", workerName, ssn, rank, state));
        }
        System.out.println("-----------------------------------------");
    }

    public static void sensor(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();
        String getForest = "SELECT * FROM SENSOR";
        ResultSet res2 = st.executeQuery(getForest);
        int sensor_id;
        double x, y;
        String lastCharged;
        String lastRead;
        String maintainer;
        int energy;

        System.out.println(String.format("%15s%15s%30s%15s%15s%15s%25s",
                "|sensor_id|", "|maintainer|", "|last charged|", "|energy_level|", "|x|", "|y|", "|last read|"));
        while (res2.next()) {
            sensor_id = res2.getInt("sensor_id");
            x = res2.getDouble("x");
            y = res2.getDouble("y");
            lastCharged = res2.getString("last_charged");
            lastRead= res2.getString("last_read");
            maintainer = res2.getString("maintainer");
            energy = res2.getInt("energy");
            System.out.println(String.format("%15d%15s%30s%15d%15f%15f%25s", sensor_id, maintainer, lastCharged, energy, x, y, lastRead));
        }
        System.out.println("-----------------------------------------");
    }

    public static void state(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();
        String getForest = "SELECT * FROM STATE";
        ResultSet res2 = st.executeQuery(getForest);

        String name;
        String abbreviaton;
        int area;
        int population;

        System.out.println(String.format("%15s%15s%15s%15s",
                "|name|", "|abbreviation|", "|area|", "|population|"));

        while (res2.next()) {
            name = res2.getString("name");
            abbreviaton = res2.getString("abbreviation");
            area = res2.getInt("area");
            population = res2.getInt("population");

            System.out.println(String.format("%15s%15s%15d%15d", name, abbreviaton,area, population));
        }
        System.out.println("-----------------------------------------");
    }

    public static void coverage(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();
        String getForest = "SELECT * FROM COVERAGE";
        ResultSet res2 = st.executeQuery(getForest);

        String forestNum;
        String state;
        double percentage;
        int area;

        System.out.println(String.format("%15s%15s%15s%15s",
                "|forest_no|", "|state|", "|area|", "|percentage|"));

        while (res2.next()) {
            forestNum = res2.getString("forest_no");
            state = res2.getString("state");
            area = res2.getInt("area");
            percentage = res2.getDouble("percentage");

            System.out.println(String.format("%15s%15s%15d%15f", forestNum, state,area, percentage));
        }
        System.out.println("-----------------------------------------");
    }

    public static void report(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();
        String getForest = "SELECT * FROM REPORT";
        ResultSet res2 = st.executeQuery(getForest);


        String sensorID;
        String report_time;
        double temp;

        System.out.println(String.format("%15s%25s%15s",
                "|sensor_id|", "|report_time|", "|temperature|"));

        while (res2.next()) {
            sensorID = res2.getString("sensor_id");
            report_time = res2.getString("report_time");
            temp = res2.getDouble("temperature");


            System.out.println(String.format("%15s%25s%15f", sensorID, report_time,temp));
        }
        System.out.println("-----------------------------------------");
    }
    public static void emergency(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();
        String getForest = "SELECT * FROM EMERGENCY";
        ResultSet res2 = st.executeQuery(getForest);


        String sensorID;
        String report_time;

        System.out.println(String.format("%15s%25s",
                "|sensor_id|", "|report_time|", "|temperature|"));

        while (res2.next()) {
            sensorID = res2.getString("sensor_id");
            report_time = res2.getString("report_time");


            System.out.println(String.format("%15s%25s", sensorID, report_time));
        }
        System.out.println("-----------------------------------------");
    }
}
