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
}
