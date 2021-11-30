import java.util.Properties;
import java.util.*;
import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

public class UpdateSensor {
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

        // Task#5: Update sensor status
        //Ask the user to supply all the necessary information in order to update the sensor status.
        //That is: coordinates of the sensor, energy level, when it was last charged, and the temperature

        Scanner inScan = new Scanner(System.in);
        double x = 0, y = 0, temperature = 0;
        Timestamp last_charged;
        int energy = 0, sensor_id = 0;

        try{
            boolean validCord = false;
            while(!validCord){
                System.out.print("Please enter the x coordinate of the sensor: ");
                x = inScan.nextDouble();
                System.out.print("Please enter the y coordinate of the sensor: ");
                y = inScan.nextDouble();

                String query1 = "SELECT x,y,sensor_id FROM SENSOR WHERE x = '"+x+"' AND y = '"+y+"'";
                ResultSet res1 = st.executeQuery(query1);

                if(res1.next()) {
                    validCord = true;
                    sensor_id = res1.getInt("sensor_id");
                }
                else
                    System.out.print("The sensor is not in the records. ");
            }

              inScan.nextLine();

            System.out.print("Please enter the energy level: ");
            energy = inScan.nextInt();

            inScan.nextLine();

            System.out.print("Please enter the last charged time: ");
            String time = inScan.nextLine();
            last_charged = Timestamp.valueOf(time);

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();
            Timestamp timestamp = Timestamp.valueOf(now);

            System.out.print("Please enter the temperature: ");
            temperature = inScan.nextDouble();

            inScan.close();

            conn.setAutoCommit(false);
            PreparedStatement upDateSensor = conn.prepareStatement("UPDATE SENSOR SET energy = ? , last_charged = ? WHERE x = ? AND y = ?");
            upDateSensor.setInt(1, energy);
            upDateSensor.setTimestamp(2, last_charged);
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
            if(res2.next()) {
                System.out.print("An emergency was reported after the sensor status was updated.");
            }
        }
        catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The current insertion failed.");
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