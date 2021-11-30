import java.util.Properties;
import java.util.*;
import java.sql.*;

public class AddSensor {
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

        // Task#3: Add sensor
        //Ask the user to supply all the necessary fields for the new sensor: X coordinate,
        // Y coordinate, last time it was charged, maintainer, last time it generated a report, and energy level).

        Scanner kbd = new Scanner(System.in);
        double x = 0, y = 0;
        Timestamp last_charged, last_read;
        int energy = 0;
        String maintainer = "";

        try{
        boolean validCord = false;
        while(!validCord){
            System.out.print("Please enter the x coordinate of the sensor: ");
            x = kbd.nextDouble();
            System.out.print("Please enter the y coordinate of the sensor: ");
            y = kbd.nextDouble();

            String query1 = "SELECT x,y FROM SENSOR WHERE x = '"+x+"' AND y = '"+y+"'";
            ResultSet res1 = st.executeQuery(query1);

            if(!res1.next())
                validCord = true;
            else
                System.out.print("The x, y coordinates already exist. ");
        }

        kbd.nextLine();

        // use Sushi's code
        System.out.print("Please enter the last charged time: ");
        String time = kbd.nextLine();
        last_charged = Timestamp.valueOf(time);

        boolean validName = false;
        while(!validName) {
            System.out.print("Please enter the maintainer's Social Security Number: ");
            maintainer = kbd.nextLine();

            String query3 = "SELECT ssn FROM WORKER WHERE ssn = '"+maintainer+"'";
            ResultSet res3 = st.executeQuery(query3);

            if(res3.next())
                validName = true;
            else
                System.out.print("The ssn is not in the worker's table. ");
        }

        // use Sushi's code
        System.out.print("Please enter the last read time: ");
        last_read = Timestamp.valueOf(kbd.nextLine());

        System.out.print("Please enter the energy: ");
        energy = kbd.nextInt();

        kbd.close();

            conn.setAutoCommit(false);
            PreparedStatement insertSensor = conn.prepareStatement("INSERT INTO SENSOR (sensor_id, x, y, last_charged, maintainer, last_read, energy) VALUES (DEFAULT, ?, ?, ?, ?, ?, ?);");

            insertSensor.setDouble(1, x);
            insertSensor.setDouble(2, y);
            insertSensor.setTimestamp(3, last_charged);
            insertSensor.setString(4, maintainer);
            insertSensor.setTimestamp(5, last_read);
            insertSensor.setInt(6, energy);


            insertSensor.executeUpdate();
            conn.commit();

            PreparedStatement query2 = conn.prepareStatement("SELECT sensor_id FROM SENSOR WHERE maintainer = ?");
            query2.setString(1, maintainer);
            ResultSet res2 = query2.executeQuery();
            while(res2.next()) {
                int confirmNum = res2.getInt("sensor_id");
                System.out.printf("\nYour insertion is successful with sensor id %d\n", confirmNum);
            }
        }
        catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The current insertion failed.");
                String query2 = "SELECT setval('SENSOR_sensor_id_seq', MAX(sensor_id), true) FROM SENSOR;";
                st.executeQuery(query2);
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