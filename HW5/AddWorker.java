import java.net.Inet4Address;
import java.util.Properties;
import java.util.*;
import java.sql.*;

public class AddWorker {
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

        // Task#2: Add Worker
        // Ask the user to supply all the necessary fields for the new worker: worker SSN, worker
        // name, worker rank, and worker’s employing state abbreviation.

        Scanner kbd = new Scanner(System.in);
        int rank;
        String name = "", ssn = "", employingState;

        try{
        boolean validSsn = false;
        while(!validSsn){
            System.out.print("Please enter the worker's ssn: ");
            ssn = kbd.nextLine();

            String query1 = "SELECT ssn FROM WORKER WHERE ssn = '"+ssn+"'";
            ResultSet res1 = st.executeQuery(query1);

            if(!res1.next())
                validSsn = true;
            else
                System.out.print("The ssn already exists. ");
        }

        boolean validName = false;
        while(!validName){
            System.out.print("Please enter the worker's name: ");
            name = kbd.nextLine();
            name = name.toUpperCase();

            String query2 = "SELECT name FROM WORKER WHERE name = '"+name+"'";
            ResultSet res2 = st.executeQuery(query2);

            if(!res2.next())
                validName = true;
            else
                System.out.print("The name already exists. ");
        }


        System.out.print("Please enter the worker's rank: ");
        rank = kbd.nextInt();

        kbd.nextLine();

        System.out.print("Please enter the worker's employing state: ");
        employingState = kbd.nextLine();

        kbd.close();

        conn.setAutoCommit(true);

        Integer insert;
        CallableStatement checkState = conn.prepareCall("{ ? = call checkState(?) }");
        checkState.registerOutParameter(1, Types.INTEGER);
        checkState.setString(2, employingState);
        checkState.execute();
        insert = checkState.getInt(1);
        checkState.close();

        if(insert==1)
            System.out.println("The employing state you input has been successfully inserted.");


        conn.setAutoCommit(false);
        PreparedStatement insertWorker = conn.prepareStatement("INSERT INTO WORKER (ssn, name, rank, employing_state) VALUES (?, ?, ?, ?);");
        insertWorker.setString(1, ssn);
        insertWorker.setString(2, name);
        insertWorker.setInt(3, rank);
        insertWorker.setString(4, employingState);

        insertWorker.executeUpdate();
        conn.commit();

        PreparedStatement query2 = conn.prepareStatement("SELECT name FROM WORKER WHERE ssn = ?");
        query2.setString(1, ssn);
        ResultSet res2 = query2.executeQuery();
        while(res2.next()) {
            String confirmName = res2.getString("name");
            System.out.printf("\nYour insertion is successful with worker's name %s\n", confirmName);
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