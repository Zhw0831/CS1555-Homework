import java.net.Inet4Address;
import java.util.Properties;
import java.util.*;
import java.sql.*;


public class SwitchWorker {
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

        //Task#4: switch worker duties
        //Ask the user to supply the two workers’ names (worker A and worker B).
        //Make sure that the sensors that worker A was maintaining, are the now maintained by
        //worker B, and vice versa. Also, make sure that sensors that are maintained by worker A
        //could be maintained by worker B (i.e., worker A sensors are in the same employing state
        //of worker B, and vice versa).

        Scanner kbd = new Scanner(System.in);
        String nameA = "", nameB = "";

        boolean validNameA = false;
        while(!validNameA){
            System.out.print("Enter the name of worker A: ");
            nameA = kbd.nextLine();

            String query1 = "SELECT name FROM WORKER WHERE name = '"+nameA+"'";
            ResultSet res1 = st.executeQuery(query1);

            if(res1.next())
                validNameA = true;
            else
                System.out.print("Worker A's name is not in the worker table. ");
        }

        boolean validNameB = false;
        while(!validNameB){
            System.out.print("Enter the name of worker B: ");
            nameB = kbd.nextLine();

            String query2 = "SELECT name FROM WORKER WHERE name = '"+nameB+"'";
            ResultSet res2 = st.executeQuery(query2);

            if(res2.next())
                validNameB = true;
            else
                System.out.print("Worker B's name is not in the worker table. ");
        }

        PreparedStatement getState = conn.prepareStatement("SELECT ssn, employing_state FROM WORKER WHERE name = ?");

        getState.setString(1, nameA);
        ResultSet res2 = getState.executeQuery();

        String stateA = "", ssnA = "";
        while(res2.next()) {
            stateA = res2.getString("employing_state");
            ssnA = res2.getString("ssn");
        }

        getState.setString(1, nameB);
        ResultSet res3 = getState.executeQuery();

        String stateB = "", ssnB = "";
        while(res3.next()) {
            stateB = res3.getString("employing_state");
            ssnB = res3.getString("ssn");
        }

        if(!stateA.equals(stateB)){
            System.out.print("The two workers work in different states. Cannot switch duties.");
            System.exit(0);
        }

        try{
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

            System.out.print("Your switch is successful.");
        }
        catch (SQLException e1) {
            try {
                System.out.print(e1.toString());
                System.out.println("The switch failed.");
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
