import java.util.Properties;
import java.sql.*;

public class JavaDemoFunctionCalling {
    public static void main(String args[]) throws
            ClassNotFoundException, SQLException {

        //check if jdbc driver is properly linked
        Class.forName("org.postgresql.Driver");

        //connection
        String url = "jdbc:postgresql://class3.cs.pitt.edu:5432/";
        Properties props = new Properties();
        props.setProperty("user", "<your_pitt_user>>");
        props.setProperty("password", "<your_pitt_pass>");


        //connection
        Connection conn = DriverManager.getConnection(url, props);

        //create a query
        Statement st = conn.createStatement();
        String query = "SELECT * FROM STUDENT WHERE Major='CS'";

        //execute a query
        ResultSet resultSet = st.executeQuery(query);

        //retrieve result
        String rId = "";
        String rName = "";
        String rMajor = "";
        while (resultSet.next()) {
            rId = resultSet.getString("SID");
            rName = resultSet.getString("Name");
            rMajor = resultSet.getString("major");
            System.out.println(rId + " " + rName + " " + rMajor);
        }


        //calling a function with return value
        Boolean rReturn;
        CallableStatement properCase = conn.prepareCall("{ ? = call can_pay_loan( ? ) }");
        properCase.registerOutParameter(1, Types.BIT);
        properCase.setString(2, "111222333");
        properCase.execute();
        rReturn = properCase.getBoolean(1);
        properCase.close();
        System.out.println(rReturn);


        //calling a function that returns query
        int id;
        query = "select * from returnning_new_student_table(123)";
        st = conn.createStatement();
        ResultSet resultSet2 = st.executeQuery(query);
        while (resultSet2.next()) {
            id = resultSet2.getInt("sid_r");
            rName = resultSet2.getString("name_r");
            System.out.println(id + " " + rName);
        }
    }
}