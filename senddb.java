import java.util.Scanner;
import java.sql.*;

/*
You can compile and run this example with a command like:
  javac senddb.java && 
  java -cp ~/bin/postgresql-42.0.0.jar:. senddb
You can download the postgres JDBC driver jar from https://jdbc.postgresql.org.
*/
public class senddb {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        // Load the postgres JDBC driver.
        Class.forName("org.postgresql.Driver");

        // Connect to the database.
        Connection db = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:26257/tpch?sslmode=disable", "root", "");

        try {
            Scanner input = new Scanner(System.in);
            while (input.hasNextLine()){
              db.createStatement().execute(input.nextLine());
            }
        } finally {
            // Close the database connection.
            db.close();
        }
    }
}
