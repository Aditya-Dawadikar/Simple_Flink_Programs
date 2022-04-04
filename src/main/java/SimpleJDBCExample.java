import java.sql.*;

public class SimpleJDBCExample {
    private static Connection con = null;
    private static String USERNAME = "chandler";
    private static String PASSWORD = "";
    private static String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static String URL = "jdbc:mysql://localhost:3306/temp_db";

    public static void run() throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER);
        con = DriverManager.getConnection(URL,USERNAME,PASSWORD);

        Statement stmt = con.createStatement();
        ResultSet res;

        res = stmt.executeQuery("Select * from books");

        while (res.next()) {
            int id = res.getInt("id");
            String title = res.getString("title");
            String authors = res.getString("authors");
            int year = res.getInt("year");
            System.out.println(id+","+title+","+authors+","+year);
        }
        res.close();
        stmt.close();
        con.close();
    }
}
