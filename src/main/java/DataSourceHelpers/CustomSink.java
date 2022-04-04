package DataSourceHelpers;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CustomSink extends RichSinkFunction<Book> {
    PreparedStatement ps;
    private Connection con;
    private static String USERNAME = "chandler";
    private static String PASSWORD = "";
    private static String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static String URL = "jdbc:mysql://localhost:3306/temp_db";

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        con = getConnection();

        String query = "Insert into books_2 values(?,?,?,?)";
        ps = con.prepareStatement(query);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(con!=null){
            con.close();
        }
        if(ps!=null){
            ps.close();
        }
    }

    @Override
    public void invoke(Book b,Context ctx) throws SQLException {
        ps.setInt(1,b.getId());
        ps.setString(2,b.getTitle());
        ps.setString(3,b.getAuthors());
        ps.setInt(4,b.getYear());
        ps.executeUpdate();
    }

    Connection getConnection() throws ClassNotFoundException, SQLException {
        Connection con;
        Class.forName(DRIVER);
        con = DriverManager.getConnection(URL,USERNAME,PASSWORD);
        return con;
    }
}
