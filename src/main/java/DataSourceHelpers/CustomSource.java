package DataSourceHelpers;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

public class CustomSource extends RichSourceFunction<Book> {
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

        String query = "select * from books";
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
    public void run(SourceContext ctx) throws SQLException {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Book b = new Book(
                    resultSet.getInt("id"),
                    resultSet.getString("title").trim(),
                    resultSet.getString("authors").trim(),
                    resultSet.getInt("year"));
            ctx.collect(b);
        }
    }

    @Override
    public void cancel(){

    }

    Connection getConnection() throws ClassNotFoundException, SQLException {
        Connection con;
        Class.forName(DRIVER);
        con = DriverManager.getConnection(URL,USERNAME,PASSWORD);
        return con;
    }
}
