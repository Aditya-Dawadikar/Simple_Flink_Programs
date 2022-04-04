import DataSourceHelpers.Book;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleMySqlConnection {

    void run() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                new Book(101, "abcde", "bdew, ndoqbf", 2019),
                new Book(102, "fghi", "fh2, fhp23h", 2018),
                new Book(103, "jklm", "fboqow", 2017),
                new Book(104, "nopqr", "fh92f 239hfh", 2017)
        ).addSink(
                JdbcSink.sink(
//                        DML Statement --mandatory
                        "Insert into books(id,title,authors,year) values (?,?,?,?)",
//                        Statement Builder --mandatory
                        (statement,book)->{
                            statement.setLong(1,book.id);
                            statement.setString(2,book.title);
                            statement.setString(3,book.authors);
                            statement.setInt(4,book.year);
                        },
//                        Execution Options --optional
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
//                        JDBC connection options --mandatory
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/temp_db")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("chandler")
                                .withPassword("")
                                .build()
                )
        );

        env.execute();
    }
}
