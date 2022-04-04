import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SimpleJoinQuery {

    SimpleJoinQuery(){}

    void run() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStream<Row> ordersStream = env.fromElements(
                Row.of(10012,112,"Margherita","1","Regular"),
                Row.of(10311,210,"Chicken Fiesta","1","Medium"),
                Row.of(10145,178,"Veg Delight","2","Pan Pizza")
        );
        DataStream<Row> usersStream = env.fromElements(
                Row.of(112,"Geekgod","Pune"),
                Row.of(210,"Chandler","NYC"),
                Row.of(178,"Aditya","Akurdi")
        );

        Table orderTable = tenv.fromDataStream(ordersStream)
                .as(
                        "order_id",
                        "user_id",
                        "products",
                        "quantity",
                        "size"
                );
        Table userTable = tenv.fromDataStream(usersStream)
                .as(
                        "user_id",
                        "user_name",
                        "location"
                );

        tenv.createTemporaryView("OrderTable",orderTable);
        tenv.createTemporaryView("UserTable",userTable);

        Table resultTable = tenv.sqlQuery(
                "Select " +
                        "UserTable.user_id,UserTable.user_name,OrderTable.order_id" +
                        " from " +
                        "OrderTable inner join UserTable" +
                        " on " +
                        "OrderTable.user_id = UserTable.user_id"
        );

        DataStream<Row> resultStream = tenv.toDataStream(resultTable);
        resultStream.print().setParallelism(1);

        env.execute();
    }

}
