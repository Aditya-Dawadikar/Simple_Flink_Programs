import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SimpleSelectQuery {

    SimpleSelectQuery(){

    }

    void run() throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Row> input = env.fromElements(
                Row.of("Aditya","Dawadikar",100),
                Row.of("Geekgod","Chandler",101),
                Row.of("boba","fett",102),
                Row.of("Luke","Skywalker",103)
        );
        input.print().setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        Table inputTable = tenv.fromDataStream(input).as("fname","lname","id");

        tenv.createTemporaryView("InputTable",inputTable);
        Table resultTable = tenv.sqlQuery("SELECT UPPER(fname),UPPER(lname),id FROM InputTable");

        DataStream<Row> resultStream = tenv.toDataStream(resultTable);

        resultStream.print().setParallelism(1);

        env.execute();
    }

}
