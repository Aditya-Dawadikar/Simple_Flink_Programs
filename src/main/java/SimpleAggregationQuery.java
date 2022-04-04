import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SimpleAggregationQuery {
    SimpleAggregationQuery(){

    }

    void run() throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Row> inputData = env.fromElements(
                Row.of(101,"ABC",13,"fours"),
                Row.of(102,"BCD",5,"sixes"),
                Row.of(103,"CDE",18,"fours"),
                Row.of(104,"DEF",4,"fours"),
                Row.of(103,"EFG",2,"sixes"),
                Row.of(104,"FGH",1,"sixes")
        );

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        Table inputTable = tenv.fromDataStream(inputData).as("id","username","score","type");

        tenv.createTemporaryView("Player",inputTable);
        Table results = tenv.sqlQuery("Select type,Sum(score) from Player group by type");

//        inputTable.printSchema();
//        results.printSchema();

        DataStream<Row> resultStream = tenv.toChangelogStream(results);
        resultStream.print().setParallelism(1);
        env.execute();
    }
//    public static void main(String args[]) throws Exception{
//
//    }
}
