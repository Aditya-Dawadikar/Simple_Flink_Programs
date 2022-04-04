import DataSourceHelpers.CustomSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import DataSourceHelpers.CustomSource;

public class SimpleMySqlCustomSourceSink {

    void run() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream input = env.addSource(new CustomSource());

        input.addSink(new CustomSink());

        env.execute();
    }
}
