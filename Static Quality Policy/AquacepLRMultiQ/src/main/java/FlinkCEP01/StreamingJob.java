package FlinkCEP01;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

    public class StreamingJob {

        public static void main(String[] args) throws Exception {

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(1);
            DataStream<DataEvent> input = env.fromElements(
                    new DataEvent("ProducerID p2 Type Temperature Value 19.48 GroundTruth 20.0 TimeStamp 1646081857515 SendingLatency 2.0"),
                    new DataEvent("ProducerID p2 Type Temperature Value 18.48 GroundTruth 19.0 TimeStamp 1646081857812 SendingLatency 2.0"),
                    new DataEvent("ProducerID p2 Type Temperature Value 20.48 GroundTruth 21.0 TimeStamp 1646081859090 SendingLatency 2.0"),
                    new DataEvent("ProducerID p2 Type Temperature Value 19.48 GroundTruth 20.0 TimeStamp 1646081857515 SendingLatency 2.0"),
                    new DataEvent("ProducerID p2 Type Temperature Value 18.48 GroundTruth 19.0 TimeStamp 1646081857819 SendingLatency 2.0"),
                    new DataEvent("ProducerID p2 Type Temperature Value 20.48 GroundTruth 21.0 TimeStamp 1646081859090 SendingLatency 2.0")
            );

            input.print();
            Pattern<DataEvent, ?> pattern = Pattern.<DataEvent>begin("start").where(new SimpleCondition<DataEvent>() {
                @Override
                public boolean filter(DataEvent e) throws Exception {
                    return e.getValue().equals("19.48");
                }
            }).next("next").where(new SimpleCondition<DataEvent>() {
                @Override
                public boolean filter(DataEvent e) throws Exception {
                    return e.getValue().equals("18.48");
                }
            });

            PatternStream<DataEvent> patternStream = CEP.pattern(input, pattern);

            // NOTE: This is the older version of the API in the CEP library. It is still available
            // and can be used, but the recommendation is to use the newer API to detect patterns
            DataStream<DataEvent> matches = patternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {
                @Override
                public void processMatch(Map<String, List<DataEvent>> map,
                                         Context context,
                                         Collector<DataEvent> collector) throws Exception {
                    collector.collect(new DataEvent("QueryID q1 TimeStamp "+map.get("next").get(0).getTimeStamp()));
                }
            });

            matches.print();

            env.execute("Single Pattern Match");
        }
    }

