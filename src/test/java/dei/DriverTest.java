package dei;

//import org.junit.Test;

import MR.Mappers.TestMapper;
import MR.Reducers.TestReducer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mrunit.mapreduce.*;
        import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
        import java.util.List;


public class DriverTest
{
    // Create mapper and reducer instances.
    private Mapper mapper = new TestMapper();
    private Reducer reducer = new TestReducer();

    // Drivers to drive mapper.
    private MapDriver<LongWritable, Text, IntWritable, IntWritable>
            mapDriver = MapDriver.newMapDriver(mapper);

    // Driver to drive reducer.
    private ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable>
            reduceDriver = ReduceDriver.newReduceDriver(reducer);

    // Driver to drive mapper reducer pipeline.
    private MapReduceDriver<LongWritable, Text, IntWritable, IntWritable, IntWritable, IntWritable>
            mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

    private final static LongWritable ZERO = new LongWritable(0);
    private final static Text text = new Text();
    private final static IntWritable ONE = new IntWritable(1);

    @Test
    public void mapOnly() throws IOException {

        text.set("[01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245");
        mapDriver.addInput(ZERO, text);

        text.set("unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985");
        mapDriver.addInput(ZERO, text);

        text.set("199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] \"GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0\" 200 4085");
        mapDriver.addInput(ZERO, text);

        mapDriver.addOutput(new IntWritable(200), ONE);
        mapDriver.addOutput(new IntWritable(200), ONE);
        mapDriver.addOutput(new IntWritable(200), ONE);

        mapDriver.runTest();
    }

    @Test
    public void reduceOnly() throws IOException {

        // Test reducer adds up values.
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(ONE);
        values.add(ONE);
        values.add(ONE);

        reduceDriver.addInput(new IntWritable(200), values);
        reduceDriver.addOutput(new IntWritable(200), new IntWritable(3));
        reduceDriver.runTest();
    }

    @Test
    public void mapReduce() throws IOException {

        // Test map reduce pipeline works correctly.
        text.set("[01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245");
        mapReduceDriver.addInput(ZERO, text);

        text.set("unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985");
        mapReduceDriver.addInput(ZERO, text);

        text.set("199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] \"GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0\" 200 4085");
        mapReduceDriver.addInput(ZERO, text);

        mapReduceDriver.addOutput(new IntWritable(200), new IntWritable(3));

        mapReduceDriver.runTest();
    }
}
