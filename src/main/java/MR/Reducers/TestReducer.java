package MR.Reducers;


import com.google.common.collect.Iterables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TestReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private static final IntWritable intWritable = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = Iterables.size(values);

        intWritable.set(count);

        context.write(key, intWritable);
    }
}