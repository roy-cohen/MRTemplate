package MR.Mappers;

import com.google.common.base.Function;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;

import Utilities.FileUtil;

import javax.annotation.Nullable;

public class TestMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final String locationFile = "locations.csv";

    private HashMap<Integer,String> locMap = new HashMap<>();

    public Void apply(String line) {
        String[] elem = line.split(",");

        if (!elem[0].startsWith("#")) {
            locMap.put(Integer.parseInt(elem[0]), elem[1] + "," + elem[2]);
        }

        return null;
    }

    protected void setup(Mapper.Context context) throws IOException, InterruptedException {

        Path file = Paths.get("locations.csv");
        Charset charset = Charset.defaultCharset();

        try (Stream<String> lines = Files.lines(file,charset)) {
            lines.forEach((line) -> {
                String[] elem = line.split(",");

                if (!elem[0].startsWith("#")) {
                    locMap.put(Integer.parseInt(elem[0]), elem[1] + "," + elem[2]);
                }
            });
        }

//        try (Stream<String> lines = Files.lines(file,charset)) {
//            lines.forEach((line) -> applyToLine(line));
//        }
    }

    private void applyToLine(String line) {
        String[] elem = line.split(",");

        if (!elem[0].startsWith("#")) {
            locMap.put(Integer.parseInt(elem[0]), elem[1] + "," + elem[2]);
        }
    }

    private static final IntWritable ONE = new IntWritable(1);

    public void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {


        // Extract text from value.
        String line = value.toString();
        String[] elem = line.split(",");

        if (!elem[0].startsWith("#")) {

            int locID = Integer.parseInt(elem[1]);
            String location = locMap.get(locID);

            Text text = new Text(line + "," + location);
            context.write(text, NullWritable.get());
        }
    }
}
