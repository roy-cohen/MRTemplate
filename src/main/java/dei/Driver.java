package dei;

import MR.MRJob;
import MR.MRJobBuilder;
import MR.Mappers.TestMapper;
import Utilities.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool
{
    private Configuration conf;

    public static void main( String[] args ) throws Exception {
        int status = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(status);
    }

    public int run(String[] strings) throws Exception {

        conf = getConf();
        String inputPath = "resources/input";
        String outputPath = "resources/output";

        // Delete output directory.
        FileUtil.deleteDirectoryRecursive(outputPath);

        MRJob MRJob = new MRJobBuilder()
                .setJobName(inputPath)
                .setConf(conf)
                .setJarClass(Driver.class)
                .setInputPath(inputPath)
                .setInputDirRecursive(false)
                .setOutputPath(outputPath)
                .setMapperClass(TestMapper.class)
                //.setReducerClass(TestReducer.class)
                .setOutputKeyClass(Text.class)
                .setOutputValueClass(NullWritable.class)
                .createMRJob();
        boolean success = MRJob.Run();
        return success ? 0 : 1;
    }
}
