package Utilities;


import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class FileUtil {

    public static void deleteDirectoryRecursive(String path){

        String[] deleteCommand = { "rm", "-rf", path };
        System.out.println("Deleting directory: " + path);
        try {
            Runtime.getRuntime().exec(deleteCommand);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readAndApplyLineByLine(String filePath, Function function) throws IOException {

        File file = new File(filePath);
        FileInputStream fis = new FileInputStream(file);

        //Construct BufferedReader from InputStreamReader
        try(BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
            String line = null;
            while ((line = br.readLine()) != null) {

                    function.apply(line);
            }

            br.close();
        }
    }
}
