package org.demo.bigdata.tfidf.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MapReducerUtils {

    public static void removeOutputFolder(Configuration configuration, String outputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }
}
