/*
 * CS 4365 Project
 */

package gatech.hadoopER.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author eric
 */
public class Util {
        public static List<Path> recurseDir(FileSystem fs, Path dir) throws IOException {
        List<Path> files = new ArrayList<>();
        for(FileStatus item: fs.listStatus(dir)) {
            if(item.isDirectory()) {
                for (FileStatus iitem: fs.listStatus(item.getPath())) {
                    if(iitem.isFile()&&!iitem.getPath().getName().startsWith("_")) {
                        files.add(iitem.getPath());
                    }
                }
            }
            if(item.isFile()&&!item.getPath().getName().startsWith("_")) {
                files.add(item.getPath());
            }
        }
        return files;
    }
}
