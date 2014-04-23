/*
 * CS 4365 Project
 */
package gatech.hadoopER.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author eric
 */
public class ERUtil {

    public static void main(String[] args) {
        System.out.println(jaccardSimilarity("153 West Squire Dr", "147 West Squire Dr"));
    }

    public static List<Path> recurseDir(FileSystem fs, Path dir) throws IOException {
        List<Path> files = new ArrayList<>();
        for (FileStatus item : fs.listStatus(dir)) {
            if (item.isDirectory()) {
                for (FileStatus iitem : fs.listStatus(item.getPath())) {
                    if (iitem.isFile() && !iitem.getPath().getName().startsWith("_")) {
                        files.add(iitem.getPath());
                    }
                }
            }
            if (item.isFile() && !item.getPath().getName().startsWith("_")) {
                files.add(item.getPath());
            }
        }
        return files;
    }

    public static Set<String> splitWords(String name) {
        return new HashSet<>(Arrays.asList(name.split("\\s+")));
    }

    public static double jaccardSimilarity(String similar1, String similar2) {
        HashSet<String> h1 = new HashSet<>();
        HashSet<String> h2 = new HashSet<>();

        for (String s : similar1.split("\\s+")) {
            h1.add(s);
        }
        for (String s : similar2.split("\\s+")) {
            h2.add(s);
        }

        int sizeh1 = h1.size();
        //Retains all elements in h3 that are contained in h2 ie intersection
        h1.retainAll(h2);
        //h1 now contains the intersection of h1 and h2

        h2.removeAll(h1);
            //h2 now contains unique elements

        //Union 
        int union = sizeh1 + h2.size();
        int intersection = h1.size();

        return (double) intersection / union;
    }

    private static int minimum(int a, int b, int c) {
        return Math.min(Math.min(a, b), c);
    }

    public static int computeLevenshteinDistance(String str1, String str2) {
        int[][] distance = new int[str1.length() + 1][str2.length() + 1];

        for (int i = 0; i <= str1.length(); i++) {
            distance[i][0] = i;
        }
        for (int j = 1; j <= str2.length(); j++) {
            distance[0][j] = j;
        }

        for (int i = 1; i <= str1.length(); i++) {
            for (int j = 1; j <= str2.length(); j++) {
                distance[i][j] = minimum(
                        distance[i - 1][j] + 1,
                        distance[i][j - 1] + 1,
                        distance[i - 1][j - 1] + ((str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0 : 1));
            }
        }

        return distance[str1.length()][str2.length()];
    }
}
