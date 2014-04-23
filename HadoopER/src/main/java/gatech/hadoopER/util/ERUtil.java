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
        String s1 = "microsoft(r) money 2007 deluxe";
        String s2 = "microsoft(r) expression web 1.0";
        System.out.println(computeJaccardOfString(s1,s2));
        System.out.println(computeLevenshteinDistance(s1,s2));
        System.out.println(computeJaroWinklerDistance(s1, s2));
        String[] set = {"hello", "my name", "is", "jonas"};
        System.out.println(splitToWords(new HashSet<>(Arrays.asList(set))));
    }
    
    public static double getPercentDifference(double a, double b) {
        double avg = (a + b) / 2.0;
        return Math.abs((a - b)) / avg;
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
    
    public static Set<String> splitToWords(Set<String> strSet) {
        HashSet<String> words = new HashSet<>();
        for (String str: strSet) {
            words.addAll(splitString(str.replaceAll("[^a-z]","")));
        }
        return words;
    }
    
    public static Set<String> splitString(String str) {
        return new HashSet<>(Arrays.asList(str.split("\\s+")));
    }
    
    public static double computeJaccardOfWords(Set<String> set1, Set<String> set2) {
        return computeJaccardSimilarity(splitToWords(set1),splitToWords(set2));
    }
    
    public static double computeJaccardOfString(String similar1, String similar2) {
        HashSet<String> h1 = new HashSet<>();
        HashSet<String> h2 = new HashSet<>();
        h1.add(similar1);
        h2.add(similar2);
        return computeJaccardOfWords(h1, h2);
    }

    public static double computeJaccardSimilarity(Set<String> similar1, Set<String> similar2) {
        int sizeh1 = similar1.size();
        //Retains all elements in h3 that are contained in h2 ie intersection
        similar1.retainAll(similar2);
        //h1 now contains the intersection of h1 and h2

        similar2.removeAll(similar1);
            //h2 now contains unique elements

        //Union 
        int union = sizeh1 + similar2.size();
        int intersection = similar1.size();

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

    public static float computeJaroWinklerDistance(String s1, String s2) {
        int[] mtp = matches(s1, s2);
        float m = mtp[0];
        if (m == 0) {
            return 0f;
        }
        float j = ((m / s1.length() + m / s2.length() + (m - mtp[1]) / m)) / 3;
        float jw = j < getThreshold() ? j : j + Math.min(0.1f, 1f / mtp[3]) * mtp[2]
                * (1 - j);
        return jw;
    }

    private static int[] matches(String s1, String s2) {
        String max, min;
        if (s1.length() > s2.length()) {
            max = s1;
            min = s2;
        } else {
            max = s2;
            min = s1;
        }
        int range = Math.max(max.length() / 2 - 1, 0);
        int[] matchIndexes = new int[min.length()];
        Arrays.fill(matchIndexes, -1);
        boolean[] matchFlags = new boolean[max.length()];
        int matches = 0;
        for (int mi = 0; mi < min.length(); mi++) {
            char c1 = min.charAt(mi);
            for (int xi = Math.max(mi - range, 0), xn = Math.min(mi + range + 1, max
                    .length()); xi < xn; xi++) {
                if (!matchFlags[xi] && c1 == max.charAt(xi)) {
                    matchIndexes[mi] = xi;
                    matchFlags[xi] = true;
                    matches++;
                    break;
                }
            }
        }
        char[] ms1 = new char[matches];
        char[] ms2 = new char[matches];
        for (int i = 0, si = 0; i < min.length(); i++) {
            if (matchIndexes[i] != -1) {
                ms1[si] = min.charAt(i);
                si++;
            }
        }
        for (int i = 0, si = 0; i < max.length(); i++) {
            if (matchFlags[i]) {
                ms2[si] = max.charAt(i);
                si++;
            }
        }
        int transpositions = 0;
        for (int mi = 0; mi < ms1.length; mi++) {
            if (ms1[mi] != ms2[mi]) {
                transpositions++;
            }
        }
        int prefix = 0;
        for (int mi = 0; mi < min.length(); mi++) {
            if (s1.charAt(mi) == s2.charAt(mi)) {
                prefix++;
            } else {
                break;
            }
        }
        return new int[]{matches, transpositions / 2, prefix, max.length()};
    }

    private static float getThreshold() {
        return 0.7f;
    }
}
