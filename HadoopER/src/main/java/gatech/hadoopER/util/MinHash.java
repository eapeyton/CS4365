/*
 * CS 4365 Project
 */
package gatech.hadoopER.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

/**
 * This class was used for testing but is not a functioning component.
 * @author eric
 */
public class MinHash {

    private static final int NUM = 200;
    private static final int GRAM = 3;

    public static void main(String[] args) throws FileNotFoundException, JDOMException, IOException {
        ArrayList<String> descriptions = new ArrayList<>();

        SAXBuilder sax = new SAXBuilder();
        Document doc = sax.build("/Users/eric/Development/CS4365/HadoopER/src/main/resources/descriptions.txt");
        Element root = doc.getRootElement();
        System.out.println(root.getChildren("event").size());
        for (Element descs : root.getChildren()) {
            descriptions.add(descs.getTextNormalize());
        }

        int[] rands = new int[NUM];
        Random rand = new Random();
        for (int i = 0; i < rands.length; i++) {
            rands[i] = rand.nextInt();
        }

        int[][] minHashes = new int[descriptions.size()][NUM];
        for (String description : descriptions) {
            //String nDescript = description.replaceAll("[\n\t]", " ").replaceAll("[^A-Za-z0-9 \\-]", "");
            String nDescript = description.replaceAll("[^A-Za-z0-9\\-]", "");
            //String[] words = nDescript.split(" ");
            ArrayList<String> shingles = new ArrayList<>();
            for (int i = 0; i < nDescript.length() - GRAM - 1; i++) {
                String shingle = "" + nDescript.charAt(i);
                for (int j = 1; j < GRAM; j++) {
                    shingle += nDescript.charAt(i + j);
                }
                shingles.add(shingle.toLowerCase());
            }
            int[] mins = new int[NUM];
            for (int i = 0; i < mins.length; i++) {
                int min = Integer.MAX_VALUE;
                for (String s : shingles) {
                    int hash = s.hashCode() ^ rands[i];
                    if (hash < min) {
                        min = hash;
                    }
                    mins[i] = min;
                }
            }
            minHashes[descriptions.indexOf(description)] = mins;
        }
        int[][] counts = new int[descriptions.size()][descriptions.size()];
        for (int[] dcount : counts) {
            for (int i = 0; i < dcount.length; i++) {
                dcount[i] = 0;
            }
        }
        for (int i = 0; i < minHashes.length; i++) {
            int[] m = minHashes[i];
            for (int n : m) {
                int count = 0;
                for (int j = 0; j < minHashes.length; j++) {
                    int[] m2 = minHashes[j];
                    for (int n2 : m2) {
                        if (n == n2 && i != j) {
                            counts[i][j]++;
                        }
                    }
                }
                if (count > 1) {
                }
            }
        }
        for (int[] minHash : minHashes) {
            System.out.println(Arrays.toString(minHash));
        }
        for (int[] count : counts) {
            //System.out.println(Arrays.toString(count));
        }
    }
}
