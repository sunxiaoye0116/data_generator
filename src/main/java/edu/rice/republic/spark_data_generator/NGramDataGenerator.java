package edu.rice.republic.spark_data_generator;

/**
 * Created by xs6 on 10/10/16.
 */


import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;


public class NGramDataGenerator {
    private HashSet<Long> vocabulary;
    private ArrayList<Long> vocabList;
    private String inputPath;
    private String outputPath;
    private int N;
    private int docNum;

    public NGramDataGenerator(String input, String output, int n, int doc) {
        this.vocabulary = new HashSet<Long>();

        if (!input.endsWith("/")) {
            input += "/";
        }
        this.inputPath = input;
        if (!output.endsWith("/")) {
            output += "/";
        }
        this.outputPath = output;
        this.N = n;
        this.docNum = doc;
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.print("program <inputpath> <N-gram> <docNum:16923>");
        }

        String inputPath = args[0];
        int N = Integer.parseInt(args[1]);
        int docNum = Integer.parseInt(args[2]);

        String outputPath = inputPath + "_" + N + "_gram";
        File file = new File(outputPath);
        if (!file.exists()) {
//            System.out.println("file " + outputPath + " doesn't exist!");
            file.mkdirs();
        }
        NGramDataGenerator generator = new NGramDataGenerator(inputPath, outputPath, N, docNum);
        generator.getVocab();
        generator.encodeVocab();
        generator.createWord();
    }

    public void getVocab() {
        for (int i = 0; i < docNum; i++) {
            String docName = inputPath + i + ".txt";
            ArrayList<Long> singletonList = new ArrayList<Long>();

            BufferedReader br = null;
            try {
                singletonList.clear();
                br = new BufferedReader(new FileReader(docName));

                String line = br.readLine();
                while (line != null) {
                    long curntLn = Long.parseLong(line);
                    singletonList.add(curntLn);
                    line = br.readLine();
                }

                for (int n = 0; n < N; n++) {
                    int startIndex = 0;
                    while (startIndex + n < singletonList.size()) {
                        long encode = 0L;
                        for (int k = 0; k <= n; k++) {
                            encode = 10000 * encode + singletonList.get(startIndex + k);
                        }
                        if (!vocabulary.contains(encode)) {
                            vocabulary.add(encode);
                        }
                        startIndex++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (br != null)
                        br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void encodeVocab() {
        vocabList = new ArrayList<Long>(vocabulary);
        Collections.sort(vocabList);

                /*for (int i = 0; i < vocabList.size(); i++) {
                    System.out.println(i + ": " + vocabList.get(i));
                }*/

        System.out.println("Vocabulary size is: " + vocabList.size());
    }

    public void createWord() {
        for (int i = 0; i < docNum; i++) {
            String docName = inputPath + i + ".txt";
            String outDoc = outputPath + i + ".txt";
            ArrayList<Long> singletonList = new ArrayList<Long>();

            BufferedReader br = null;
            BufferedWriter bw = null;
            try {
                singletonList.clear();
                br = new BufferedReader(new FileReader(docName));

                File outFile = new File(outDoc);
                if (!outFile.exists()) {
                    outFile.createNewFile();
                }
                bw = new BufferedWriter(new FileWriter(outFile));

                String line = br.readLine();
                while (line != null) {
                    long curntLn = Long.parseLong(line);
                    singletonList.add(curntLn);
                    line = br.readLine();
                }

                for (int n = 0; n < N; n++) {
                    int startIndex = 0;
                    while (startIndex + n < singletonList.size()) {
                        long encode = 0L;
                        for (int k = 0; k <= n; k++) {
                            encode = 10000 * encode + singletonList.get(startIndex + k);
                        }

                        int vocabIndex = Collections.binarySearch(vocabList, encode);
                        //System.out.println(vocabIndex);
                        bw.write(vocabIndex + "\n");

                        startIndex++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (br != null)
                        br.close();
                    if (bw != null)
                        bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
