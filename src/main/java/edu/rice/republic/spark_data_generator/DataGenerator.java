package edu.rice.republic.spark_data_generator;

/**
 * Created by xs6 on 10/10/16.
 */


import jsc.distributions.Beta;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeMap;

public class DataGenerator {
    private static String folder = "";

    public static void main(String[] args) {
        if (args == null) {
            System.out.println("model (gmm, gmm_imp, lda, hmm, lasso)");
            System.exit(-1);
        }

        String model = args[0];
        if (model.equals("gmm")) {
            if (args.length < 7) {
                System.out.println("gmm " +
                        "num_machine " +
                        "num_docs " +
                        "num_cluster " +
                        "dimension " +
                        "outFolder " +
                        "system");
                System.exit(-1);
            }

            int num_machine = Integer.parseInt(args[1]);
            int num_data_per_machine = Integer.parseInt(args[2]) / num_machine;
            int num_cluster = Integer.parseInt(args[3]);
            int dimension = Integer.parseInt(args[4]);
            String outFolder = args[5];
            String system = args[6];

            if (system.equals("simsql")) {
                runGaussianSimSQL(num_machine,
                        num_data_per_machine,
                        num_cluster,
                        dimension,
                        outFolder);
            } else if (system.equals("giraph")) {
                runGaussianGiraph(num_machine,
                        num_data_per_machine,
                        num_cluster,
                        dimension,
                        outFolder);
            } else if (system.equals("spark")) {
                runGaussianSpark(num_machine,
                        num_data_per_machine,
                        num_cluster,
                        dimension,
                        outFolder);
            } else if (system.equals("graphlab")) {
                runGaussianGraphLab(num_machine,
                        num_data_per_machine,
                        num_cluster,
                        dimension,
                        outFolder);
            }
        } else if (model.equals("gmm_imp")) {
            if (args.length < 8) {
                System.out.println("gmm_imp " +
                        "num_machine " +
                        "num_docs " +
                        "num_cluster " +
                        "dimension " +
                        "missingFraction " +
                        "outFolder " +
                        "system");
                System.exit(-1);
            }

            int num_machine = Integer.parseInt(args[1]);
            int num_data_per_machine = Integer.parseInt(args[2]) / num_machine;
            int num_cluster = Integer.parseInt(args[3]);
            int dimension = Integer.parseInt(args[4]);
            double missingfraction = Double.parseDouble(args[5]);
            String outFolder = args[6];
            String system = args[7];

            if (system.equals("simsql")) {
                //do it by yourself by copying and changing the code.
            } else if (system.equals("giraph")) {
                runGaussianImpGiraph(num_machine,
                        num_data_per_machine,
                        num_cluster,
                        dimension,
                        missingfraction,
                        outFolder);
            } else if (system.equals("spark")) {
                //do it by yourself by copying and changing the code.
                runGaussianImpSpark(num_machine,
                        num_data_per_machine,
                        num_cluster,
                        dimension,
                        missingfraction,
                        outFolder);

            } else if (system.equals("graphlab")) {
                runGaussianImpGraphLab(num_machine,
                        num_data_per_machine,
                        num_cluster,
                        dimension,
                        missingfraction,
                        outFolder);
            }
        } else if (model.equals("lda")) {

            if (args.length < 7) {
                System.out.println("lda " +
                        "num_machine " +
                        "num_docs " +
                        "num_topics " +
                        "20news_source_path " +
                        "outFolder " +
                        "system");
                System.exit(-1);
            }
            int num_machine = Integer.parseInt(args[1]);
            int num_docs_per_machine = Integer.parseInt(args[2]) / num_machine;
            int num_topics = Integer.parseInt(args[3]);
            String source = args[4];
            String outFolder = args[5];
            String system = args[6];

            if (system.equals("simsql")) {
                runLDASimsql(num_machine, num_docs_per_machine, num_topics, source, outFolder);
            } else if (system.equals("simsql2")) {
                // group-based LDA.
                runLDASimsql2(num_machine, num_docs_per_machine, num_topics, source, outFolder);
            } else if (system.equals("giraph")) {
                runLDAGiraph(num_machine,
                        num_docs_per_machine,
                        num_topics,
                        source,
                        outFolder);
            } else if (system.equals("spark")) {
                runLDASpark(num_machine,
                        num_docs_per_machine,
                        num_topics,
                        source,
                        outFolder);
            } else if (system.equals("graphlab")) {
                runLDAGraphLab(num_machine,
                        num_docs_per_machine,
                        num_topics,
                        source,
                        outFolder);
            }
        } else if (model.equals("hmm")) {
            if (args.length < 7) {
                System.out.println("hmm " +
                        "num_machine " +
                        "num_docs " +
                        "num_topics " +
                        "20news_source_path " +
                        "outFolder " +
                        "system");
                System.exit(-1);
            }
            int num_machine = Integer.parseInt(args[1]);
            int num_docs_per_machine = Integer.parseInt(args[2]) / num_machine;
            int num_topics = Integer.parseInt(args[3]);
            String source = args[4];
            String outFolder = args[5];
            String system = args[6];

            if (system.equals("simsql")) {
                runHMMSimsql(num_machine,
                        num_docs_per_machine,
                        num_topics,
                        source,
                        outFolder);
            } else if (system.equals("giraph")) {
                int num_doc_per_line = 3125 * 2;
                int num_block_per_machine = num_docs_per_machine / num_doc_per_line;
                runHMMGiraph3(num_machine,
                        num_block_per_machine,
                        num_doc_per_line,
                        num_topics,
                        source,
                        outFolder);
            } else if (system.equals("spark")) {
                runHMMSpark(num_machine,
                        num_docs_per_machine,
                        num_topics,
                        source,
                        outFolder);

            } else if (system.equals("graphlab")) {
                runHMMGraphLab(num_machine,
                        num_docs_per_machine,
                        num_topics,
                        source,
                        outFolder);
            }
        } else if (model.equals("lasso")) {
            if (args.length < 6) {
                System.out.println("lasso " +
                        "num_machine " +
                        "num_docs " +
                        "dimension " +
                        "outFolder " +
                        "system");
                System.exit(-1);
            }

            int num_machine = Integer.parseInt(args[1]);
            int num_data_per_machine = Integer.parseInt(args[2]) / num_machine;
            int dimension = Integer.parseInt(args[3]);
            String outFolder = args[4];
            String system = args[5];
            if (system.equals("simsql")) {
                //
            } else if (system.equals("giraph")) {
                runBlockLasso(num_machine,
                        num_data_per_machine,
                        2500,
                        dimension,
                        outFolder);
            } else if (system.equals("spark")) {
                runLassoSpark(num_machine,
                        num_data_per_machine,
                        dimension,
                        outFolder);
            } else if (system.equals("graphlab")) {
                runLassoGraphLab(num_machine,
                        num_data_per_machine,
                        dimension,
                        outFolder);
            }
        }


    }


    public static void runGaussianSimSQL(int num_machines,
                                         int num_data_per_machine,
                                         int num_cluster,
                                         int dimension,
                                         String outFolder) {
        int num_data = num_machines * num_data_per_machine;

        // hyper parameters
        double hyper_mean[] = new double[dimension];
        double hyper_covariance[] = new double[dimension];

        // cluster info
        double[][] clusterMean = new double[num_cluster][dimension];
        double[][] clusterCovariance = new double[num_cluster][dimension];

        Random random = new Random();

        for (int i = 0; i < dimension; i++) {
            hyper_mean[i] = 0;
        }

        for (int i = 0; i < hyper_covariance.length; i++) {
            hyper_covariance[i] = 100;
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterMean[i][j] = random.nextGaussian() * 40;
            }
        }

        for (int i = 0; i < num_cluster; i++) {
            FileOperation.save_to_file(outFolder + "gaussian_simsql/", "readme", "Means "
                    + i + ":");
            for (int j = 0; j < dimension; j++) {
                FileOperation.save_to_file(outFolder + "gaussian_simsql/", "readme",
                        clusterMean[i][j] + " ");
            }
            FileOperation.save_to_file(outFolder + "gaussian_simsql/", "readme", "\n");
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterCovariance[i][j] = Math.abs(5 + 15 * Math.random());
            }
        }

        int membership;
        double dimesion_value;
        StringBuffer result = new StringBuffer("");
        for (int i = 0; i < num_data; i++) {
            membership = (int) (Math.random() * num_cluster);

            for (int j = 0; j < dimension; j++) {
                result.append(i + "|" + j + "|");

                dimesion_value = clusterMean[membership][j] + random.nextGaussian() * clusterCovariance[membership][j];

                result.append(dimesion_value + "|");

                if (j != dimension - 1 || i != num_data - 1)
                    result.append("\r\n");
            }

            if (i % 1000 == 0) {
                FileOperation.save_to_file(outFolder + "gaussian_simsql/", "DATA.tbl", result.toString());
                System.out.println("i = " + i);
                result = new StringBuffer("");
            }
        }

        result.append("\r\n");
        FileOperation.save_to_file(outFolder + "gaussian_simsql/", "DATA.tbl", result.toString());

        result = new StringBuffer("");
        for (int i = 0; i < num_cluster; i++) {
            result.append(i + "|1.0|");

            if (i != num_cluster - 1) {
                result.append("\r\n");
            }
        }

        FileOperation.save_to_file(outFolder + "gaussian_simsql/", "CLUSTER.tbl", result.toString());

        result = new StringBuffer("");
        result.append(dimension + "|" + num_data + "|" + num_cluster + "|");

        FileOperation.save_to_file(outFolder + "gaussian_simsql/", "META.tbl", result.toString());

    }

    public static void runGaussianGiraph(int num_machines,
                                         int num_data_per_machine,
                                         int num_cluster,
                                         int dimension,
                                         String outFolder) {
        int num_data = num_machines * num_data_per_machine;

        // hyper parameters
        double hyper_mean[] = new double[dimension];
        double hyper_covariance[] = new double[dimension];

        // cluster info
        double[][] clusterMean = new double[num_cluster][dimension];
        double[][] clusterCovariance = new double[num_cluster][dimension];

        Random random = new Random();

        for (int i = 0; i < dimension; i++) {
            hyper_mean[i] = 0;
        }

        for (int i = 0; i < hyper_covariance.length; i++) {
            hyper_covariance[i] = 100;
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterMean[i][j] = i * 40;
            }
        }

        for (int i = 0; i < num_cluster; i++) {
            FileOperation.save_to_file(outFolder + "gaussian_giraph/", "readme", "Means "
                    + i + ":");
            for (int j = 0; j < dimension; j++) {
                FileOperation.save_to_file(outFolder + "gaussian_giraph/", "readme",
                        clusterMean[i][j] + " ");
            }
            FileOperation.save_to_file(outFolder + "gaussian_giraph/", "readme", "\n");
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterCovariance[i][j] = Math.abs(5 + 15 * Math.random());
            }
        }

        int membership;
        double dimesion_value;
        StringBuffer result = new StringBuffer("");
        for (int i = 0; i < num_data; i++) {
            result.append(i + " ");

            membership = (int) (Math.random() * num_cluster);
            for (int j = 0; j < dimension; j++) {

                dimesion_value = clusterMean[membership][j]
                        + random.nextGaussian()
                        * clusterCovariance[membership][j];

                result.append(dimesion_value + " ");
            }
            result.append("; ");

            if ((i + 1) % 7 == 0) {
                result.append("\r\n");
                FileOperation.save_to_file(outFolder + "gaussian_giraph/", "DATA.tbl", result.toString());
                System.out.println("i = " + i);
                result = new StringBuffer("");
            }
        }

        result.append("\r\n");

        for (int i = 0; i < num_cluster; i++) {
            result.append("centrid " + (-i - 1) + "\r\n");
        }

        result.append("dirichlet_prior " + (-num_cluster - 1) + " 1");

        FileOperation.save_to_file(outFolder + "gaussian_giraph/", "DATA.tbl", result.toString());
    }

    public static void runGaussianSpark(int num_machines,
                                        int num_data_per_machine,
                                        int num_cluster,
                                        int dimension,
                                        String outFolder) {
        int num_data = num_machines * num_data_per_machine;

        // hyper parameters
        double hyper_mean[] = new double[dimension];
        double hyper_covariance[] = new double[dimension];

        // cluster info
        double[][] clusterMean = new double[num_cluster][dimension];
        double[][] clusterCovariance = new double[num_cluster][dimension];

        Random random = new Random();

        for (int i = 0; i < dimension; i++) {
            hyper_mean[i] = 0;
        }

        for (int i = 0; i < hyper_covariance.length; i++) {
            hyper_covariance[i] = 100;
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterMean[i][j] = random.nextGaussian() * 40;
            }
        }

        for (int i = 0; i < num_cluster; i++) {
            FileOperation.save_to_file(outFolder + "gaussian_spark/", "readme",
                    "Means " + i + ":");
            for (int j = 0; j < dimension; j++) {
                FileOperation.save_to_file(outFolder + "gaussian_spark/", "readme",
                        clusterMean[i][j] + " ");
            }
            FileOperation.save_to_file(outFolder + "gaussian_spark/", "readme", "\n");
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterCovariance[i][j] = Math.abs(5 + 15 * Math.random());
            }
        }

        int membership;
        double dimesion_value;
        StringBuffer result = new StringBuffer("");
        for (int i = 0; i < num_data; i++) {
            result.append(i + " ");

            membership = (int) (Math.random() * num_cluster);
            for (int j = 0; j < dimension; j++) {

                dimesion_value = clusterMean[membership][j]
                        + random.nextGaussian()
                        * clusterCovariance[membership][j];

                result.append(dimesion_value + " ");
            }
            result.append("\r\n");

            if (i % 1000 == 0) {
                FileOperation.save_to_file(outFolder + "gaussian_spark/", "DATA.tbl",
                        result.toString());
                //	System.out.println("i = " + i);
                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(outFolder + "gaussian_spark/", "DATA.tbl",
                result.toString());
    }

    public static void runGaussianGraphLab(int num_machines,
                                           int num_data_per_machine,
                                           int num_cluster,
                                           int dimension,
                                           String outFolder) {
        int num_data = num_machines * num_data_per_machine;

        // hyper parameters
        double hyper_mean[] = new double[dimension];
        double hyper_covariance[] = new double[dimension];

        // cluster info
        double[][] clusterMean = new double[num_cluster][dimension];
        double[][] clusterCovariance = new double[num_cluster][dimension];

        Random random = new Random();

        for (int i = 0; i < dimension; i++) {
            hyper_mean[i] = 0;
        }

        for (int i = 0; i < hyper_covariance.length; i++) {
            hyper_covariance[i] = 100;
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterMean[i][j] = random.nextGaussian() * 40;
            }
        }

        for (int i = 0; i < num_cluster; i++) {
            FileOperation.save_to_file(outFolder + "gaussian_graphlab/", "readme",
                    "Means " + i + ":");
            for (int j = 0; j < dimension; j++) {
                FileOperation.save_to_file(outFolder + "gaussian_graphlab/", "readme",
                        clusterMean[i][j] + " ");
            }
            FileOperation.save_to_file(outFolder + "gaussian_graphlab/", "readme", "\n");
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterCovariance[i][j] = Math.abs(5 + 15 * Math.random());
            }
        }

        int membership;
        double dimesion_value;
        StringBuffer result = new StringBuffer("");
        for (int i = 0; i < num_data; i++) {
            membership = (int) (Math.random() * num_cluster);
            for (int j = 0; j < dimension; j++) {

                dimesion_value = clusterMean[membership][j]
                        + random.nextGaussian()
                        * clusterCovariance[membership][j];

                result.append(dimesion_value + " ");
            }
            result.append("\r\n");

            if (i % 1000 == 0) {
                FileOperation.save_to_file(outFolder + "gaussian_graphlab/", "DATA.tbl",
                        result.toString());
                System.out.println("i = " + i);
                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(outFolder + "gaussian_graphlab/", "DATA.tbl",
                result.toString());
    }

    public static void runGaussianImpGiraph(int num_machines,
                                            int num_data_per_machine,
                                            int num_cluster,
                                            int dimension,
                                            double missingfraction,
                                            String outFolder) {
        int num_data = num_machines * num_data_per_machine;
        double hyper_mean[] = new double[dimension];
        double hyper_covariance[] = new double[dimension];

        // cluster info
        double[][] clusterMean = new double[num_cluster][dimension];
        double[][] clusterCovariance = new double[num_cluster][dimension];

        Random random = new Random();

        for (int i = 0; i < dimension; i++) {
            hyper_mean[i] = 0;
        }

        for (int i = 0; i < hyper_covariance.length; i++) {
            hyper_covariance[i] = 100;
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterMean[i][j] = random.nextGaussian() * 40;
            }
        }

        StringBuffer result = new StringBuffer("");

        for (int i = 0; i < num_cluster; i++) {
            result.append("Means " + i + ":");

            for (int j = 0; j < dimension; j++) {
                result.append(clusterMean[i][j] + " ");
            }

            result.append("\r\n");
        }


        FileOperation.save_to_file(outFolder + "gau_im_giraph/", "readme", result.toString());

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterCovariance[i][j] = Math.abs(5 + 15 * Math.random());
            }
        }

        int membership;
        double dimesion_value;
        result = new StringBuffer("");
        ArrayList<Integer> emptyList = new ArrayList<Integer>();
        for (int i = 0; i < num_data; i++) {
            membership = (int) (Math.random() * num_cluster);
            result.append(i + " ");
            emptyList.clear();
            double beta = 1;
            double alpha = 1 / (1 - missingfraction) - 1;
            double realImputation = new Beta(alpha, beta).random();

            for (int j = 0; j < dimension; j++) {
                if (Math.random() > realImputation || emptyList.size() >= dimension - 1) //not all the values are missing
                {
                    dimesion_value = clusterMean[membership][j]
                            + random.nextGaussian()
                            * clusterCovariance[membership][j];
                } else {
                    emptyList.add(j);
                    dimesion_value = -1;
                }

                result.append(dimesion_value + " ");
            }

            result.append(" : ");

            for (int j = 0; j < emptyList.size(); j++) {
                result.append(emptyList.get(j) + " ");
            }

            result.append("\r\n");

            if (i % 1000 == 0) {
                FileOperation.save_to_file(
                        outFolder + "gau_im_giraph/",
                        "DATA_imp.tbl", result.toString());
                System.out.println(i);

                result = new StringBuffer("");
            }
        }

        for (int i = 0; i < num_cluster; i++) {
            result.append("centrid " + (-i - 1) + "\r\n");
        }

        result.append("dirichlet_prior " + (-num_cluster - 1) + " 1");
        FileOperation.save_to_file(
                outFolder + "gau_im_giraph/",
                "DATA_imp.tbl", result.toString());
    }


    public static void runGaussianImpSpark(int num_machines,
                                           int num_data_per_machine,
                                           int num_cluster,
                                           int dimension,
                                           double missingfraction,
                                           String outFolder) {
        int num_data = num_machines * num_data_per_machine;
        double hyper_mean[] = new double[dimension];
        double hyper_covariance[] = new double[dimension];

        // cluster info
        double[][] clusterMean = new double[num_cluster][dimension];
        double[][] clusterCovariance = new double[num_cluster][dimension];

        Random random = new Random();

        for (int i = 0; i < dimension; i++) {
            hyper_mean[i] = 0;
        }

        for (int i = 0; i < hyper_covariance.length; i++) {
            hyper_covariance[i] = 100;
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterMean[i][j] = random.nextGaussian() * 40;
            }
        }

        StringBuffer result = new StringBuffer("");

        for (int i = 0; i < num_cluster; i++) {
            result.append("Means " + i + ":");

            for (int j = 0; j < dimension; j++) {
                result.append(clusterMean[i][j] + " ");
            }

            result.append("\r\n");
        }


        FileOperation.save_to_file(outFolder + "gau_im_spark/", "readme", result.toString());

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterCovariance[i][j] = Math.abs(5 + 15 * Math.random());
            }
        }

        int membership;
        double dimesion_value;
        result = new StringBuffer("");
        ArrayList<Integer> emptyList = new ArrayList<Integer>();

        for (int i = 0; i < num_data; i++) {
            membership = (int) (Math.random() * num_cluster);

            result.append(i + " ");
            emptyList.clear();
            double beta = 1;
            double alpha = 1 / (1 - missingfraction) - 1;
            double realImputation = new Beta(alpha, beta).random();


            for (int j = 0; j < dimension; j++) {
                if (Math.random() > realImputation || emptyList.size() >= dimension - 1) {
                    dimesion_value = clusterMean[membership][j]
                            + random.nextGaussian()
                            * clusterCovariance[membership][j];
                } else {
                    emptyList.add(j);
                    dimesion_value = -1;
                }

                result.append(dimesion_value + " ");
            }

            result.append(" : ");

            for (int j = 0; j < emptyList.size(); j++) {
                result.append(emptyList.get(j) + " ");
            }

            result.append("\r\n");

            if (i % 1000 == 0) {
                FileOperation.save_to_file(
                        outFolder + "gau_im_spark/",
                        "DATA_imp.tbl", result.toString());
                //		System.out.println(i);

                result = new StringBuffer("");
            }

            if (i % 10000000 == 0)
                System.out.println(i);
        }

        FileOperation.save_to_file(
                outFolder + "gau_im_spark/",
                "DATA_imp.tbl", result.toString());
    }

    public static void runGaussianImpGraphLab(int num_machines,
                                              int num_data_per_machine,
                                              int num_cluster,
                                              int dimension,
                                              double missingfraction,
                                              String outFolder) {
        int num_data = num_machines * num_data_per_machine;
        double hyper_mean[] = new double[dimension];
        double hyper_covariance[] = new double[dimension];

        // cluster info
        double[][] clusterMean = new double[num_cluster][dimension];
        double[][] clusterCovariance = new double[num_cluster][dimension];

        Random random = new Random();

        for (int i = 0; i < dimension; i++) {
            hyper_mean[i] = 0;
        }

        for (int i = 0; i < hyper_covariance.length; i++) {
            hyper_covariance[i] = 100;
        }

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterMean[i][j] = random.nextGaussian() * 40;
            }
        }

        StringBuffer result = new StringBuffer("");

        for (int i = 0; i < num_cluster; i++) {
            result.append("Means " + i + ":");

            for (int j = 0; j < dimension; j++) {
                result.append(clusterMean[i][j] + " ");
            }

            result.append("\r\n");
        }


        FileOperation.save_to_file(outFolder + "gau_im_graphlab/", "readme", result.toString());

        for (int i = 0; i < num_cluster; i++) {
            for (int j = 0; j < dimension; j++) {
                clusterCovariance[i][j] = Math.abs(5 + 15 * Math.random());
            }
        }

        int membership;
        double dimesion_value;
        result = new StringBuffer("");
        ArrayList<Integer> emptyList = new ArrayList<Integer>();

        for (int i = 0; i < num_data; i++) {
            membership = (int) (Math.random() * num_cluster);
            emptyList.clear();
            double beta = 1;
            double alpha = 1 / (1 - missingfraction) - 1;
            double realImputation = new Beta(alpha, beta).random();


            for (int j = 0; j < dimension; j++) {
                if (Math.random() > realImputation || emptyList.size() >= dimension - 1) {
                    dimesion_value = clusterMean[membership][j]
                            + random.nextGaussian()
                            * clusterCovariance[membership][j];
                } else {
                    emptyList.add(j);
                    dimesion_value = -1;
                }

                result.append(dimesion_value + " ");
            }

            result.append(emptyList.size() + " ");
            for (int j = 0; j < emptyList.size(); j++) {
                result.append(emptyList.get(j) + " ");
            }

            result.append("\r\n");

            if (i % 1000 == 0) {
                FileOperation.save_to_file(
                        outFolder + "gau_im_graphlab/",
                        "DATA_imp.tbl", result.toString());
                System.out.println(i);

                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(
                outFolder + "gau_im_graphlab/",
                "DATA_imp.tbl", result.toString());
    }


    public static void runLasso() {
        int num_machines = 5;
        int num_data = num_machines * 1000;
        int dimension = 20;

        Random random = new Random();
        double[] coefficients = new double[dimension];
        for (int i = 0; i < coefficients.length; i++) {
            if (i % 2 == 1)
                coefficients[i] = 0.01;
            else
                coefficients[i] = i / (double) 2 + 1;
        }

        StringBuffer result = new StringBuffer();
        double y, dimensionValue;
        for (int i = 0; i < num_data; i++) {
            result.append(i + " ");
            y = 0;
            for (int j = 0; j < dimension; j++) {
                dimensionValue = (Math.random() - 0.5) * 100;
                result.append(dimensionValue + " ");
                y += dimensionValue * coefficients[j];
            }

            y = y + random.nextGaussian() * 2;

            result.append(y + "\r\n");
        }

        for (int i = 0; i < dimension; i++) {
            result.append("dimension " + (-i - 1) + "\r\n");
        }
        result.append("model " + (-dimension - 1) + " 1 1");

        FileOperation.save_to_file(folder + "lasso/", "DATA.tbl",
                result.toString());
    }

    public static void runLassoSpark(int num_machines,
                                     int num_data_per_machine,
                                     int dimension,
                                     String outFolder) {
        int num_data = num_machines * num_data_per_machine;
        Random random = new Random();
        double[] coefficients = new double[dimension];
        for (int i = 0; i < coefficients.length; i++) {
            if (i % 2 == 1)
                coefficients[i] = 0.01;
            else
                coefficients[i] = Math.random() * 40;
        }

        StringBuffer result = new StringBuffer();
        double y, dimensionValue;
        for (int i = 0; i < num_data; i++) {
            result.append(i + " ");
            y = 0;
            for (int j = 0; j < dimension; j++) {
                dimensionValue = (Math.random() - 0.5) * 100;
                result.append(dimensionValue + " ");
                y += dimensionValue * coefficients[j];
            }

            y = y + random.nextGaussian() * 2;

            result.append(y + "\r\n");

            if (i % 1000 == 0) {
                FileOperation.save_to_file(outFolder + "lasso/", "DATA.tbl", result.toString());
                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(outFolder + "lasso/", "DATA.tbl",
                result.toString());

        result = new StringBuffer("");
        for (int j = 0; j < dimension; j++) {
            result.append(coefficients[j] + "\r\n");
        }

        FileOperation.save_to_file(outFolder + "lasso/", "coefficients.tbl", result.toString());
    }

    public static void runLassoGiraph(int num_machines,
                                      int num_data_per_machine,
                                      int dimension,
                                      String outFolder) {
        int num_data = num_machines * num_data_per_machine;
        Random random = new Random();
        double[] coefficients = new double[dimension];
        for (int i = 0; i < coefficients.length; i++) {
            if (i % 2 == 1)
                coefficients[i] = 0.01;
            else
                coefficients[i] = Math.random() * 40;
        }

        StringBuffer result = new StringBuffer();
        double y, dimensionValue;
        for (int i = 0; i < num_data; i++) {
            result.append(i + " ");
            y = 0;
            for (int j = 0; j < dimension; j++) {
                dimensionValue = (Math.random() - 0.5) * 100;
                result.append(dimensionValue + " ");
                y += dimensionValue * coefficients[j];
            }

            y = y + random.nextGaussian() * 2;

            result.append(y + "\r\n");

            if (i % 1000 == 0) {
                FileOperation.save_to_file(outFolder + "lasso/", "DATA.tbl", result.toString());
                System.out.println(i + "");
                result = new StringBuffer("");
            }
        }

        for (int i = 0; i < dimension; i++) {
            result.append("dimension " + (-i - 1) + "\r\n");
        }
        result.append("model " + (-dimension - 1) + " 1 1");

        FileOperation.save_to_file(outFolder + "lasso/", "DATA.tbl",
                result.toString());

        result = new StringBuffer("");
        for (int j = 0; j < dimension; j++) {
            result.append(coefficients[j] + "\r\n");
        }

        FileOperation.save_to_file(outFolder + "lasso/", "coefficients.tbl",
                result.toString());
    }

    public static void runBlockLasso(int num_machines,
                                     int num_data_per_machine,
                                     int num_perline,
                                     int dimension,
                                     String outFolder) {
        int num_data = num_machines * num_data_per_machine;

        Random random = new Random();
        double[] coefficients = new double[dimension];
        for (int i = 0; i < coefficients.length; i++) {
            if (i % 2 == 1)
                coefficients[i] = 0.01;
            else
                coefficients[i] = Math.random() * 40;
        }

        StringBuffer result = new StringBuffer();
        double y, dimensionValue;
        for (int i = 0; i < num_data; i++) {
            result.append(i + " ");
            y = 0;
            for (int j = 0; j < dimension; j++) {
                dimensionValue = (Math.random() - 0.5) * 100;
                result.append(dimensionValue + " ");
                y += dimensionValue * coefficients[j];
            }

            y = y + random.nextGaussian() * 2;

            if (i != num_data - 1) {
                if ((i + 1) % num_perline == 0)
                    result.append(y + "\r\n");
                else
                    result.append(y + ";  ");
            } else {
                result.append(y + "\r\n");
            }

            if (i % 1000 == 0) {
                System.out.println("i = " + i);
                FileOperation.save_to_file(outFolder + "block_lasso/", "DATA.tbl", result.toString());
                result = new StringBuffer("");
            }
        }

        for (int i = 0; i < dimension; i++) {
            result.append("dimension " + (-i - 1) + "\r\n");
        }
        result.append("model " + (-dimension - 1) + " 1 1");

        FileOperation.save_to_file(outFolder + "block_lasso/", "DATA.tbl", result.toString());
    }

    public static void runLassoGraphLab(int num_machines,
                                        int num_data_per_machine,
                                        int dimension,
                                        String outFolder) {
        int num_data = num_machines * num_data_per_machine;
        Random random = new Random();
        double[] coefficients = new double[dimension];
        for (int i = 0; i < coefficients.length; i++) {
            if (i % 2 == 1)
                coefficients[i] = 0.01;
            else
                coefficients[i] = Math.random() * 40;
        }

        StringBuffer result = new StringBuffer();
        double y, dimensionValue;
        for (int i = 0; i < num_data; i++) {
            y = 0;
            for (int j = 0; j < dimension; j++) {
                dimensionValue = (Math.random() - 0.5) * 100;
                result.append(dimensionValue + " ");
                y += dimensionValue * coefficients[j];
            }

            y = y + random.nextGaussian() * 2;

            result.append(y + "\r\n");

            if (i % 1000 == 0) {
                FileOperation.save_to_file(outFolder + "lasso/", "DATA.tbl", result.toString());
                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(outFolder + "lasso/", "DATA.tbl",
                result.toString());

        result = new StringBuffer("");
        for (int j = 0; j < dimension; j++) {
            result.append(coefficients[j] + "\r\n");
        }

        FileOperation.save_to_file(outFolder + "lasso/", "coefficients.tbl", result.toString());
    }

    public static void runLDAGiraph(int num_machines,
                                    int num_docs_per_machine,
                                    int topicNum,
                                    String sourceFolder,
                                    String outFolder) {
        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 16923;

        ArrayList<TreeMap<Integer, Integer>> docSet = new ArrayList<TreeMap<Integer, Integer>>(
                numBasicDoc);
        String file;
        for (int i = 0; i < numBasicDoc; i++) {
            file = sourceFolder + i + ".txt";
            docSet.add(getWordCountMap(file));
            System.out.println("loading files " + i);
        }

        TreeMap<Integer, Integer> generatedDoc;
        int unionIndex1, unionIndex2;
//        String tempString;

        StringBuffer result = new StringBuffer("");
        Random rng = new Random(12345); // added by L.

        int doc_per_block = 6250;
        int block_num = numDoc / doc_per_block;

        for (int i = 0; i < block_num; i++) {
            result = new StringBuffer("");
            for (int j = 0; j < doc_per_block; j++) {
                result.append((i * doc_per_block + j) + " ");

                if (i * doc_per_block + j < numBasicDoc)
                    generatedDoc = docSet.get(i * doc_per_block + j);
                else {
                    unionIndex1 = rng.nextInt(numBasicDoc);
                    unionIndex2 = rng.nextInt(numBasicDoc);

                    generatedDoc = union(docSet.get(unionIndex1), docSet.get(unionIndex2));
                }

                for (Integer o : generatedDoc.keySet()) {
                    result.append(o + " " + generatedDoc.get(o) + " ");
                }
                result.append("; ");
            }
            result.append("\r\n");
            FileOperation.save_to_file(outFolder + "lda/", "WORD_IN_DOC.tbl", result.toString());
            System.out.println("i = " + i);
        }

        result = new StringBuffer("");

        for (int i = 0; i < topicNum; i++) {
            result.append("topic " + (-(i + 1)));
            if (i != topicNum - 1)
                result.append("\r\n");
        }

        FileOperation.save_to_file(outFolder + "lda/", "WORD_IN_DOC.tbl", result.toString());
    }

    public static void runLDASpark(int num_machines,
                                   int num_docs_per_machine,
                                   int topicNum,
                                   String sourceFolder,
                                   String outFolder) {

//        num_docs_per_machine = 1000 * 1000 / num_machines;


        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 16923;

        System.out.println("total docs:\t" + (numDoc));
        System.out.println("docs per executor:\t" + (num_docs_per_machine));

        ArrayList<TreeMap<Integer, Integer>> docSet = new ArrayList<TreeMap<Integer, Integer>>(
                numBasicDoc);
        String file;
        for (int i = 0; i < numBasicDoc; i++) {
            file = sourceFolder + i + ".txt";
            docSet.add(getWordCountMap(file));
            if (i % 1000 == 0)
                System.out.println("loading files " + i);
        }

        TreeMap<Integer, Integer> generatedDoc;
        int unionIndex1, unionIndex2;
        String tempString;

        StringBuffer result = new StringBuffer("");
        Random rng = new Random(12345); // added by L.

        String outputFilename = "WORD_IN_DOC" + "_" + num_machines + ".tbl";
        for (int i = 0; i < numDoc; i++) {
            result.append(i + " ");

            if (i < numBasicDoc)
                generatedDoc = docSet.get(i);
            else {
                unionIndex1 = rng.nextInt(numBasicDoc);
                unionIndex2 = rng.nextInt(numBasicDoc);

                generatedDoc = union(docSet.get(unionIndex1), docSet.get(unionIndex2));
            }

            for (Integer o : generatedDoc.keySet()) {
                result.append(o + " " + generatedDoc.get(o) + " ");
            }

            result.append("\r\n");

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "lda/", outputFilename, result.toString());
                result = new StringBuffer("");
                //	System.out.println("i = " + i);
            }
        }

/*		for(int i = 0; i < topicNum; i++)
        {
			result.append("topic " + (-(i+1)));
			if( i != topicNum-1)
				result.append("\r\n");
		}
		*/
        FileOperation.save_to_file(outFolder + "lda/", outputFilename, result.toString());
    }


    public static void runLDAGraphLab(int num_machines,
                                      int num_docs_per_machine,
                                      int topicNum,
                                      String sourceFolder,
                                      String outFolder) {
        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 16923;

        ArrayList<TreeMap<Integer, Integer>> docSet = new ArrayList<TreeMap<Integer, Integer>>(
                numBasicDoc);
        String file;
        for (int i = 0; i < numBasicDoc; i++) {
            file = sourceFolder + i + ".txt";
            docSet.add(getWordCountMap(file));
            System.out.println("loading files " + i);
        }

        TreeMap<Integer, Integer> generatedDoc;
        int unionIndex1, unionIndex2;
        String tempString;

        StringBuffer result = new StringBuffer("");
        Random rng = new Random(12345); // added by L.

        for (int i = 0; i < numDoc; i++) {
            if (i < numBasicDoc)
                generatedDoc = docSet.get(i);
            else {
                unionIndex1 = rng.nextInt(numBasicDoc);
                unionIndex2 = rng.nextInt(numBasicDoc);

                generatedDoc = union(docSet.get(unionIndex1), docSet.get(unionIndex2));
            }

            result.append(generatedDoc.size() + " ");
            for (Integer o : generatedDoc.keySet()) {
                result.append(o + " " + generatedDoc.get(o) + " ");
            }

            result.append("\r\n");

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "lda/", "WORD_IN_DOC.tbl", result.toString());
                result = new StringBuffer("");
                System.out.println("i = " + i);
            }
        }

        FileOperation.save_to_file(outFolder + "lda/", "WORD_IN_DOC.tbl", result.toString());
    }

    public static void runLDASimsql(int num_machines,
                                    int num_docs_per_machine,
                                    int topicNum,
                                    String sourceFolder,
                                    String outFolder) {

        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 16923;

        ArrayList<TreeMap<Integer, Integer>> docSet = new ArrayList<TreeMap<Integer, Integer>>(
                numBasicDoc);
        String file;

        // get the PRIOR table
        FileOperation.save_to_file(outFolder + "lda/", "PRIOR.tbl", "1.0|1.0|\n");
        StringBuffer result = new StringBuffer("");

        // get the DOCS table.
        for (int i = 0; i < numDoc; i++) {
            result.append(i + "|\n");
            file = sourceFolder + i + ".txt";

            if (i < numBasicDoc) {
                docSet.add(getWordCountMap(file));
                System.out.println("loading files " + i);
            }

            if ((i % 1000) == 0) {
                FileOperation.save_to_file(outFolder + "lda/", "DOCS.tbl", result.toString());
                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(outFolder + "lda/", "DOCS.tbl", result.toString());
        result = new StringBuffer("");

        // get the TOPICS table
        for (int i = 0; i < topicNum; i++) {
            result.append(i + "|\n");

            if ((i % 1000) == 0) {
                FileOperation.save_to_file(outFolder + "lda/", "TOPICS.tbl", result.toString());
                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(outFolder + "lda/", "TOPICS.tbl", result.toString());
        result = new StringBuffer("");

        TreeMap<Integer, Integer> generatedDoc;
        int unionIndex1, unionIndex2;
        String tempString;

        int numWords = 0;
        Random rng = new Random(12345); //added by L.

        // get the WORD_IN_DOC table.
        for (int i = 0; i < numDoc; i++) {
            if (i < numBasicDoc)
                generatedDoc = docSet.get(i);
            else {
                unionIndex1 = rng.nextInt(numBasicDoc);
                unionIndex2 = rng.nextInt(numBasicDoc);

                generatedDoc = union(docSet.get(unionIndex1), docSet.get(unionIndex2));
            }

            for (Integer o : generatedDoc.keySet()) {
                if (o > numWords) {
                    numWords = o;
                }

                result.append(i + "|" + o + "|" + generatedDoc.get(o) + "|\n");
            }

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "lda/", "WORD_IN_DOC.tbl", result.toString());
                result = new StringBuffer("");
                System.out.println("i = " + i);
            }
        }

        FileOperation.save_to_file(outFolder + "lda/", "WORD_IN_DOC.tbl", result.toString());

        result = new StringBuffer("");
        for (int i = 0; i <= numWords; i++) {
            result.append(i + "|\n");

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "lda/", "WORDS.tbl", result.toString());
                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(outFolder + "lda/", "WORDS.tbl", result.toString());

    }

    public static void runLDASimsql2(int num_machines,
                                     int num_docs_per_machine,
                                     int topicNum,
                                     String sourceFolder,
                                     String outFolder) {

        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 16923;

        int numDocsPerGroup = 10000;

        ArrayList<TreeMap<Integer, Integer>> docSet = new ArrayList<TreeMap<Integer, Integer>>(
                numBasicDoc);
        String file;

        // get the PRIOR table
        FileOperation.save_to_file(outFolder + "lda/", "PRIOR.tbl", "1.0|1.0|\n");
        StringBuffer result = new StringBuffer("");

        // get the DOCS table.
        int curDoc = 0;
        int curGroup = 0;

        for (int i = 0; i < numDoc; i++) {
            result.append(curGroup + "|" + curDoc + "|\n");
            file = sourceFolder + i + ".txt";

            if (i < numBasicDoc) {
                docSet.add(getWordCountMap(file));
                System.out.println("loading files " + i);
            }

            if ((i % 1000) == 0) {
                FileOperation.save_to_file(outFolder + "lda/", "DOCS.tbl", result.toString());
                result = new StringBuffer("");
            }

            curDoc++;
            if (curDoc >= numDocsPerGroup) {
                curGroup++;
                curDoc = 0;
            }
        }

        FileOperation.save_to_file(outFolder + "lda/", "DOCS.tbl", result.toString());
        result = new StringBuffer("");

        // get the GROUPS table
        for (int i = 0; i < curGroup; i++) {
            result.append(i + "|\n");
        }

        FileOperation.save_to_file(outFolder + "lda/", "GROUPS.tbl", result.toString());
        result = new StringBuffer("");

        // get the TOPICS table
        for (int i = 0; i < topicNum; i++) {
            result.append(i + "|\n");

            if ((i % 1000) == 0) {
                FileOperation.save_to_file(outFolder + "lda/", "TOPICS.tbl", result.toString());
                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(outFolder + "lda/", "TOPICS.tbl", result.toString());
        result = new StringBuffer("");

        TreeMap<Integer, Integer> generatedDoc;
        int unionIndex1, unionIndex2;
        String tempString;

        int numWords = 0;
        Random rng = new Random(12345); //added by L.

        // get the WORD_IN_DOC table.
        curDoc = 0;
        curGroup = 0;
        for (int i = 0; i < numDoc; i++) {
            if (i < numBasicDoc)
                generatedDoc = docSet.get(i);
            else {
                unionIndex1 = rng.nextInt(numBasicDoc);
                unionIndex2 = rng.nextInt(numBasicDoc);

                generatedDoc = union(docSet.get(unionIndex1), docSet.get(unionIndex2));
            }

            for (Integer o : generatedDoc.keySet()) {
                if (o > numWords) {
                    numWords = o;
                }

                result.append(curGroup + "|" + curDoc + "|" + o + "|" + generatedDoc.get(o) + "|\n");
            }

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "lda/", "WORD_IN_DOC.tbl", result.toString());
                result = new StringBuffer("");
                System.out.println("i = " + i);
            }

            curDoc++;
            if (curDoc >= numDocsPerGroup) {
                curGroup++;
                curDoc = 0;
            }
        }

        FileOperation.save_to_file(outFolder + "lda/", "WORD_IN_DOC.tbl", result.toString());

        result = new StringBuffer("");
        for (int i = 0; i <= numWords; i++) {
            result.append(i + "|\n");

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "lda/", "WORDS.tbl", result.toString());
                result = new StringBuffer("");
            }
        }

        FileOperation.save_to_file(outFolder + "lda/", "WORDS.tbl", result.toString());

    }

    public static void runHMMSimsql(int num_machines,
                                    int num_docs_per_machine,
                                    int topicNum,
                                    String sourceFolder,
                                    String outFolder) {


        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 20003;

        // number of words
        int numWords = 10000;

        // dummy word for padding odd-length docs.
        final int dummyWord = numWords;

        // END word for marking the end of a document.
        final int endWord = numWords + 1;

        // first, create transition_prior
        StringBuffer buf = new StringBuffer("");
        for (int i = 0; i < topicNum; i++) {
            buf.append(i);
            buf.append("|1.0|\n");
        }

        FileOperation.save_to_file(outFolder + "hmm/", "transition_prior.tbl", buf.toString());

        // now, create word_prior with two additional words (for padding odd-sized docs and marking the end)
        buf.setLength(0);
        for (int i = 0; i < numWords; i++) {
            buf.append(i);
            buf.append("|1.0|\n");
        }

        // the two additional words.
        buf.append(dummyWord);
        buf.append("|0.01|\n");
        buf.append(endWord);
        buf.append("|0.01|\n");

        FileOperation.save_to_file(outFolder + "hmm/", "word_prior.tbl", buf.toString());
        buf.setLength(0);

        // read the documents
        ArrayList<ArrayList<Integer>> docSet = new ArrayList<ArrayList<Integer>>(numBasicDoc);
        String file;
        for (int i = 0; i < numBasicDoc; i++) {
            file = sourceFolder + i + ".txt";
            docSet.add(getWordSequenceMap(file));
            System.out.println("Loading file " + i);
        }

        // now, we create the big table.
        final int flushBufSize = 1024 * 1024 * 2;
        final long passFileSize = 1024L * 1024L * 1024L * 20;
        Random rng = new Random(12345); // added by L.
        int unionIndex1, unionIndex2;
        int curFile = 0;
        long curFileSize = 0;

        ArrayList<Integer> emptyDoc = new ArrayList<Integer>();
        ArrayList<Integer> generatedDoc1 = null;
        ArrayList<Integer> generatedDoc2 = null;
        for (int i = 0; i < numDoc; i++) {

            if (i < numBasicDoc) {
                generatedDoc1 = docSet.get(i);
                generatedDoc2 = emptyDoc;
            } else {
                unionIndex1 = rng.nextInt(numBasicDoc);
                unionIndex2 = rng.nextInt(numBasicDoc);
                generatedDoc1 = docSet.get(unionIndex1);
                generatedDoc2 = docSet.get(unionIndex2);
            }

            int j = 0;

            // add the first doc.
            for (Integer o : generatedDoc1) {

                buf.append(i);
                buf.append("|");
                buf.append(j);
                buf.append("|");
                buf.append(o);
                buf.append("|\n");
                j++;
            }

            // add the second doc.
            for (Integer o : generatedDoc2) {
                buf.append(i);
                buf.append("|");
                buf.append(j);
                buf.append("|");
                buf.append(o);
                buf.append("|\n");
                j++;
            }

            // before adding the end word, pad documents to make them even.
            if ((j % 2) == 0) {
                buf.append(i);
                buf.append("|");
                buf.append(j);
                buf.append("|");
                buf.append(dummyWord);
                buf.append("|\n");
                j++;
            }

            // and add the end word.
            buf.append(i);
            buf.append("|");
            buf.append(j);
            buf.append("|");
            buf.append(endWord);
            buf.append("|\n");

            // see if we must flush.
            if (buf.length() >= flushBufSize) {

                FileOperation.save_to_file(outFolder + "hmm/", "words" + curFile + ".tbl", buf.toString());
                curFileSize += buf.length();
                buf.setLength(0);

                if (curFileSize >= passFileSize) {
                    curFileSize = 0;
                    curFile++;
                }
            }
        }

        // write out the remainder.
        FileOperation.save_to_file(outFolder + "hmm/", "words" + curFile + ".tbl", buf.toString());
    }


    public static void runHMMGiraph(int num_machines,
                                    int num_docs_per_machine,
                                    int topicNum,
                                    String sourceFolder,
                                    String outFolder) {
        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 20003;

        ArrayList<ArrayList<Integer>> docSet = new ArrayList<ArrayList<Integer>>(numBasicDoc);
        String file;
        for (int i = 0; i < numBasicDoc; i++) {
            file = sourceFolder + i + ".txt";
            docSet.add(getWordSequenceMap(file));
            System.out.println("loading files " + i);
        }

        ArrayList<Integer> generatedDoc;
        int unionIndex1, unionIndex2;
        String tempString;
        StringBuffer result = new StringBuffer("");
        Random rng = new Random(12345); // added by L.

        for (int i = 0; i < numDoc; i++) {
            result.append(i + " ");

            if (i < numBasicDoc)
                generatedDoc = docSet.get(i);
            else {
                unionIndex1 = rng.nextInt(numBasicDoc);
                unionIndex2 = rng.nextInt(numBasicDoc);

                generatedDoc = new ArrayList<Integer>();
                generatedDoc.addAll(docSet.get(unionIndex1));
                generatedDoc.addAll(docSet.get(unionIndex2));
            }

            for (Integer o : generatedDoc) {
                result.append(o + " ");
            }

            result.append("\r\n");

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl", result.toString());
                result = new StringBuffer("");
                System.out.println("i = " + i);
            }
        }

        for (int i = 0; i < topicNum; i++) {
            if (i != topicNum - 1)
                result.append("topic " + (-(i + 1)) + "\r\n");
            else
                result.append("topic " + (-(i + 1)));
        }

        FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl", result.toString());
    }

    public static void runHMMGiraph3(int num_machines,
                                     int num_blocks_per_machine,
                                     int doc_per_block,
                                     int topicNum,
                                     String sourceFolder,
                                     String outFolder) {
        int numBasicDoc = 20003;

        ArrayList<ArrayList<Integer>> docSet = new ArrayList<ArrayList<Integer>>(
                numBasicDoc);
        String file;
        for (int i = 0; i < numBasicDoc; i++) {
            file = sourceFolder + i + ".txt";
            docSet.add(getWordSequenceMap(file));
            System.out.println("loading files " + i);
        }

        ArrayList<Integer> generatedDoc;
        int unionIndex1, unionIndex2;
        StringBuffer result = new StringBuffer("");
        Random rng = new Random(12345); // added by L.

        for (int i = 0; i < num_machines * num_blocks_per_machine; i++) {
            result.append(i + " ");

            for (int j = 0; j < doc_per_block; j++) {
                if (i * doc_per_block + j < numBasicDoc)
                    generatedDoc = docSet.get(i);
                else {
                    unionIndex1 = rng.nextInt(numBasicDoc);
                    unionIndex2 = rng.nextInt(numBasicDoc);

                    generatedDoc = new ArrayList<Integer>();
                    generatedDoc.addAll(docSet.get(unionIndex1));
                    generatedDoc.addAll(docSet.get(unionIndex2));
                }

                for (Integer o : generatedDoc) {
                    result.append(o + " ");
                }
            }

            result.append("\r\n");
            FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl", result.toString());
            result = new StringBuffer("");
            System.out.println("i = " + i);
        }

        for (int i = 0; i < topicNum; i++) {
            if (i != topicNum - 1)
                result.append("topic " + (-(i + 1)) + "\r\n");
            else
                result.append("topic " + (-(i + 1)));
        }

        FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl",
                result.toString());
    }

    public static void runHMMGiraph2(int num_machines,
                                     int num_docs_per_machine,
                                     int topicNum,
                                     String sourceFolder,
                                     String outFolder) {
        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 20003;

        ArrayList<ArrayList<Integer>> docSet = new ArrayList<ArrayList<Integer>>(numBasicDoc);
        String file;
        for (int i = 0; i < numBasicDoc; i++) {
            file = sourceFolder + i + ".txt";
            docSet.add(getWordSequenceMap(file));
            System.out.println("loading files " + i);
        }

        ArrayList<Integer> generatedDoc;
        int unionIndex1, unionIndex2;
        String tempString;

        StringBuffer result = new StringBuffer("");
        Random rng = new Random(12345); // added by L

        for (int i = 0; i < numDoc; i++) {

            if (i < numBasicDoc)
                generatedDoc = docSet.get(i);
            else {
                unionIndex1 = rng.nextInt(numBasicDoc);
                unionIndex2 = rng.nextInt(numBasicDoc);

                generatedDoc = new ArrayList<Integer>();
                generatedDoc.addAll(docSet.get(unionIndex1));
                generatedDoc.addAll(docSet.get(unionIndex2));
            }

            for (int j = 0; j < generatedDoc.size(); j++) {
                if (j + 1 >= generatedDoc.size())
                    result.append(i + " " + j + " " + generatedDoc.get(j) + " " + (-1) + "\r\n");
                else
                    result.append(i + " " + j + " " + generatedDoc.get(j) + " " + (j + 1) + "\r\n");
            }

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl", result.toString());
                result = new StringBuffer("");
                System.out.println("i = " + i);
            }
        }

        for (int i = 0; i < topicNum; i++) {
            if (i != topicNum - 1)
                result.append("topic " + (-(i + 1)) + "\r\n");
            else
                result.append("topic " + (-(i + 1)));
        }

        FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl", result.toString());
    }

    public static void runHMMSpark(int num_machines,
                                   int num_docs_per_machine,
                                   int topicNum,
                                   String sourceFolder,
                                   String outFolder) {
        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 20003;

        ArrayList<ArrayList<Integer>> docSet = new ArrayList<ArrayList<Integer>>(numBasicDoc);
        String file;
        for (int i = 0; i < numBasicDoc; i++) {
            file = sourceFolder + i + ".txt";
            docSet.add(getWordSequenceMap(file));
            if (i % 1000 == 0)
                System.out.println("loading files " + i);
        }

        ArrayList<Integer> generatedDoc;
        int unionIndex1, unionIndex2;
        String tempString;

        StringBuffer result = new StringBuffer("");
        Random rng = new Random(12345); // added by L

        for (int i = 0; i < numDoc; i++) {

            if (i < numBasicDoc)
                generatedDoc = docSet.get(i);
            else {
                unionIndex1 = rng.nextInt(numBasicDoc);
                unionIndex2 = rng.nextInt(numBasicDoc);

                generatedDoc = new ArrayList<Integer>();
                generatedDoc.addAll(docSet.get(unionIndex1));
                generatedDoc.addAll(docSet.get(unionIndex2));
            }

            result.append(i + " ");

            for (int j = 0; j < generatedDoc.size(); j++) {
                //	if(j + 1 >= generatedDoc.size())
                //		result.append(i + " " + j + " " + generatedDoc.get(j) + " " + (-1) + "\r\n");
                //	else
                result.append(generatedDoc.get(j) + " ");
            }

            result.append("\r\n");

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl", result.toString());
                result = new StringBuffer("");
                //		System.out.println("i = " + i);
            }
        }

//		for(int i = 0; i < topicNum; i++)
//		{
//			if( i != topicNum-1)
//				result.append("topic " + (-(i+1)) + "\r\n");
//			else
//				result.append("topic " + (-(i+1)));
//		}

        FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl", result.toString());
    }


    public static void runHMMGraphLab(int num_machines,
                                      int num_docs_per_machine,
                                      int topicNum,
                                      String sourceFolder,
                                      String outFolder) {
        int numDoc = num_docs_per_machine * num_machines;
        int numBasicDoc = 20003;

        ArrayList<ArrayList<Integer>> docSet = new ArrayList<ArrayList<Integer>>(numBasicDoc);
        String file;
        for (int i = 0; i < numBasicDoc; i++) {
            file = sourceFolder + i + ".txt";
            docSet.add(getWordSequenceMap(file));
            System.out.println("loading files " + i);
        }

        ArrayList<Integer> generatedDoc;
        int unionIndex1, unionIndex2;
        String tempString;

        StringBuffer result = new StringBuffer("");
        Random rng = new Random(12345); // added by L

        for (int i = 0; i < numDoc; i++) {

            if (i < numBasicDoc)
                generatedDoc = docSet.get(i);
            else {
                unionIndex1 = rng.nextInt(numBasicDoc);
                unionIndex2 = rng.nextInt(numBasicDoc);

                generatedDoc = new ArrayList<Integer>();
                generatedDoc.addAll(docSet.get(unionIndex1));
                generatedDoc.addAll(docSet.get(unionIndex2));
            }

            result.append(generatedDoc.size() + " ");
            for (Integer o : generatedDoc) {
                result.append(o + " ");
            }

            result.append("\r\n");

            if (i % 100 == 0) {
                FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl", result.toString());
                result = new StringBuffer("");
                System.out.println("i = " + i);
            }
        }

        FileOperation.save_to_file(outFolder + "hmm/", "WORD_IN_DOC.tbl", result.toString());
    }

    public static TreeMap<Integer, Integer> union(
            TreeMap<Integer, Integer> map1, TreeMap<Integer, Integer> map2) {
        int count1;

        TreeMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
        for (Integer o : map1.keySet()) {
            map.put(o, map1.get(o));
        }

        for (Integer o : map2.keySet()) {
            if (map.containsKey(o)) {
                count1 = map.get(o);
                map.put(o, count1 + map2.get(o));
            } else {
                map.put(o, map2.get(o));
            }
        }

        return map;
    }

    public static TreeMap<Integer, Integer> getWordCountMap(String filepath) {
        TreeMap<Integer, Integer> map = new TreeMap<Integer, Integer>();

        String temp;
        int tempValue;
        int count;
        try {
            File file = new File(filepath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            temp = bufferedReader.readLine();
            while (temp != null) {
                try {
                    tempValue = Integer.parseInt(temp);
                    if (map.containsKey(tempValue)) {
                        count = map.get(tempValue);
                        map.put(tempValue, count + 1);
                    } else {
                        map.put(tempValue, 1);
                    }
                } catch (Exception e) {
                    temp = bufferedReader.readLine();
                    continue;
                }

                temp = bufferedReader.readLine();
            }
            fileReader.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }

    public static ArrayList<Integer> getWordSequenceMap(String filepath) {
        ArrayList<Integer> list = new ArrayList<Integer>();

        String temp;
        int tempValue;
        int count;
        try {
            File file = new File(filepath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            temp = bufferedReader.readLine();
            while (temp != null) {
                try {
                    tempValue = Integer.parseInt(temp);
                    list.add(tempValue);
                } catch (Exception e) {
                    temp = bufferedReader.readLine();
                    continue;
                }

                temp = bufferedReader.readLine();
            }
            fileReader.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }

    public static void runRegression() {
        int numCoefs = 6;
        int numData = 2000;

        double[] coefs = new double[numCoefs];
        double[] dimensionValue = new double[numCoefs];
        Random random = new Random();

        for (int i = 0; i < coefs.length; i++) {
            coefs[i] = Math.pow(2, i);

            FileOperation.save_to_file(folder + "regression/", "readme",
                    coefs[i] + "\n");
        }

        String dataStr = "[\r\n";
        String outStr = "[\r\n";
        // String rStr = "[\r\n";

        for (int i = 0; i < numData; i++) {
            double sum = 0;
            double r = 0;
            for (int j = 0; j < numCoefs; j++) {
                dimensionValue[j] = Math.random() * 100;
                dataStr += "{data_id: " + i + ", \t";
                dataStr += "dimension_id: " + j + ", \t";
                dataStr += "value: " + dimensionValue[j] + "}";

                sum += coefs[j] * dimensionValue[j];
                double rand = random.nextGaussian();

                sum += rand;

                r = coefs[j] * dimensionValue[j] + rand;

                // rStr += "{data_id: " + i + ", \t";
                // rStr += "dimension_id: " + j + ", \t";
                // rStr += "value: " + r + "}";

                if (j != numCoefs - 1) {
                    dataStr += ", \r\n";
                    // rStr += ", \r\n";
                }

            }

            if (i % 100 == 0)
                System.out.println("i = " + i);

            outStr += "{data_id: " + i + ", \t";
            outStr += "value: " + sum + "}";

            if (i != numData - 1) {
                dataStr += ", \r\n";
                // rStr += ", \r\n";
                outStr += ", \r\n";
            }

            if (i % 10 == 0) {
                // FileOperation.save_to_file(
                // folder + "regression/",
                // "r.tbl", rStr);
                FileOperation.save_to_file(folder + "regression/",
                        "regressors.tbl", dataStr);
                FileOperation.save_to_file(folder + "regression/",
                        "outcomes.tbl", outStr);
                dataStr = "";
                outStr = "";
                // rStr = "";
            }
        }

        dataStr += "\r\n]\r\n";
        outStr += "\r\n]\r\n";

        FileOperation.save_to_file(folder + "regression/", "regressors.tbl",
                dataStr);
        // FileOperation.save_to_file(
        // folder + "regression/",
        // "r.tbl", rStr);
        FileOperation.save_to_file(folder + "regression/", "outcomes.tbl",
                outStr);

        String metaStr = "";
        metaStr += "[{dimensionnum:" + numCoefs;
        metaStr += ", \t datanum:" + numData;
        metaStr += "}]\r\n";
        FileOperation.save_to_file(folder + "regression/",
                "regressionMeta.tbl", metaStr);

    }

    public static void runSimpleRegression() {

        int numData = 200;

        double a = 5.23;
        double b = 10;
        double sigma = 2;

        double x, y;

        Random random = new Random();

        String dataStr = "[\r\n";

        for (int i = 0; i < numData; i++) {

            x = 100 * Math.random();
            y = a * x + b + sigma * random.nextGaussian();

            dataStr += "\t {id: " + i + ", \t" + "x: " + x + ", \t" + "y: " + y
                    + "}";

            if (i != numData - 1) {
                dataStr += ", \r\n";
            }
        }

        dataStr += "\r\n]\r\n";

        FileOperation.save_to_file(folder + "simple_regression/",
                "simpleRegData.tbl", dataStr);

        String metaStr = "";
        metaStr += "[\r\n \t {datanum:" + numData + "}\r\n";
        metaStr += "]\r\n";
        FileOperation.save_to_file(folder + "simple_regression/",
                "simpleRegMeta.tbl", metaStr);

    }

    public static void runRegressiontest() {
        int numCoefs = 5;
        int numData = 500;

        double input1[] = new double[numCoefs];
        double input2[] = new double[numCoefs];

        double[] coefs = new double[numCoefs];
        double[] dimensionValue = new double[numCoefs];
        Random random = new Random();

        for (int i = 0; i < coefs.length; i++) {
            coefs[i] = Math.pow(2, i);
            input1[i] = 0;
            input2[i] = 0;

            FileOperation.save_to_file(folder + "regression/", "readme",
                    coefs[i] + "\n");
        }

        String rStr = "[\r\n";
        String dataStr = "[\r\n";

        for (int i = 0; i < numData; i++) {
            double sum = 0;
            double r = 0;
            for (int j = 0; j < numCoefs; j++) {
                dimensionValue[j] = Math.abs(random.nextGaussian()) * 10;

                dataStr += "{data_id: " + i + ", \t";
                dataStr += "dimension_id: " + j + ", \t";
                dataStr += "value: " + dimensionValue[j] + "}";

                if (j != numCoefs - 1) {
                    dataStr += ", \r\n";
                }

                sum += coefs[j] * dimensionValue[j];
                double rand = random.nextGaussian();

                sum += rand;

                r = coefs[j] * dimensionValue[j] + rand;

                input1[j] += 2 * r * dimensionValue[j];
                input2[j] += dimensionValue[j] * dimensionValue[j];

                rStr += "{data_id: " + i + ", \t";
                rStr += "dimension_id: " + j + ", \t";
                rStr += "value:[ " + r + "]}";

                if (j != numCoefs - 1) {
                    rStr += ", \r\n";
                }
            }

            if (i != numData - 1) {
                rStr += ", \r\n";
                dataStr += ", \r\n";
            }

            if (i % 10 == 0) {
                FileOperation.save_to_file(folder + "regression/", "r.tbl",
                        rStr);
                rStr = "";

                FileOperation.save_to_file(folder + "regression/",
                        "regressors.tbl", dataStr);
                dataStr = "";
            }
        }

        rStr += "\r\n]\r\n";

        FileOperation.save_to_file(folder + "regression/", "r.tbl", rStr);

        dataStr += "\r\n]\r\n";

        FileOperation.save_to_file(folder + "regression/", "regressors.tbl",
                dataStr);

        for (int i = 0; i < coefs.length; i++) {
            System.out.print(input1[i] + "   ");
        }
        System.out.println();

        for (int i = 0; i < coefs.length; i++) {
            System.out.print(input2[i] + "    ");
        }
    }
}
