package edu.rice.republic.spark_data_generator;

/**
 * Created by xs6 on 10/10/16.
 */

import java.io.*;
import java.util.ArrayList;

public class FileOperation {

    public static boolean save_to_file(String path, String filename, String content) {
        File file;
        FileWriter fileWriter = null;
        try {
            file = new File(path);
            if (!file.exists()) {
                System.out.print("file doesn't exist!");
                file.mkdirs();
            }
            file = new File(file, filename);
            fileWriter = new FileWriter(file, true);
            fileWriter.write(content);
            fileWriter.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    public static boolean is_in_file(String filename, String target) {
        String infomation = getFile_info(filename);
        if (infomation == null) return false;
        if (infomation.indexOf(target) >= 0) return true;
        else return false;
    }


    public static String getFile_info(String filename) {
        String result = "";
        String temp = null;
        try {
            FileReader fileReader = new FileReader(filename);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            temp = bufferedReader.readLine();
            while (temp != null) {
                result += temp;
                result += "\n";
                temp = bufferedReader.readLine();
            }
            fileReader.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String[] getFile_content(String dirname) {
        String temp = null;
        ArrayList<String> resultList = new ArrayList<String>();
        try {
            FileReader fileReader = new FileReader(dirname);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            temp = bufferedReader.readLine();
            while (temp != null) {
                resultList.add(temp);
                temp = bufferedReader.readLine();
            }
            fileReader.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] resultString = new String[resultList.size()];
        for (int i = 0; i < resultString.length; i++)
            resultString[i] = resultList.get(i);
        return resultString;
    }

    public static ArrayList<String> getFile_content(String folder, String dirname) {
        String temp = null;
        ArrayList<String> resultList = new ArrayList<String>();
        try {
            File file = new File(folder, dirname);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            temp = bufferedReader.readLine();
            while (temp != null) {
                resultList.add(temp);
                temp = bufferedReader.readLine();
            }
            fileReader.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultList;
    }

    public static String[] getDir(String dirname) {
        String[] dirlist;
        try {
            File file = new File(dirname);
            dirlist = file.list();
            return dirlist;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
