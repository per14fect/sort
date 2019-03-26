package task;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static task.ConfigEnum.BUFFER_READ_SIZE;

public class FileSystem {
    private static final int VALUES_SIZE_TO_FLUSH = 10_000;

    public static void appendToFile(List<Integer> list, int min, int max, String dir) {
        try {
            File file = new File(dir, nameBy(min, max));

            if (!file.exists()) {
                file.createNewFile();
            }

            try (Writer writer = new FileWriter(file, true); BufferedWriter buf = new BufferedWriter(writer, Config.num(BUFFER_READ_SIZE))) {
                for (Integer val : list) {
                    buf.write(val.toString());
                    buf.newLine();
                }

                buf.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String nameBy(int min, int max) {
        return String.format("%d_%d.txt", min, max);
    }


    public static void cleanDir(String dir) {
        File file = new File(dir);

        if (file.exists()) {

            for (File f : file.listFiles()) {
                f.delete();
            }

            file.delete();
        }
    }

    public static void mkdir(String dir) {
        File file = new File(dir);
        file.mkdir();
    }

    public static List<Integer> readFileInMem(File file) {
        List<Integer> list = new ArrayList<>();

        try (Reader reader = new FileReader(file); BufferedReader buf = new BufferedReader(reader, Config.num(BUFFER_READ_SIZE))) {
            StreamTokenizer in = new StreamTokenizer(buf);
            int c;

            while ((c = in.nextToken()) != StreamTokenizer.TT_EOF && c != StreamTokenizer.TT_EOL) {
                int val = (int) in.nval;
                list.add(val);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return list;
    }

    public static void writeOnDisk(List<Integer> list, File file) {
        try {
            if (file.exists()) {
                throw new IllegalStateException(String.format("file exists %s", file.getName()));
            } else {
                file.createNewFile();
            }

            try (Writer writer = new FileWriter(file); BufferedWriter buf = new BufferedWriter(writer, Config.num(BUFFER_READ_SIZE))) {
                for (Integer val : list) {
                    buf.write(val.toString());
                    buf.newLine();
                }

                buf.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void clearFile(File file) {
        try(PrintWriter pw = new PrintWriter(file)) {
            pw.println("");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void appendFileToFile(File partFile, File file) {
        try (Reader reader = new FileReader(partFile);
             BufferedReader buf = new BufferedReader(reader, Config.num(BUFFER_READ_SIZE))) {

            StreamTokenizer in = new StreamTokenizer(buf);
            int c;

            List<Integer> valueList = new ArrayList<>();

            while ((c = in.nextToken()) != StreamTokenizer.TT_EOF && c != StreamTokenizer.TT_EOL) {
                int val = (int) in.nval;
                valueList.add(val);

                if (valueList.size() == VALUES_SIZE_TO_FLUSH) {
                    flushToFile(valueList, file);
                    valueList = new ArrayList<>();
                }
            }

            flushToFile(valueList, file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void flushToFile(List<Integer> list, File file) {

        try (Writer writer = new FileWriter(file, true); BufferedWriter buf = new BufferedWriter(writer, Config.num(BUFFER_READ_SIZE))) {
            for (Integer val : list) {
                buf.write(val.toString());
                buf.newLine();
            }

            buf.flush();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
