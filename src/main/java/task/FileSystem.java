package task;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static task.ConfigEnum.BUFFER_READ_SIZE;
import static task.Sout.soutText;

public class FileSystem {
    private static final int VALUES_MAP_SIZE_TO_FLUSH = 10_000;
    private static final int VALUES_SIZE_TO_FLUSH = 10_000;

    private static void appendOrCreateToFile(List<Integer> list, int min, int max, String dir) {
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

    public static void writeToNewFile(List<Integer> list, File file) {
        try {
            if (file.exists()) {
                throw new IllegalStateException(String.format("file exists %s", file.getName()));
            } else {
                file.createNewFile();
            }
            appendToExistingFile(list, file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void clearFile(File file) {
        try(PrintWriter pw = new PrintWriter(file)) {
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
                    appendToExistingFile(valueList, file);
                    valueList = new ArrayList<>();
                }
            }

            appendToExistingFile(valueList, file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void appendToExistingFile(List<Integer> list, File file) {

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

    public static void writeTestData(String file, int numRecords) throws IOException {
        try (Writer writer = new FileWriter(file); BufferedWriter buf = new BufferedWriter(writer, Config.num(BUFFER_READ_SIZE))) {
            long start = System.currentTimeMillis();

            for (int i = 1; i <= numRecords; i++) {
                Integer digit =  (int)(Math.random() * Integer.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : (-1));
                buf.write(digit.toString());
                buf.newLine();
            }

            buf.flush();
            soutText(String.format("write file %s with %d records -> %f seconds", file, numRecords, (System.currentTimeMillis() - start) / 1000.0));
        }
    }

    public static void readBigFileInSmallerFiles(File sourceFile, String dir, Map<Integer, Integer> rangeMap) {
        try (Reader reader = new FileReader(sourceFile);
             BufferedReader buf = new BufferedReader(reader, Config.num(BUFFER_READ_SIZE))) {

            StreamTokenizer in = new StreamTokenizer(buf);
            int c;

            Map<Integer, List<Integer>> valuesMap = new HashMap<>();

            while ((c = in.nextToken()) != StreamTokenizer.TT_EOF && c != StreamTokenizer.TT_EOL) {
                int val = (int)in.nval;

                int min = Scatter.findMin(rangeMap, val);
                addToMap(valuesMap, min, val, rangeMap.get(min), dir);

            }

            flushMap(valuesMap, rangeMap, dir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void flushMap(Map<Integer, List<Integer>> valuesMap, Map<Integer, Integer> rangeMap, String dir) {
        for (Integer min: valuesMap.keySet()) {
            FileSystem.appendOrCreateToFile(valuesMap.get(min), min, rangeMap.get(min), dir);
        }
    }

    private static void addToMap(Map<Integer, List<Integer>> valuesMap, int min, int val, int max, String dir) {
        if (valuesMap.get(min) == null) {
            valuesMap.put(min, new ArrayList<>());
        }

        valuesMap.get(min).add(val);

        if (valuesMap.get(min).size() > VALUES_MAP_SIZE_TO_FLUSH) {
            FileSystem.appendOrCreateToFile(valuesMap.get(min), min, max, dir);
            valuesMap.put(min, new ArrayList<>());
        }
    }

}
