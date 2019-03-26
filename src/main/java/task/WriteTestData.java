package task;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static task.ConfigEnum.*;

public class WriteTestData {
    public static void main(String... args) throws IOException {
        writeFile(Config.str(SOURCE_FILE), Config.num(NUM_AMOUNT_OF_TEST_DATA));
    }

    public static void writeFile(String file, int numRecords) throws IOException {
        try (Writer writer = new FileWriter(file); PrintWriter out = new PrintWriter(writer)) {
            long start = System.currentTimeMillis();

            for (int i = 1; i <= numRecords; i++) {
                int digit =  (int)(Math.random() * Config.num(MAX_INT)) * (Math.random() >= 0.5 ? 1 : (-1));
                out.println(digit);
            }

            out.flush();
            System.out.printf("flush %d -> %f seconds\n", numRecords, (System.currentTimeMillis() - start) / 1000.0);
        }
    }

    public static List<Integer> readFile(String file) throws IOException {
        List<Integer> list = new ArrayList<>();

        try (Reader reader = new FileReader(file); BufferedReader buf = new BufferedReader(reader, Config.num(BUFFER_READ_SIZE))) {
            StreamTokenizer in = new StreamTokenizer(buf);
            int c;

            while ( (c = in.nextToken()) != StreamTokenizer.TT_EOF && c != StreamTokenizer.TT_EOL) {
                int val = (int)in.nval;
                list.add(val);
            }

        }

        return list;

    }
 }
