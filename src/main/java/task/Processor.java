package task;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.TreeMap;

import static task.ConfigEnum.BUFFER_READ_SIZE;
import static task.ConfigEnum.FROM_SOURCE_CHUNK;

@Slf4j
public class Processor {
    static long mem;
    private StreamTokenizer in;
    private Write write = new Write();

    public static void main(String... args) throws IOException {
        log.info("start");
//        WriteTestData.writeFile(Config.str(SOURCE_FILE));
//        new Processor().processFile(Config.str(SOURCE_FILE));
    }

    public void processFile(String sourceFile) throws IOException {
        long startSort  = System.currentTimeMillis();

        try (Reader reader = new FileReader(sourceFile); BufferedReader buf = new BufferedReader(reader, Config.num(BUFFER_READ_SIZE))) {
            in = new StreamTokenizer(buf);
            boolean hasMore = true;

            for (int i = 1; hasMore; i++) {
                soutMem(i);
                System.out.printf("free memory: %f MB\n", Runtime.getRuntime().freeMemory() / (1024 * 1024.0));
                hasMore = processChunk(i);
                soutDelta();
            }
        }

        System.out.println("-----------------------");
        System.out.println("writing result file...");
        long startWriteResult = System.currentTimeMillis();
        write.aggregate();
        System.out.printf("write to result file in -> %f seconds\n", (System.currentTimeMillis() - startWriteResult) / 1000.0);
        System.out.println("cleaning dir...");
        write.clean();

        System.out.printf("sorted in -> %f seconds\n", (System.currentTimeMillis() - startSort) / 1000.0);
    }

    private boolean processChunk(int i) throws IOException {
        TreeMap<Integer, Integer> map = read();

        if (map.size() > 0) {
            write.writeOnFS(map);
            return true;

        } else {
            return false;
        }
    }

    private TreeMap<Integer, Integer> read() throws IOException {
        TreeMap<Integer, Integer> map = new TreeMap<>();
        int numCount = 1;
        int c;

        int maxCount = Config.num(FROM_SOURCE_CHUNK);

        while ( numCount <= maxCount &&(c = in.nextToken()) != StreamTokenizer.TT_EOF && c != StreamTokenizer.TT_EOL) {
            int val = (int)in.nval;
            addToMap(map, val);
            numCount++;
        }

        return map;
    }

    private void addToMap(Map<Integer, Integer> map, int key) {
        if (map.get(key) != null) {
            map.put(key, map.get(key) + 1);
        } else {
            map.put(key, 1);
        }
    }


    private static void soutMem(int i) {
        mem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        System.out.printf("chunk №%d : start mem = %f MB\n", i, mem / (1024 * 1024.0));
    }

    private static void soutDelta() {
        long delta = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - mem;
        mem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        System.out.printf("end mem = %f MB, delta = %f MB\n\n", mem / (1024 * 1024.0), delta / (1024 * 1024.0));
    }

}
