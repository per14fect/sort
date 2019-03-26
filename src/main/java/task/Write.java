package task;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static task.ConfigEnum.*;

public class Write {
    public Write() {
        File file = new File(Config.str(DIR_FOR_TEMP_FILES));
        file.mkdir();
    }

    public void clean() {
        File file = new File(Config.str(DIR_FOR_TEMP_FILES));

        for(File f: file.listFiles()) {
            f.delete();
        }
        file.delete();
    }

    private Map<Integer, Integer> minMaxMap = new TreeMap<>();

    public void writeOnFS(NavigableMap<Integer, Integer> map) throws IOException {
        int numFilesAffected = 0;

        long start = System.currentTimeMillis();
        soutMinMaxSize();

        if (minMaxMap.size() > 0) {
            List<Pair> toAddToMinMax = new ArrayList<>();

            Iterator<Map.Entry<Integer, Integer>> iterator = minMaxMap.entrySet().iterator();

            while(iterator.hasNext()) {
                Map.Entry<Integer, Integer> minMaxEntry = iterator.next();

                if ( concern(minMaxEntry, map.firstKey(), map.lastKey()) ) {

                    long readStart = System.currentTimeMillis();

                    numFilesAffected++;
                    File file = file(minMaxEntry.getKey());
                    TreeMap<Integer, Integer> mapFromFS = readFromFile(file);
                    soutTime("read from file", readStart);

                    long mergeStart = System.currentTimeMillis();

                    map = Stream.concat(map.entrySet().stream(), mapFromFS.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue + newValue, TreeMap::new));

                    soutTime("merge 2 maps", mergeStart);

                    if (map.firstKey().equals(mapFromFS.firstKey())) {
                        minMaxEntry.setValue(map.firstKey());
                    } else {
                        iterator.remove();
                        deleteFile(mapFromFS.firstKey());
                    }

                    long writeStart = System.currentTimeMillis();
                    Integer lastChunkKey = writeChunk(map);
                    toAddToMinMax.add(new Pair(map.firstKey(), lastChunkKey));
                    soutTime("write to temp file", writeStart);

                    map = map.tailMap(lastChunkKey, false);

                }
            }

            addToMinMax(toAddToMinMax);
        }

        long writeStart = System.currentTimeMillis();
        while (map.size() > 0) {
            Integer lastChunkKey = writeChunk(map);
            minMaxMap.put(map.firstKey(), lastChunkKey);
            map = map.tailMap(lastChunkKey, false);
        }
        soutTime("write to temp file", writeStart);

        soutTime("write on FS; files affected " + numFilesAffected, start);
    }

    private void addToMinMax(List<Pair> list) {
        list.stream().forEach(p -> {
            minMaxMap.put(p.getMin(), p.getMax());
        });
    }


    private void deleteFile(Integer min) {
        file(min).delete();
    }

    private File file(int min) {
        return new File(Config.str(DIR_FOR_TEMP_FILES), min + ".txt");
    }

    private TreeMap<Integer, Integer> readFromFile(File file) throws IOException {
        TreeMap<Integer, Integer> map = new TreeMap<>();

        try (Reader reader = new FileReader(file); BufferedReader buf = new BufferedReader(reader, Config.num(BUFFER_READ_SIZE))) {
            StreamTokenizer in = new StreamTokenizer(buf);

            int c = in.nextToken();
            while ( c != StreamTokenizer.TT_EOF && c != StreamTokenizer.TT_EOL) {
                int key = (int)in.nval;
                in.nextToken();
                int val = (int)in.nval;
                c = in.nextToken();
                map.put(key, val);
            }

        }

        return map;
    }

    private boolean concern(Map.Entry<Integer, Integer> entry, int min, int max) {
        int entryMin = entry.getKey();
        int entryMax = entry.getValue();

        return !((max < entryMin) || (min > entryMax));
    }

    private Integer writeChunk(NavigableMap<Integer, Integer> map) throws IOException {
        Integer lastChunkKey = null;

        int count = 1;
        File fileName = file(map.firstKey());

        try (Writer writer = new FileWriter(fileName); BufferedWriter buf = new BufferedWriter(writer, Config.num(BUFFER_READ_SIZE))) {
            Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();

            int chunkSize = Config.num(CHUNK);

            while(iterator.hasNext() && count <= chunkSize) {
                Map.Entry<Integer, Integer> entry = iterator.next();
                buf.write(entry.getKey().toString());
                buf.write(' ');
                buf.write(entry.getValue().toString());
                buf.write('\n');
                lastChunkKey = entry.getKey();
                count++;
            }

            buf.flush();
        }

        return lastChunkKey;
    }

    public void aggregate() throws IOException {
        soutAggregate();

        try(Writer writer = new FileWriter(Config.str(RESULT_FILE)); BufferedWriter buf = new BufferedWriter(writer, Config.num(BUFFER_SIZE_RESULT_FILE));
            PrintWriter out = new PrintWriter(buf)) {

            for(Map.Entry<Integer, Integer> entry: minMaxMap.entrySet()) {
                TreeMap<Integer, Integer> map = readFromFile(file(entry.getKey()));

                Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();

                while(iterator.hasNext()) {
                    Map.Entry<Integer, Integer> valueEntry = iterator.next();
                    int times = valueEntry.getValue();

                    for(int i = 1; i <= times; i++) {
                        buf.write(valueEntry.getKey().toString());
                        buf.write('\n');
                    }
                }
            }

            buf.flush();

        }
    }

    private void soutAggregate() {
        System.out.printf("aggregating result from %d files...\n", minMaxMap.size());
    }

    private void soutMinMaxSize() {
        System.out.printf("minMaxSize = %d\n", minMaxMap.size());
    }

    private void soutTime(String str, long start) {
        if (str.startsWith("write on FS")) {
            System.out.printf(str + " processed in -> %f seconds\n", (System.currentTimeMillis() - start) / 1000.0);
        }
    }
}
