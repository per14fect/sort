package task;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static task.ConfigEnum.BUFFER_READ_SIZE;
import static task.Sout.soutText;

@Slf4j
public class FastProcessor{
    private static final int VALUES_MAP_SIZE_TO_FLUSH = 10_000;
    private static final int CRITICAL_FILE_LENGTH = 10 * 1024 * 1024;

    private static final String DIR_NAME = "1";

    public void sort(File originFile) throws IOException {
        String fileName = FileSystem.nameBy(Integer.MIN_VALUE, Integer.MAX_VALUE);
        File copyFile = new File(fileName);
        if (copyFile.exists()) {
            copyFile.delete();
        }
        Files.copy(originFile.toPath(), copyFile.toPath());

        processFile(copyFile, DIR_NAME);

    }


    private void processFile(File file, String dir) {
        String fileName = file.getName().replaceAll(".txt", "");
        int min = Integer.valueOf(fileName.split("_")[0]);
        int max = Integer.valueOf(fileName.split("_")[1]);

        soutText(String.format( "process file min = %d, max = %d, length = %dMb", min, max, file.length() / (1024 * 1024)) );

        Map<Integer, Integer> rangeMap = Scatter.of(min, max);
        FileSystem.mkdir(dir);
        readOnFS(file, dir, rangeMap);

        File directory = new File(dir);
        soutText(String.format("scatters =  %d, files =  %d", rangeMap.size(), directory.listFiles().length) );

        if (directory.listFiles().length > rangeMap.size()) {
            throw new IllegalStateException(String.format("files %d > rangeMap size %d", directory.listFiles().length, rangeMap.size()));
        }

//        for (int i = 0; i < directory.listFiles().length; i++) {
//            File partFile = directory.listFiles()[i];
//            log.info(partFile.getName());
//        }

//        log.info("--------------------SORTED");


        List<File> sortedFileList = Arrays.stream(directory.listFiles())
                .sorted(Comparator.comparing(compFile -> Integer.valueOf(compFile.getName().split("_")[0])))
                .collect(Collectors.toList());

//        sortedFileList.forEach(partFile -> log.info(partFile.getName()));

        for (int i = 0; i < sortedFileList.size(); i++) {
            File partFile = sortedFileList.get(i);

            if (partFile.length() > CRITICAL_FILE_LENGTH) {

                if (rangeMap.size() > 1) {
                    String fileDir = dir + File.separator + DIR_NAME;
                    processFile(partFile, fileDir);

                } else {
                    soutText(String.format("skip scatterSize  = %d", rangeMap.size()));
                }

            } else {
                soutText(String.format("sort file length = %dMB", partFile.length() / (1024 * 1024)));
                sortInMemory(dir, partFile);
            }

        }

        FileSystem.clearFile(file);

        for (int i = 0; i < sortedFileList.size(); i++) {
            File partFile = sortedFileList.get(i);
            soutText(String.format("write file to parent = %d, file = %s", i, partFile.getName()));
            FileSystem.appendFileToFile(partFile, file);
            partFile.delete();
        }

        directory.delete();
        soutText(String.format("clean dir = %s", dir));
    }

    public static void sortInMemory(String dir, File partFile) {
        List<Integer> list = FileSystem.readFileInMem(partFile);
        Collections.sort(list);

        File sortedFile = new File(dir, "s_" + partFile.getName());
        FileSystem.writeOnDisk(list, sortedFile);
        sortedFile.renameTo(partFile);
    }


    private void readOnFS(File sourceFile, String dir, Map<Integer, Integer> rangeMap) {
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

    private void flushMap(Map<Integer, List<Integer>> valuesMap, Map<Integer, Integer> rangeMap, String dir) {
        for (Integer min: valuesMap.keySet()) {
            FileSystem.appendToFile(valuesMap.get(min), min, rangeMap.get(min), dir);
        }
    }

    private void addToMap(Map<Integer, List<Integer>> valuesMap, int min, int val, int max, String dir) {
        if (valuesMap.get(min) == null) {
            valuesMap.put(min, new ArrayList<>());
        }

        valuesMap.get(min).add(val);

        if (valuesMap.get(min).size() > VALUES_MAP_SIZE_TO_FLUSH) {
            FileSystem.appendToFile(valuesMap.get(min), min, max, dir);
            valuesMap.put(min, new ArrayList<>());
        }
    }
}
