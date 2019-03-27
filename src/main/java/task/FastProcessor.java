package task;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static task.Sout.soutText;

@Slf4j
public class FastProcessor{

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
        FileSystem.readBigFileInSmallerFiles(file, dir, rangeMap);

        File directory = new File(dir);
        soutText(String.format("scatters =  %d, files =  %d", rangeMap.size(), directory.listFiles().length) );

        if (directory.listFiles().length > rangeMap.size()) {
            throw new IllegalStateException(String.format("files %d > rangeMap size %d", directory.listFiles().length, rangeMap.size()));
        }

        List<File> sortedFileList = Arrays.stream(directory.listFiles())
                .sorted(Comparator.comparing(compFile -> Integer.valueOf(compFile.getName().split("_")[0])))
                .collect(Collectors.toList());

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
                soutText(String.format("sort file %s length = %dMB", partFile.getName(), partFile.length() / (1024 * 1024)));
                sortInMemory(dir, partFile);
            }

        }


        if (sortedFileList.size() == 1 && file.length() == sortedFileList.get(0).length()) {
            File partFile = sortedFileList.get(0);
            partFile.delete();

        } else {

            FileSystem.clearFile(file);

            for (int i = 0; i < sortedFileList.size(); i++) {
                File partFile = sortedFileList.get(i);
                soutText(String.format("write file to parent = %d, file = %s", i, partFile.getName()));
                FileSystem.appendFileToFile(partFile, file);
                partFile.delete();
            }
        }

        directory.delete();
        soutText(String.format("clean dir = %s", dir));
    }

    public static void sortInMemory(String dir, File partFile) {
        List<Integer> list = FileSystem.readFileInMem(partFile);
        Collections.sort(list);

        File sortedFile = new File(dir, "s_" + partFile.getName());
        FileSystem.writeToNewFile(list, sortedFile);
        sortedFile.renameTo(partFile);
    }

}
