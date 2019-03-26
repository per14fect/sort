package task;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static task.ConfigEnum.*;
import static task.Sout.*;

@Slf4j
public class ProcessorTest {
    @Test
    public void testProcessor() throws IOException {
        WriteTestData.writeFile(Config.str(SOURCE_FILE), Config.num(NUM_AMOUNT_OF_TEST_DATA));
        long start = System.currentTimeMillis();
        long mem = soutMem();

        Processor processor = new Processor();
        processor.processFile(Config.str(SOURCE_FILE));

        soutTime("sort by algorithm", start);
        soutDelta(mem);

//
//        List<Integer> fromSource = WriteTestData.readFile(Config.str(SOURCE_FILE));
//        Collections.sort(fromSource);
//        List<Integer> fromResult = WriteTestData.readFile(Config.str(RESULT_FILE));
//
//        Assert.assertEquals(fromSource, fromResult);

    }

    @Test
    public void readAndSortInMemory() throws IOException {
        WriteTestData.writeFile(Config.str(SOURCE_FILE), Config.num(NUM_AMOUNT_OF_TEST_DATA));
        long start = System.currentTimeMillis();
        long mem = soutMem();

        List<Integer> fromSource = WriteTestData.readFile(Config.str(SOURCE_FILE));
        Collections.sort(fromSource);

        soutTime("sort in memory", start);
        soutDelta(mem);
    }

    @Test
    public void testConfig3() throws IOException {
        Config.reload("config3");
        WriteTestData.writeFile(Config.str(SOURCE_FILE), Config.num(NUM_AMOUNT_OF_TEST_DATA));
        Processor processor = new Processor();
        processor.processFile(Config.str(SOURCE_FILE));

        List<Integer> fromSource = WriteTestData.readFile(Config.str(SOURCE_FILE));
        Collections.sort(fromSource);
        List<Integer> fromResult = WriteTestData.readFile(Config.str(RESULT_FILE));

        Assert.assertEquals(fromSource, fromResult);

    }

    @Test
    public void testScatter() {
        Map<Integer, Integer> rangeMap;
        List<Integer> list;

        rangeMap = Scatter.of(Integer.MIN_VALUE, Integer.MAX_VALUE);
        log.info(String.format("scatters = %d", rangeMap.size()));
        list = new ArrayList<>(rangeMap.keySet());
        Collections.sort(list);

        for(Integer item: list) {
            log.info("start = {}; end = {}", item, rangeMap.get(item));
        }
        log.info("----------------");

        rangeMap = Scatter.of(Integer.MIN_VALUE, 0);
        log.info(String.format("scatters = %d", rangeMap.size()));
        list = new ArrayList<>(rangeMap.keySet());
        Collections.sort(list);

        for(Integer item: list) {
            log.info("start = {}; end = {}", item, rangeMap.get(item));
        }
        log.info("----------------");

        rangeMap = Scatter.of(Integer.MAX_VALUE-1, Integer.MAX_VALUE);
        log.info(String.format("scatters = %d", rangeMap.size()));
        list = new ArrayList<>(rangeMap.keySet());
        Collections.sort(list);

        for(Integer item: list) {
            log.info("start = {}; end = {}", item, rangeMap.get(item));
        }
        log.info("----------------");

        rangeMap = Scatter.of(Integer.MAX_VALUE, Integer.MAX_VALUE);
        log.info(String.format("scatters = %d", rangeMap.size()));
        list = new ArrayList<>(rangeMap.keySet());
        Collections.sort(list);

        for(Integer item: list) {
            log.info("start = {}; end = {}", item, rangeMap.get(item));
        }
        log.info("----------------");

        rangeMap = Scatter.of(-4, 9);
        log.info(String.format("scatters = %d", rangeMap.size()));
        list = new ArrayList<>(rangeMap.keySet());
        Collections.sort(list);

        for(Integer item: list) {
            log.info("start = {}; end = {}", item, rangeMap.get(item));
        }
        log.info("----------------");

        rangeMap = Scatter.of(-1331439863, -1288490193);
        log.info(String.format("scatters = %d", rangeMap.size()));

        list = new ArrayList<>(rangeMap.keySet());
        Collections.sort(list);

        for(Integer item: list) {
            log.info("start = {}; end = {}", item, rangeMap.get(item));
        }
        log.info("----------------");
    }

    @Test
    public void testFastProcessor() throws IOException {
//        WriteTestData.writeFile(Config.str(SOURCE_FILE));
        String test1Gb = "test1Gb.txt";

        long start = System.currentTimeMillis();

        File sourceFile = new File(test1Gb);
        FastProcessor processor = new FastProcessor();
        processor.sort(sourceFile);

        soutTime("fast processor", start);
    }

    @Test
    public void test_sortInMemory() throws IOException {
        String name = "local-sort.txt";
        WriteTestData.writeFile(name, 1_200_000);
        File file = new File(name);

        FastProcessor.sortInMemory(".", file);

    }

    @Test
    public void test_appendFileToFile() throws IOException {
        WriteTestData.writeFile("one", 1_000_000);
        File one = new File("one");

        WriteTestData.writeFile("two", 1_000_000);
        File two = new File("two");

        File res = new File("res");
        res.createNewFile();

        FileSystem.appendFileToFile(one, res);
        FileSystem.appendFileToFile(two, res);

        List<Integer> listOne = FileSystem.readFileInMem(one);
        List<Integer> listTwo = FileSystem.readFileInMem(two);

        listOne.addAll(listTwo);

        List<Integer> listRes = FileSystem.readFileInMem(res);

        one.delete();
        two.delete();
        res.delete();

        Assert.assertEquals(listOne, listRes);

    }



    @Test
    public void testFastProcessor100Mb() throws IOException {
        String fileName = "test100Mb.txt";
        WriteTestData.writeFile(fileName, 10 * 1024 * 1024);

        File sourceFile = new File(fileName);
        FastProcessor processor = new FastProcessor();
        processor.sort(sourceFile);

        File sortedFile = new File("-2147483648_2147483647.txt");

        List<Integer> listOne = FileSystem.readFileInMem(sourceFile);
        Collections.sort(listOne);
        List<Integer> listTwo = FileSystem.readFileInMem(sortedFile);

        Assert.assertEquals(listOne, listTwo);
        sourceFile.delete();
        sortedFile.delete();
    }


}
