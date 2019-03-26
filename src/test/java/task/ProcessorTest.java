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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static task.Sout.soutTime;

@Slf4j
public class ProcessorTest {

    @Test
    public void test_fast_processor() throws IOException {
        log.debug("------------     test_fast_processor");
        log.debug("");
        String test1Gb = "testOther1Gb.txt";
        FileSystem.writeTestData("testOther1Gb.txt",100_000_000);
//        String test1Gb = "test1Gb.txt";

        long start = System.currentTimeMillis();

        File sourceFile = new File(test1Gb);
        FastProcessor processor = new FastProcessor();
        processor.sort(sourceFile);

        log.debug("");
        soutTime("fast processor", start);
    }

    @Test
    public void test_fast_processor_100MB_file() throws IOException {
        log.debug("------------     test_fast_processor_100MB_file");
        log.debug("");
        String testFile = "test100MB.txt";
        FileSystem.writeTestData(testFile,1024 * 1024);

        long start = System.currentTimeMillis();

        File sourceFile = new File(testFile);
        FastProcessor processor = new FastProcessor();
        processor.sort(sourceFile);

        log.debug("");
        soutTime("fast processor", start);
    }

    @Test
    public void test_fast_processor_with_sort_100Mb_in_memory() throws IOException {
        log.debug("------------     test_fast_processor_with_sort_100Mb_in_memory");
        log.debug("");
        String fileName = "test100Mb.txt";
        FileSystem.writeTestData(fileName, 10 * 1024 * 1024);

        long start = System.currentTimeMillis();

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

        log.debug("");
        soutTime("fast processor", start);
    }

    // tests that values range from min to max is scattered properly
    @Test
    public void test_all_scatters() {
        log.debug("------------     test_all_scatters");
        log.debug("");
        Map<Integer, Integer> rangeMap;
        int min;
        int max;
        boolean passed;

        min = Integer.MIN_VALUE; max = Integer.MAX_VALUE;
        rangeMap = Scatter.of(min, max);
        log.debug(String.format("checked scatters = %d min = %d max = %d", rangeMap.size(), min, max));
        passed = test_ranges(rangeMap, min, max);
        Assert.assertTrue(passed);

        min = Integer.MIN_VALUE; max = 0;
        rangeMap = Scatter.of(min, max);
        log.debug(String.format("checked scatters = %d min = %d max = %d", rangeMap.size(), min, max));
        passed = test_ranges(rangeMap, min, max);
        Assert.assertTrue(passed);

        min = Integer.MAX_VALUE; max = Integer.MAX_VALUE;
        rangeMap = Scatter.of(min, max);
        log.debug(String.format("checked scatters = %d min = %d max = %d", rangeMap.size(), min, max));
        Assert.assertTrue(rangeMap.size() == 1);
        Assert.assertTrue(Integer.MAX_VALUE == rangeMap.get(Integer.MAX_VALUE));

        min = Integer.MIN_VALUE; max = Integer.MIN_VALUE + 1;
        rangeMap = Scatter.of(min, max);
        log.debug(String.format("checked scatters = %d min = %d max = %d", rangeMap.size(), min, max));
        passed = test_ranges(rangeMap, min, max);
        Assert.assertTrue(passed);

        min = Integer.MAX_VALUE - 1; max = Integer.MAX_VALUE;
        rangeMap = Scatter.of(min, max);
        log.debug(String.format("checked scatters = %d min = %d max = %d", rangeMap.size(), min, max));
        passed = test_ranges(rangeMap, min, max);
        Assert.assertTrue(passed);

        min = -4; max = 9;
        rangeMap = Scatter.of(min, max);
        log.debug(String.format("checked scatters = %d min = %d max = %d", rangeMap.size(), min, max));
        passed = test_ranges(rangeMap, min, max);
        Assert.assertTrue(passed);

        min = -1331439863; max = -1288490193;
        rangeMap = Scatter.of(min, max);
        log.debug(String.format("checked scatters = %d min = %d max = %d", rangeMap.size(), min, max));
        passed = test_ranges(rangeMap, min, max);
        Assert.assertTrue(passed);

        assertThatThrownBy(() -> Scatter.of(Integer.MAX_VALUE - 10, Integer.MIN_VALUE + 10))
                .isInstanceOf(IllegalArgumentException.class);

    }

    private boolean test_ranges(Map<Integer, Integer> rangeMap, int min, int max) {
        boolean passed = true;

        List<Integer> list = new ArrayList<>(rangeMap.keySet());
        Collections.sort(list);

        if (list.size() >= 1) {
            int first = list.get(0);
            int last = list.get(list.size() - 1);

            if (first == min && rangeMap.get(last) == max) {

                for (int i = 1; i < list.size(); i++) {
                    int end = rangeMap.get(list.get(i-1));
                    int start = list.get(i);

                    if (end != start - 1) {
                        log.error("end != start - 1 {} {}", end, start);
                        return false;
                    }
                }
            } else {
                return false;
            }
        }

        return passed;
    }

    @Test
    public void test_sortInMemory() throws IOException {
        log.debug("------------     test_sortInMemory");
        log.debug("");
        String name = "local-sort.txt";
        FileSystem.writeTestData(name, 1_200_000);
        File file = new File(name);

        List<Integer> listInMem = FileSystem.readFileInMem(file);
        Collections.sort(listInMem);

        FastProcessor.sortInMemory(".", file);
        List<Integer> listFromDisk = FileSystem.readFileInMem(file);

        assertThat(listInMem).isEqualTo(listFromDisk);
        file.delete();
    }

    @Test
    public void test_appendFileToFile() throws IOException {
        log.debug("------------     test_appendFileToFile");
        log.debug("");
        FileSystem.writeTestData("one", 1_000_000);
        File one = new File("one");

        FileSystem.writeTestData("two", 1_000_000);
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

        assertThat(listOne).isEqualTo(listRes);
    }






}
