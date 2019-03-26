package task;

import lombok.extern.slf4j.Slf4j;

import java.lang.management.ManagementFactory;

@Slf4j
public class Sout {
//    private static void soutMem(int i) {
//        mem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
//        System.out.printf("chunk №%d : start mem = %f MB\n", i, mem / (1024 * 1024.0));
//    }
//
//    private static void soutDelta() {
//        long delta = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - mem;
//        mem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
//        System.out.printf("end mem = %f MB, delta = %f MB\n\n", mem / (1024 * 1024.0), delta / (1024 * 1024.0));
//    }

    public static void soutText(String msg) {
        log.debug(msg);
    }

    public static void soutTime(String str, long start) {
        String msg = String.format("%s processed in -> %.05f seconds", str, (System.currentTimeMillis() - start) / 1000.0);
        log.debug(msg);
    }

    public static long soutMem() {
        long mem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        String msg = String.format("start mem = %.05f MB of total mem = %.05f", mem / (1024 * 1024.0),
                Runtime.getRuntime().totalMemory() / (1024 * 1024.0));
        log.debug(msg);
        return mem;
    }

    public static void soutDelta(long start) {
        long delta = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - start;
        long mem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        String msg = String.format("end mem = %.05f MB, delta = %.05f MB", mem / (1024 * 1024.0), delta / (1024 * 1024.0));
        log.debug(msg);
    }
}
