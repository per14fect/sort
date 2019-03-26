package task;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Sout {
    public static void soutText(String msg) {
        log.debug(msg);
    }

    public static void soutTime(String str, long start) {
        String msg = String.format("%s processed in -> %.05f seconds", str, (System.currentTimeMillis() - start) / 1000.0);
        log.debug(msg);
    }

    public static void soutFreeMem() {
        String msg = String.format("free mem = %.05f MB of total mem = %.05f", Runtime.getRuntime().freeMemory() / (1024 * 1024.0),
                Runtime.getRuntime().totalMemory() / (1024 * 1024.0));
        log.debug(msg);
    }
}
