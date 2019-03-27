package task;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Scatter {
    private static final int DIVIDED_BY = 10;

    public static Map<Integer, Integer> of(final int min, final int max) {
        if (min > max) {
            throw new IllegalArgumentException(String.format("min = %d > max = %d", min, max));
        }

        Map <Integer, Integer> rangeMap = new HashMap<>();

        int ind = min;

        if (min != max) {
            int chunk = calculateChunk(min, max);

            if (max >= Integer.MIN_VALUE + chunk) {

                for (ind = min; ind <= max - chunk; ) {
                    rangeMap.put(ind, ind + chunk - 1);
                    ind += chunk;
                }
            }
        }

        rangeMap.put(ind, max);

        if (rangeMap.size() > DIVIDED_BY + 1) {
            throw new IllegalStateException(String.format("rangeMap size = %d", rangeMap.size()));
        }

        return rangeMap;
    }

    private static int calculateChunk(final int min, final int max) {
        int maxPart = (max / DIVIDED_BY) == 0 ? (int)Math.signum(max) : (max / DIVIDED_BY);
        int minPart = (min / DIVIDED_BY) == 0 ? (int)Math.signum(min) : (min / DIVIDED_BY);
        int chunk = maxPart - minPart;

        if (chunk == 0) {
            if (max != min) {
                chunk = (max / 2) - (min / 2);
            }
        }

        chunk = (chunk > 1) ? chunk : 2;

        if (chunk <= 1) {
            throw new IllegalArgumentException(String.format("chunk = %d", chunk));
        }

        return chunk;
    }


    public static Integer findMin(Map<Integer, Integer> rangeMap, int val) {
        for (Integer  key: rangeMap.keySet()) {
            if (val >= key && val <= rangeMap.get(key)) {
                return key;
            }
        }

        return null;
    }
}
