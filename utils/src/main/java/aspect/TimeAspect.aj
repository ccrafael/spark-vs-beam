package aspect;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public aspect TimeAspect {

    pointcut time() : * main(*);

    boolean around() : time() {

        long start = System.nanoTime();

        Object result = proceed(amount, acc);

        System.out.println(String.format("Time: %f s ", MILLISECONDS.convert(System.nanoTime() - start, NANOSECONDS) / 1000f));
        return result;
    }

}
