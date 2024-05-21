import com.google.common.base.Stopwatch;
import io.korona.core.metadata.options.KoronaOption;
import io.korona.server.KoronaDatabase;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class WriteTest {

    @Test
    public void test() throws IOException, InterruptedException {
        KoronaOption option = KoronaDatabase.findOption("");
        KoronaDatabase koronaDatabase = new KoronaDatabase(option);

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();

        int RANGE = 1000000;

        for (int j = 0; j < RANGE; j++) {
            koronaDatabase.put(j, j);
        }

        stopwatch.stop();

        //waiting data flush if that setting is enable

        long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("Elapsed time: " + millis + " ms");
    }

}
