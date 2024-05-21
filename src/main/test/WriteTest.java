import io.korona.benchmark.Stopwatch;
import io.korona.core.metadata.options.KoronaOption;
import io.korona.server.KoronaDatabase;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

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

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        int RANGE = 1000000;

        for (int j = 0; j < RANGE; j++) {
            koronaDatabase.put(j, j);
        }

        //waiting data flush if that setting is enable
        stopwatch.stop();

        long millis = stopwatch.getElapsedTime();
        System.out.println("Elapsed time: " + millis + " ms");
    }

}
