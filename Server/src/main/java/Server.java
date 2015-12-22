import java.io.IOException;
import java.net.InetSocketAddress;
import discovery.DiscoveryListener;
import transport.TransportListener;

/**
 * Created by Adrian on 11/22/2015.
 */
/**
 * if exec-maven-plugin installed
 * the app can be started in terminal with:
 * mvn exec:java -Dexec.args="5555"
 */
public class Server {
    public static void main(String[] args) throws IOException {
        int dataServerPort = 4444;
        if (args.length > 0) {
            dataServerPort = Integer.parseInt(args[0]);
        }

        InetSocketAddress serverLocation = new InetSocketAddress("127.0.0.1", dataServerPort);
        System.out.println("[INFO] -----------------------------------------\n" +
                "[INFO] Server is running... on " + dataServerPort);

        new DiscoveryListener(serverLocation);
        new TransportListener(dataServerPort);
    }
}
