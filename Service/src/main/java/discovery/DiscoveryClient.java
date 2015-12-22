package discovery;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;



import model.Location;
import org.apache.commons.lang3.SerializationUtils;

import static model.ProtocolConfig.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;

/**
 * Created by Adrian on 11/22/2015.
 */
public class DiscoveryClient {
    private InetSocketAddress clientAddress;

    public DiscoveryClient(InetSocketAddress clientAddress) {
        this.clientAddress = clientAddress;
    }

    public Location retrieveLocation() throws IOException {
        sendLocationRequest();
        ArrayList<Location> locations = receiveLocations();

        for (int i = 0; i < locations.size(); i++) {
            if(locations.get(i).getLocation().getPort()==3333){
                return locations.get(i);
            }
        }
        return null;
    }

    /**
     * Receives UDP unicast datagrams which include
     * server locations of distributed data collections
     *
     * @throws IOException
     */
    private ArrayList<Location> receiveLocations() throws IOException {
        ArrayList<Location> locations = new ArrayList<Location>();
        DatagramSocket datagramServer = new DatagramSocket(clientAddress);
        byte dataFromServer[] = new byte[2048];
        boolean isTimeExpired = false;
        datagramServer.setSoTimeout((int) SECONDS.toMillis(PROTOCOL_GROUP_TIMEOUT));

        System.out.println("[INFO] -----------------------------------------\n" +
                "[INFO] Discovering... information nodes.");
        while (!isTimeExpired) {
            DatagramPacket pongPacket = new DatagramPacket(dataFromServer, dataFromServer.length);
            try {
                datagramServer.receive(pongPacket);
            } catch (SocketTimeoutException e) {
                System.out.println("[WARNING] -----------------------------------------\n" +
                        "[WARNING] Waiting time expired...");
                isTimeExpired = true;
                continue;
            }
            locations.add(SerializationUtils.<Location>deserialize(pongPacket.getData()));
            System.out.println("[INFO] " +
                    "Receiving reply from: (" +
                    pongPacket.getPort() + ", " +
                    pongPacket.getAddress().getHostAddress() + ")");
        }
        datagramServer.close();
        return locations;
    }

    /**
     * Sends UDP multicast request to node group of distributed system.
     * Request includes client address used by discovery listener.
     *
     * @throws IOException
     */

    private void sendLocationRequest() throws IOException {
        MulticastSocket s = new MulticastSocket();
        byte sendData[] = serialize(new Location(clientAddress));
        DatagramPacket pingPacket = new DatagramPacket(sendData, sendData.length,
                InetAddress.getByName(PROTOCOL_GROUP_ADDRESS),
                PROTOCOL_GROUP_PORT);
        s.send(pingPacket);
        s.close();
    }

}
