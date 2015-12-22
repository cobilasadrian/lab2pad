package discovery;

import model.Location;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

import static model.ProtocolConfig.PROTOCOL_GROUP_ADDRESS;
import static model.ProtocolConfig.PROTOCOL_GROUP_PORT;
import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
/**
 * Created by Adrian on 11/22/2015.
 */
public class DiscoveryListener {

    private final static ExecutorService executor = Executors.newFixedThreadPool(10);

    public DiscoveryListener(InetSocketAddress dataServerAddress) {
        try {
            sendDataServerLocation(receiveClientRequest().get(),
                                    new Location(dataServerAddress));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Receives request through UDP multicast and returns client location where
     * "discovery" listener must send information (address and port) about data
     * transport server.
     */
    private Future<Location> receiveClientRequest(){
        return executor.submit(new Callable<Location>(){
            public Location call(){
                Location clientLocation = null;
                try {
                    MulticastSocket s = new MulticastSocket(PROTOCOL_GROUP_PORT);
                    s.joinGroup(InetAddress.getByName(PROTOCOL_GROUP_ADDRESS));

                    byte buf[] = new byte[2048];
                    DatagramPacket pingPacket = new DatagramPacket(buf, buf.length);
                    s.receive(pingPacket);

                    clientLocation = deserialize(pingPacket.getData());

                    System.out.println("[INFO] -----------------------------------------\n" +
                            "[INFO] Received location request...");

                    s.leaveGroup(InetAddress.getByName(PROTOCOL_GROUP_ADDRESS));
                    s.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return clientLocation;
            }
        });
    }

    /**
     * Sends data server location using UDP unicast
     *
     * @param clientLocation where information is send
     * @param serverLocation where client app can request data collection
     */
    private Future sendDataServerLocation(final Location clientLocation,final Location serverLocation) {
        return executor.submit(new Runnable() {
            public void run() {
                try {
                    byte[] sendDataServerAddress = serialize(serverLocation);
                    DatagramSocket clientSocket = new DatagramSocket();
                    DatagramPacket pongPacket = new DatagramPacket(sendDataServerAddress,
                            sendDataServerAddress.length,
                            clientLocation.getLocation());
                    clientSocket.send(pongPacket);
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
