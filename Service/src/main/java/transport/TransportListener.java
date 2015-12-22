package transport;

import model.Employee;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static model.ProtocolConfig.PROTOCOL_GROUP_TIMEOUT;
import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;

/**
 * Created by Adrian on 11/26/2015.
 */
public class TransportListener {
    private final static ExecutorService executor = Executors.newFixedThreadPool(10);
    private InetSocketAddress mavenLocation;

    public TransportListener(int dataServerPort) throws IOException {
        mavenLocation = new InetSocketAddress("127.0.0.1", 3333);
        switch (dataServerPort) {
            case 1111:
                sendDataUdp(mavenLocation, new Employee("Eugen", "Moraru", "Vinzari", 501.0));
                System.out.println("[INFO] -----------------------------------------\n" +
                        "[INFO] Data was sent to Maven");
                break;
            case 2222:
                sendDataUdp(mavenLocation, new Employee("Ion", "Pascaru", "Marketing", 502.0));
                System.out.println("[INFO] -----------------------------------------\n" +
                        "[INFO] Data was sent to Maven");
                break;
            case 3333:
                ArrayList<Employee> employees = receiveDataUdp(mavenLocation);
                sendDataTcp(dataServerPort,employees);
                System.out.println("[INFO] -----------------------------------------\n" +
                        "[INFO] Data was sent to client");
                break;
            case 4444:
                sendDataUdp(mavenLocation, new Employee("Adriana", "Munteanu", "Financiar", 503.0));
                System.out.println("[INFO] -----------------------------------------\n" +
                        "[INFO] Data was sent to Maven");
                break;
            case 5555:
                sendDataUdp(mavenLocation,new Employee("Valeriu", "Buzatu", "Tehnic", 504.0));
                System.out.println("[INFO] -----------------------------------------\n" +
                        "[INFO] Data was sent to Maven");
                break;
            case 6666:
                System.out.println("[INFO] ----------------------------------------- \n" +
                        "[INFO] This node is isolated.");
                break;
            default:
                System.out.println("[WARNING] ----------------------------------------- \n" +
                        "[WARNING] Server port is not correct.");
                break;
        }
    }

    private void sendDataTcp(final int dataServerPort, final ArrayList<Employee> employees) throws IOException {
        ServerSocket serverSocket = new ServerSocket(dataServerPort);
        serverSocket.setSoTimeout((int) SECONDS.toMillis(100));
        boolean isTimeExpired = false;
        while (!isTimeExpired) {
            try {
                Socket socket = serverSocket.accept();  // Blocking call!
                Employee[] s = new Employee[employees.size()];
                serialize((Employee[]) employees.toArray(s), socket.getOutputStream());
                socket.close();
            } catch (SocketTimeoutException e) {
                System.out.println("[WARNING] ----------------------------------------- \n" +
                        "[WARNING] Waiting time expired... Socket is closed.");
                isTimeExpired = true;
                continue;
            }
        }
        serverSocket.close();
    }


    private Future sendDataUdp(final InetSocketAddress clientLocation,final Employee employee) {
        return executor.submit(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(SECONDS.toMillis(1));
                    DatagramSocket clientSocket = new DatagramSocket();
                    byte[] sendDataServer = serialize((Employee) employee);
                    DatagramPacket pongPacket = new DatagramPacket(sendDataServer,
                            sendDataServer.length,
                            clientLocation);
                    clientSocket.send(pongPacket);
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private ArrayList<Employee> receiveDataUdp(InetSocketAddress clientAddress) throws IOException {
        ArrayList<Employee> employees = new ArrayList<Employee>();
        DatagramSocket datagramServer = new DatagramSocket(clientAddress);
        byte dataFromServer[] = new byte[2048];
        boolean isTimeExpired = false;
        datagramServer.setSoTimeout((int) SECONDS.toMillis(PROTOCOL_GROUP_TIMEOUT-5));

        System.out.println("[INFO] -----------------------------------------\n" +
                "[INFO] Collecting information from nodes....");
        while (!isTimeExpired) {
            DatagramPacket datagramPacketacket = new DatagramPacket(dataFromServer, dataFromServer.length);
            try {
                datagramServer.receive(datagramPacketacket);
            } catch (SocketTimeoutException e) {
                System.out.println("[WARNING] -----------------------------------------\n" +
                        "[WARNING] Waiting time expired...");
                isTimeExpired = true;
                continue;
            }
            Employee employee = (Employee) deserialize(datagramPacketacket.getData());
            System.out.println("[INFO] " +
                    "Received employee: " +
                    employee.toString());
            employees.add(employee);
        }
        datagramServer.close();
        return employees;
    }
}
