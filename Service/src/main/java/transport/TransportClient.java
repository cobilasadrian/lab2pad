package transport;

import model.Employee;
import model.Location;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.commons.lang3.SerializationUtils.deserialize;

public class TransportClient {

    public ArrayList<Employee> getEmployeesFrom(Location location) throws IOException {
        Socket socket = new Socket();
        socket.connect(location.getLocation());
        Employee[] employees = (Employee[]) deserialize(socket.getInputStream());
        socket.close();
        return new ArrayList<Employee>(Arrays.asList(employees));
    }
}
