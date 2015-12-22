package model;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class Location implements Serializable {
    private InetSocketAddress location;

    public Location() {
    }

    public Location(InetSocketAddress location) {
        this.location = location;
    }

    public Location(String ipAddress, int port) {
        location = new InetSocketAddress(ipAddress, port);
    }

    public InetSocketAddress getLocation() {
        return location;
    }

    public void setLocation(InetSocketAddress location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "Location{" +
                "ip address=" + location.getHostString() + ", " +
                "port=" + location.getPort() +
                '}';
    }
}
