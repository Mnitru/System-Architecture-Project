package model;

import java.io.Serializable;
import java.util.Objects;

public class ClientInfo implements Serializable {

    private String host;
    private int port;
    
    private int currentLoad; 

    public ClientInfo(String host, int port) {
        this.host = host;
        this.port = port;
        this.currentLoad = 0;        
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getCurrentLoad() {
        return currentLoad;
    }

    public void setCurrentLoad(int currentLoad) {
        this.currentLoad = currentLoad;
    }

    public void incrementLoad() {
        this.currentLoad++;
    }

    public void decrementLoad() {
        if (this.currentLoad > 0) this.currentLoad--;
    }

    public String toString() {
        return host + ":" + port + " (load=" + currentLoad + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ClientInfo)) return false;
        ClientInfo c = (ClientInfo) o;
        return host.equals(c.host) && port == c.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);   
    }
}