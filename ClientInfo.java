import java.io.Serializable;

public class ClientInfo implements Serializable {

    private String host;
    private int port;

    public ClientInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ClientInfo)) return false;
        ClientInfo c = (ClientInfo) o;
        return host.equals(c.host) && port == c.port;
    }

    @Override
    public int hashCode() {
        return host.hashCode() + port;
    }
}