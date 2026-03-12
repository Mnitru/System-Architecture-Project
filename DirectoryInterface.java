import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface DirectoryInterface extends Remote {

    void register(ClientInfo client, List<String> files) throws RemoteException;

    void unregister(ClientInfo client) throws RemoteException;

    List<ClientInfo> getSourcesForFile(String filename) throws RemoteException;

    List<ClientInfo> getSourcesSortedByLoad(String filename) throws RemoteException;

    void incrementLoad(ClientInfo client) throws RemoteException;

    void decrementLoad(ClientInfo client) throws RemoteException;
}