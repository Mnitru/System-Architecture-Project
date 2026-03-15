package directory;

import java.rmi.*;
import java.util.List;
import model.ClientInfo;

public interface DirectoryInterface extends Remote {

    void register(ClientInfo client, List<String> files) throws RemoteException;

    void unregister(ClientInfo client) throws RemoteException;

    List<ClientInfo> getSourcesSortedByLoad(String filename) throws RemoteException;

    void incrementLoad(ClientInfo c) throws RemoteException;

    void decrementLoad(ClientInfo c) throws RemoteException;

    void heartbeat(ClientInfo client) throws RemoteException;
}