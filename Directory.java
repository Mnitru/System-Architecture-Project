import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Directory extends UnicastRemoteObject implements DirectoryInterface {

    private Map<String, Set<ClientInfo>> fileToClients = new HashMap<>();
    private Map<ClientInfo, Integer> clientLoad = new HashMap<>();
    private Map<ClientInfo, Long> lastSeen = new HashMap<>();

    protected Directory() throws RemoteException {}

    @Override
    public synchronized void register(ClientInfo client, List<String> files) {

        for (String f : files) {

            fileToClients
                .computeIfAbsent(f, k -> new HashSet<>())
                .add(client);
        }

        clientLoad.putIfAbsent(client, 0);
        lastSeen.put(client, System.currentTimeMillis());
        System.out.println("REGISTER " + client);
    }

    @Override
    public synchronized void unregister(ClientInfo client) {

        for (Set<ClientInfo> s : fileToClients.values())
            s.remove(client);

        clientLoad.remove(client);

        System.out.println("UNREGISTER " + client);
    }

    @Override
    public synchronized List<ClientInfo> getSourcesForFile(String filename) {

        long now = System.currentTimeMillis();

        lastSeen.entrySet().removeIf(e ->
                now - e.getValue() > 30000
        );

        cleanupDeadClients();
        
        return new ArrayList<>(
                fileToClients.getOrDefault(filename, new HashSet<>())
        );
    }

    @Override
    public synchronized List<ClientInfo> getSourcesSortedByLoad(String filename) {

        List<ClientInfo> list = getSourcesForFile(filename);

        list.sort(Comparator.comparingInt(
                c -> clientLoad.getOrDefault(c, 0)
        ));

        return list;
    }

    @Override
    public synchronized void incrementLoad(ClientInfo c) {

        clientLoad.put(c, clientLoad.getOrDefault(c, 0) + 1);
    }

    @Override
    public synchronized void decrementLoad(ClientInfo c) {

        clientLoad.put(c,
                Math.max(0, clientLoad.getOrDefault(c, 0) - 1));
    }

    public static void main(String[] args) throws Exception {

        LocateRegistry.createRegistry(1099);

        Directory d = new Directory();

        Naming.rebind("Directory", d);

        System.out.println("Directory server ready");
    }
    private void cleanupDeadClients() {

        long now = System.currentTimeMillis();

        Iterator<ClientInfo> it = lastSeen.keySet().iterator();

        while (it.hasNext()) {

            ClientInfo c = it.next();

            if (now - lastSeen.get(c) > 30000) { // 30 seconds

                System.out.println("REMOVE DEAD CLIENT " + c);

                unregister(c);

                it.remove();
            }
        }
    }
}