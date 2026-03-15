package directory;

import model.ClientInfo;

import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Directory extends UnicastRemoteObject implements DirectoryInterface {

    private final Map<String, Set<ClientInfo>> fileIndex = new HashMap<>();
    private final Map<ClientInfo, Integer> loadMap = new HashMap<>();   
    private final Map<ClientInfo, Long> lastSeen = new HashMap<>();

    protected Directory() throws RemoteException {
        super();
    }

    @Override
    public synchronized void register(ClientInfo client, List<String> files) {
        for (String f : files) {
            fileIndex
                    .computeIfAbsent(f, k -> new HashSet<>())
                    .add(client);
        }
        loadMap.putIfAbsent(client, 0);
        lastSeen.put(client, System.currentTimeMillis());

        System.out.println(" REGISTER: " + client + " | Files: " + files.size());
    }

    @Override
    public synchronized void unregister(ClientInfo client) {
        for (Set<ClientInfo> set : fileIndex.values()) {
            set.remove(client);
        }
        loadMap.remove(client);
        lastSeen.remove(client);
        System.out.println(" UNREGISTER: " + client);
    }

    @Override
    public synchronized void heartbeat(ClientInfo client) {
        lastSeen.put(client, System.currentTimeMillis());
        loadMap.putIfAbsent(client, 0);
    }

    private synchronized void cleanup() {
        long now = System.currentTimeMillis();
        Iterator<ClientInfo> it = lastSeen.keySet().iterator();

        while (it.hasNext()) {
            ClientInfo c = it.next();
            if (now - lastSeen.get(c) > 10000) {   
                System.out.println(" REMOVE DEAD CLIENT: " + c);
                it.remove();
                loadMap.remove(c);
                for (Set<ClientInfo> s : fileIndex.values()) {
                    s.remove(c);
                }
            }
        }
    }

    @Override
    public synchronized List<ClientInfo> getSourcesSortedByLoad(String filename) {
        cleanup(); 

        Set<ClientInfo> owners = fileIndex.getOrDefault(filename, new HashSet<>());
        List<ClientInfo> list = new ArrayList<>(owners);   

        list.sort(Comparator.comparingInt(c -> loadMap.getOrDefault(c, 0)));

        return list;
    }

    @Override
    public synchronized void incrementLoad(ClientInfo c) {
        loadMap.put(c, loadMap.getOrDefault(c, 0) + 1);
    }

    @Override
    public synchronized void decrementLoad(ClientInfo c) {
        loadMap.put(c, Math.max(0, loadMap.getOrDefault(c, 0) - 1));
    }

    public static void main(String[] args) throws Exception {
        LocateRegistry.createRegistry(1099);
        Directory d = new Directory();
        Naming.rebind("Directory", d);

        System.out.println(" Directory server ready on port 1099");

        // Thread tự động cleanup dead clients
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);
                    d.cleanup();
                } catch (Exception ignored) {}
            }
        }).start();
    }
}