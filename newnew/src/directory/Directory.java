package directory;

import model.ClientInfo;

import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Directory extends UnicastRemoteObject implements DirectoryInterface {

    private Map<String, Set<ClientInfo>> fileIndex = new HashMap<>();
    private Map<ClientInfo, Integer> load = new HashMap<>();
    private Map<ClientInfo, Long> lastSeen = new HashMap<>();

    protected Directory() throws RemoteException {}

    public synchronized void register(ClientInfo client, List<String> files) {

        for (String f : files) {

            fileIndex
                    .computeIfAbsent(f, k -> new HashSet<>())
                    .add(client);
        }

        load.putIfAbsent(client, 0);
        lastSeen.put(client, System.currentTimeMillis());

        System.out.println("REGISTER " + client);
    }

    public synchronized void unregister(ClientInfo client) {

        for (Set<ClientInfo> set : fileIndex.values())
            set.remove(client);

        load.remove(client);
        lastSeen.remove(client);
    }

    public synchronized void heartbeat(ClientInfo client) {

        lastSeen.put(client, System.currentTimeMillis());
    }

    private synchronized void cleanup() {

        long now = System.currentTimeMillis();

        Iterator<ClientInfo> it = lastSeen.keySet().iterator();

        while (it.hasNext()) {

            ClientInfo c = it.next();

            if (now - lastSeen.get(c) > 10000) {

                System.out.println("REMOVE DEAD " + c);

                it.remove();  // remove from lastSeen

                load.remove(c);

                for (Set<ClientInfo> s : fileIndex.values()) {
                    s.remove(c);
                }
            }
        }
    }

    public synchronized List<ClientInfo> getSourcesSortedByLoad(String filename) {

        cleanup();

        List<ClientInfo> list =
                new ArrayList<>(fileIndex.getOrDefault(filename, new HashSet<>()));

        list.sort(Comparator.comparingInt(c -> load.getOrDefault(c, 0)));

        return list;
    }

    public synchronized void incrementLoad(ClientInfo c) {

        load.put(c, load.getOrDefault(c, 0) + 1);
    }

    public synchronized void decrementLoad(ClientInfo c) {

        load.put(c, Math.max(0, load.getOrDefault(c, 0) - 1));
    }

    public static void main(String[] args) throws Exception {

        LocateRegistry.createRegistry(1099);

        Directory d = new Directory();

        Naming.rebind("Directory", d);

        System.out.println("Directory server ready");

        // AUTO CLEANUP THREAD
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