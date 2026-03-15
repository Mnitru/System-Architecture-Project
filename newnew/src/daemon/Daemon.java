package daemon;

import directory.DirectoryInterface;
import model.ClientInfo;

import java.io.File;
import java.net.ServerSocket;
import java.rmi.Naming;
import java.util.*;

public class Daemon {

    static String SHARED_DIR;

    public static void main(String[] args) throws Exception {

        int port = Integer.parseInt(args[0]);

        SHARED_DIR = "shared_" + port;

        File folder = new File(SHARED_DIR);
        if (!folder.exists()) folder.mkdir();

        DirectoryInterface directory =
                (DirectoryInterface) Naming.lookup("rmi://localhost/Directory");

        ClientInfo client = new ClientInfo("127.0.0.1", port);

        List<String> files = new ArrayList<>();

        for (File f : folder.listFiles())
            files.add(f.getName());

        directory.register(client, files);

        startHeartbeat(directory, client);

        new Thread(() -> startFileServer(port)).start();

        // COMMAND LINE DOWNLOAD
        Scanner sc = new Scanner(System.in);

        while (true) {

            System.out.print("download file: ");

            String filename = sc.nextLine();

            DownloadManager.download(filename, directory, port);
        }
    }

    static void startHeartbeat(DirectoryInterface directory, ClientInfo client) {

        new Thread(() -> {

            while (true) {

                try {

                    Thread.sleep(5000);

                    directory.heartbeat(client);

                } catch (Exception ignored) {}

            }

        }).start();
    }

    static void startFileServer(int port) {

        try {

            ServerSocket server = new ServerSocket(port);

            System.out.println("Daemon running on " + port);

            while (true) {

                new Thread(
                        new FileServer(server.accept())
                ).start();
            }

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}