package daemon;

import directory.DirectoryInterface;
import model.ClientInfo;

import java.io.File;
import java.net.ServerSocket;
import java.rmi.Naming;
import java.util.*;

public class Daemon {

    static String CLIENT_DIR;     
    static String SHARED_DIR;
    static String DOWNLOAD_DIR;
    static int SPEED_LIMIT_KBPS = 0;  

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.err.println(" Usage: java daemon.Daemon <port> [speedKbps]");
            System.err.println("   speedKbps = 0 (unlimited) hoặc 1000 = 1Mbps, 2000 = 2Mbps...");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        if (args.length >= 2) {
            SPEED_LIMIT_KBPS = Integer.parseInt(args[1]);
        }

        CLIENT_DIR   = "client_" + port;
        SHARED_DIR   = CLIENT_DIR + "/shared";
        DOWNLOAD_DIR = CLIENT_DIR + "/downloads";

        File clientFolder = new File(CLIENT_DIR);
        if (!clientFolder.exists()) {
            clientFolder.mkdir();
            System.out.println(" Created client folder: " + CLIENT_DIR);
        }

        File sharedFolder = new File(SHARED_DIR);
        if (!sharedFolder.exists()) {
            sharedFolder.mkdir();
            System.out.println("shared/ created");
        }

        File downloadFolder = new File(DOWNLOAD_DIR);
        if (!downloadFolder.exists()) {
            downloadFolder.mkdir();
            System.out.println(" downloads/ created");
        }

        DirectoryInterface directory =
                (DirectoryInterface) Naming.lookup("rmi://localhost/Directory");

        ClientInfo client = new ClientInfo("127.0.0.1", port);

        List<String> files = new ArrayList<>();
        File[] fileList = sharedFolder.listFiles();
        if (fileList != null) {
            for (File f : fileList) {
                if (f.isFile()) files.add(f.getName());
            }
        }

        directory.register(client, files);

        System.out.println("\nClient " + port + " structure:");
        System.out.println("   " + CLIENT_DIR + "/");
        System.out.println(" shared/     (" + files.size() + " files to share)");
        files.forEach(f -> System.out.println("   │   • " + f));
        System.out.println(" downloads/  (download destination)");

        System.out.println(" Speed limit: " + 
                (SPEED_LIMIT_KBPS == 0 ? "Unlimited" : SPEED_LIMIT_KBPS + " Kbps"));
        System.out.println(" Daemon registered successfully\n");

        startHeartbeat(directory, client);
        new Thread(() -> startFileServer(port)).start();

        System.out.println(" Daemon ready on port " + port + 
                " | Downloads will go to: " + DOWNLOAD_DIR + "\n");

        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print(" download file: ");
            String input = sc.nextLine().trim();

            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                directory.unregister(client);
                System.out.println(" Daemon shutting down...");
                System.exit(0);
            }

            if (!input.isEmpty()) {
                try {
                    DownloadManager.download(input, directory, port, DOWNLOAD_DIR);
                } catch (Exception e) {
                    System.err.println(" Download failed: " + e.getMessage());
                }
            }
        }
    }

    static void startHeartbeat(DirectoryInterface directory, ClientInfo client) {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    directory.heartbeat(client);
                } catch (Exception e) {
                    System.err.println(" Heartbeat failed: " + e.getMessage());
                    break;
                }
            }
        }).start();
    }

    static void startFileServer(int port) {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println(" FileServer listening on port " + port);

            while (true) {
                new Thread(new FileServer(server.accept(), SPEED_LIMIT_KBPS)).start();
            }
        } catch (Exception e) {
            System.err.println(" FileServer error: " + e.getMessage());
        }
    }
}