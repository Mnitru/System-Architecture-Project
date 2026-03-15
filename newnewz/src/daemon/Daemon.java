package daemon;

import directory.DirectoryInterface;
import model.ClientInfo;

import java.io.File;
import java.net.ServerSocket;
import java.rmi.Naming;
import java.util.*;

public class Daemon {

    static String SHARED_DIR;
    static String DOWNLOAD_DIR; 

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.err.println(" Usage: java Daemon <port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);

        SHARED_DIR = "shared_" + port;
        DOWNLOAD_DIR = "downloads_" + port;

        File sharedFolder = new File(SHARED_DIR);
        if (!sharedFolder.exists()) {
            sharedFolder.mkdir();
            System.out.println(" Created shared folder: " + SHARED_DIR);
        }

        File downloadFolder = new File(DOWNLOAD_DIR);
        if (!downloadFolder.exists()) {
            downloadFolder.mkdir();
            System.out.println(" Created download folder: " + DOWNLOAD_DIR);
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

        System.out.println(" Shared files (" + SHARED_DIR + "):");
        if (files.isEmpty()) {
            System.out.println("   (no files yet - put files into folder to share)");
        } else {
            files.forEach(f -> System.out.println("   • " + f));
        }
        System.out.println(" Daemon registered with " + files.size() + " files");

        startHeartbeat(directory, client);
        new Thread(() -> startFileServer(port)).start();

        System.out.println(" Daemon ready on port " + port + " | Downloads go to: " + DOWNLOAD_DIR + "\n");

        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print(" download file: ");
            String input = sc.nextLine().trim();

            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                directory.unregister(client);
                System.out.println(" Shutting down...");
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
                    System.err.println("Heartbeat failed: " + e.getMessage());
                    break;
                }
            }
        }).start();
    }

    static void startFileServer(int port) {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("🔌 FileServer listening on port " + port);
            while (true) {
                new Thread(new FileServer(server.accept())).start();
            }
        } catch (Exception e) {
            System.err.println("FileServer error: " + e.getMessage());
        }
    }
}