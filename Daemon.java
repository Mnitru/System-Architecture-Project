import java.io.*;
import java.net.*;
import java.rmi.Naming;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class Daemon {

    static final String SHARED_DIR = "shared";

    public static void main(String[] args) throws Exception {

        int port = Integer.parseInt(args[0]);

        DirectoryInterface directory =
                (DirectoryInterface) Naming.lookup("rmi://localhost/Directory");

        ClientInfo client = new ClientInfo(
                InetAddress.getLocalHost().getHostAddress(),
                port
        );

        // lấy danh sách file trong folder shared
        File folder = new File(SHARED_DIR);
        List<String> files = new ArrayList<>();

        for (File f : folder.listFiles()) {
            files.add(f.getName());
        }

        // đăng ký với Directory
        directory.register(client, files);

        ServerSocket server = new ServerSocket(port);

        System.out.println("Daemon running on " + port);

        while (true) {

            Socket socket = server.accept();

            new Thread(() -> handle(socket)).start();
        }
    }

    static void handle(Socket socket) {

        try {

            DataInputStream in =
                    new DataInputStream(socket.getInputStream());

            GZIPOutputStream gzip =
                    new GZIPOutputStream(socket.getOutputStream());

            DataOutputStream out =
                    new DataOutputStream(gzip);

            String cmd = in.readUTF();

            if (!cmd.equals("GET")) return;

            String filename = in.readUTF();
            long start = in.readLong();
            long length = in.readLong();

            File file = new File(SHARED_DIR + "/" + filename);

            RandomAccessFile raf = new RandomAccessFile(file, "r");

            raf.seek(start);

            byte[] buffer = new byte[8192];

            long remaining = length;

            out.writeUTF("OK");

            while (remaining > 0) {

                int read = raf.read(
                        buffer,
                        0,
                        (int) Math.min(buffer.length, remaining)
                );

                if (read == -1) break;

                out.write(buffer, 0, read);

                remaining -= read;
            }

            out.flush();
            gzip.finish();

            raf.close();
            socket.close();

        } catch (Exception e) {

            System.out.println("Client disconnected");
        }
    }
}