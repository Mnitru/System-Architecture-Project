import java.io.*;
import java.net.*;
import java.rmi.Naming;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

public class Download {

    static final int THREADS = 4;

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        String filename = args[0];

        DirectoryInterface directory =
                (DirectoryInterface) Naming.lookup("rmi://localhost/Directory");

        List<ClientInfo> sources =
                directory.getSourcesSortedByLoad(filename);

        System.out.println("Sources: " + sources.size());

        for (ClientInfo c : sources) {
            System.out.println("Source: " + c.getHost() + ":" + c.getPort());
        }

        if (sources.isEmpty()) {
            System.out.println("No source available");
            return;
        }

        // lấy size file từ source đầu
        ClientInfo first = sources.get(0);

        Socket socket = new Socket(first.getHost(), first.getPort());

        DataOutputStream out =
                new DataOutputStream(socket.getOutputStream());

        DataInputStream in =
                new DataInputStream(socket.getInputStream());

        out.writeUTF("SIZE");
        out.writeUTF(filename);

        long size = in.readLong();

        socket.close();

        System.out.println("File size: " + size);

        // tạo folder downloads
        File folder = new File("downloads");
        if (!folder.exists()) folder.mkdir();

        RandomAccessFile output =
                new RandomAccessFile("downloads/" + filename, "rw");

        ExecutorService pool =
                Executors.newFixedThreadPool(THREADS);

        long chunkSize = size / THREADS;

        for (int i = 0; i < THREADS; i++) {

            long start = i * chunkSize;

            long length =
                    (i == THREADS - 1)
                            ? size - start
                            : chunkSize;

            pool.submit(() ->
                    downloadChunk(
                            sources,
                            filename,
                            start,
                            length,
                            output
                    )
            );
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);

        output.close();

        System.out.println("Download finished");

        long endTime = System.currentTimeMillis();

        System.out.println(
                "Download time: " + (endTime - startTime) + " ms"
        );
    }

    static void downloadChunk(
            List<ClientInfo> sources,
            String filename,
            long start,
            long length,
            RandomAccessFile output
    ) {

        long downloaded = 0;

        while (downloaded < length) {

            for (ClientInfo source : sources) {

                try {

                    Socket socket =
                            new Socket(source.getHost(), source.getPort());

                    DataOutputStream out =
                            new DataOutputStream(socket.getOutputStream());

                    GZIPInputStream gzip =
                            new GZIPInputStream(socket.getInputStream());

                    DataInputStream in =
                            new DataInputStream(gzip);

                    out.writeUTF("GET");
                    out.writeUTF(filename);
                    out.writeLong(start + downloaded);
                    out.writeLong(length - downloaded);

                    String resp = in.readUTF();

                    if (!resp.equals("OK")) {
                        socket.close();
                        continue;
                    }

                    byte[] buffer = new byte[8192];

                    while (downloaded < length) {

                        int read = in.read(buffer);

                        if (read == -1) break;

                        synchronized (output) {

                            output.seek(start + downloaded);

                            output.write(buffer, 0, read);
                        }

                        downloaded += read;
                    }

                    socket.close();

                    return;

                } catch (Exception e) {

                    System.out.println(
                            "Source " + source.getPort() + " failed, trying next source"
                    );
                }
            }
        }
    }
}