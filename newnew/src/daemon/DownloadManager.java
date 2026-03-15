package daemon;

import directory.DirectoryInterface;
import model.ClientInfo;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class DownloadManager {

    static final int THREADS = 8;

    public static void download(String filename,
                                DirectoryInterface directory,
                                int myPort)
            throws Exception {

        long startTime = System.currentTimeMillis();

        List<ClientInfo> sources =
                directory.getSourcesSortedByLoad(filename);

        // remove self source
        sources.removeIf(c -> c.getPort() == myPort);

        if (sources.isEmpty()) {

            System.out.println("No source found");
            return;
        }

        System.out.println("Sources: " + sources.size());

        for (ClientInfo c : sources) {

            System.out.println(
                    "Source: " +
                    c.getHost() + ":" +
                    c.getPort()
            );
        }

        // get file size
        ClientInfo first = sources.get(0);

        Socket socket =
                new Socket(first.getHost(), first.getPort());

        DataOutputStream out =
                new DataOutputStream(socket.getOutputStream());

        DataInputStream in =
                new DataInputStream(socket.getInputStream());

        out.writeUTF("SIZE");
        out.writeUTF(filename);

        long size = in.readLong();

        socket.close();

        System.out.println("File size: " + size + " bytes");

        // create downloads folder
        File folder = new File("downloads");
        if (!folder.exists()) folder.mkdir();

        RandomAccessFile output =
                new RandomAccessFile(
                        "downloads/" + filename, "rw"
                );

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
                            output,
                            directory
                    )
            );
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);

        output.close();

        long endTime = System.currentTimeMillis();

        System.out.println("Download complete");

        System.out.println(
                "Download time: " +
                (endTime - startTime) + " ms"
        );
    }

    static void downloadChunk(
            List<ClientInfo> sources,
            String filename,
            long start,
            long length,
            RandomAccessFile output,
            DirectoryInterface directory) {

        long downloaded = 0;

        while (downloaded < length) {

            for (ClientInfo source : sources) {

                try {

                    directory.incrementLoad(source);

                    System.out.println(
                            "Downloading chunk from " +
                            source.getHost() +
                            ":" +
                            source.getPort()
                    );

                    Socket socket =
                            new Socket(
                                    source.getHost(),
                                    source.getPort()
                            );

                    DataOutputStream out =
                            new DataOutputStream(
                                    socket.getOutputStream()
                            );

                    DataInputStream in =
                            new DataInputStream(
                                    socket.getInputStream()
                            );

                    out.writeUTF("GET");
                    out.writeUTF(filename);
                    out.writeLong(start + downloaded);
                    out.writeLong(length - downloaded);

                    byte[] buffer = new byte[65536];

                    while (downloaded < length) {

                        int read = in.read(buffer);

                        if (read == -1) break;

                        synchronized (output) {

                            output.seek(start + downloaded);

                            output.write(buffer, 0, read);
                        }

                        downloaded += read;
                    }

                    directory.decrementLoad(source);

                    socket.close();

                    return;

                } catch (Exception e) {

                    System.out.println(
                            "Source "
                                    + source.getPort()
                                    + " failed, trying next source"
                    );

                    try {
                        directory.decrementLoad(source);
                    } catch (Exception ignored) {}
                }
            }

            System.out.println("All sources failed, retrying...");
        }
    }
}