package daemon;

import directory.DirectoryInterface;
import model.ClientInfo;

import java.io.*;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class DownloadManager {

    static final int THREADS = 16;
    private static List<ClientInfo> currentSources;
    private static AtomicLong totalDownloaded;
    private static long fileSize;
    private static final boolean USE_COMPRESSION = true;

    public static void download(String filename,
                                DirectoryInterface directory,
                                int myPort,
                                String downloadDir) throws Exception {

        long startTime = System.currentTimeMillis();

        String partFilePath  = downloadDir + "/" + filename + ".part";
        String finalFilePath = downloadDir + "/" + filename;

        File partFile  = new File(partFilePath);
        File finalFile = new File(finalFilePath);

        if (finalFile.exists()) {
            System.out.println("✅ File already completed");
            return;
        }

        long alreadyDownloaded = partFile.exists() ? partFile.length() : 0;
        if (alreadyDownloaded > 0) {
            System.out.println("🔄 RESUMING from " + alreadyDownloaded + " bytes");
        } else {
            new File(downloadDir).mkdirs();
        }

        List<ClientInfo> sources = directory.getSourcesSortedByLoad(filename);
        sources.removeIf(c -> c.getPort() == myPort);

        if (sources.isEmpty()) {
            System.out.println("No source found");
            return;
        }

        System.out.println("Using " + sources.size() + " sources in parallel (round-robin - no speed measurement)");
        sources.forEach(c -> System.out.println("  → " + c));

        currentSources = new CopyOnWriteArrayList<>(sources);

        ScheduledExecutorService refresher = Executors.newSingleThreadScheduledExecutor();
        refresher.scheduleAtFixedRate(() -> refreshSources(directory, filename, myPort), 5, 5, TimeUnit.SECONDS);

        fileSize = getFileSize(sources.get(0), filename);
        System.out.println("File size: " + fileSize + " bytes");

        if (alreadyDownloaded >= fileSize) {
            partFile.renameTo(finalFile);
            refresher.shutdownNow();
            return;
        }

        RandomAccessFile output = new RandomAccessFile(partFile, "rw");
        totalDownloaded = new AtomicLong(alreadyDownloaded);

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);

        long remaining = fileSize - alreadyDownloaded;
        long chunkSize = remaining / THREADS;

        // ROUND-ROBIN THUẦN – TẤT CẢ NGUỒN ĐỀU ĐƯỢC DÙNG
        for (int i = 0; i < THREADS; i++) {
            long chunkStart = alreadyDownloaded + (i * chunkSize);
            long chunkLength = (i == THREADS - 1) ? fileSize - chunkStart : chunkSize;

            ClientInfo assignedSource = sources.get(i % sources.size());

            final long fStart = chunkStart;
            final long fLength = chunkLength;
            final ClientInfo fSource = assignedSource;

            pool.submit(() -> downloadChunkWithFixedSource(filename, fStart, fLength, output, directory, fSource));
        }

        // Progress bar
        Thread progressThread = new Thread(() -> {
            try {
                while (!pool.isTerminated()) {
                    long done = totalDownloaded.get();
                    int percent = (int) (done * 100 / fileSize);
                    System.out.printf("\r[Progress] %3d%% | %10d / %10d bytes | Sources: %2d ",
                            percent, done, fileSize, currentSources.size());
                    Thread.sleep(600);
                }
            } catch (InterruptedException ignored) {}
            System.out.println();
        });
        progressThread.start();

        pool.shutdown();
        pool.awaitTermination(2, TimeUnit.HOURS);

        progressThread.interrupt();
        refresher.shutdownNow();
        output.close();

        String md5 = computeMD5(partFilePath);
        long endTime = System.currentTimeMillis();

        System.out.println("\n✅ Download complete!");
        System.out.println("   MD5: " + md5);
        System.out.println("   Time: " + (endTime - startTime) + " ms");

        partFile.renameTo(finalFile);
        System.out.println("📁 Saved to: " + finalFilePath);
    }

    // ==================== CÁC HÀM HỖ TRỢ ====================

    private static long getFileSize(ClientInfo source, String filename) throws Exception {
        try (Socket s = new Socket(source.getHost(), source.getPort());
             DataOutputStream out = new DataOutputStream(s.getOutputStream());
             DataInputStream in = new DataInputStream(s.getInputStream())) {
            out.writeUTF("SIZE");
            out.writeUTF(filename);
            return in.readLong();
        }
    }

    private static void refreshSources(DirectoryInterface directory, String filename, int myPort) {
        try {
            List<ClientInfo> newList = directory.getSourcesSortedByLoad(filename);
            newList.removeIf(c -> c.getPort() == myPort);
            currentSources.clear();
            currentSources.addAll(newList);
        } catch (Exception ignored) {}
    }

    private static void downloadChunkWithFixedSource(String filename,
                                                     long chunkStart,
                                                     long chunkLength,
                                                     RandomAccessFile output,
                                                     DirectoryInterface directory,
                                                     ClientInfo fixedSource) {

        long downloaded = 0;
        ClientInfo currentSource = fixedSource;

        while (downloaded < chunkLength) {
            try {
                directory.incrementLoad(currentSource);

                Socket socket = new Socket(currentSource.getHost(), currentSource.getPort());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF("GET");
                out.writeUTF(filename);
                out.writeLong(chunkStart + downloaded);
                out.writeLong(chunkLength - downloaded);

                InputStream rawIn = socket.getInputStream();
                InputStream dataIn = USE_COMPRESSION ? new GZIPInputStream(rawIn) : rawIn;

                byte[] buffer = new byte[65536];
                long remaining = chunkLength - downloaded;
                int read;

                while (remaining > 0 && (read = dataIn.read(buffer, 0, (int) Math.min(buffer.length, remaining))) > 0) {
                    synchronized (output) {
                        output.seek(chunkStart + downloaded);
                        output.write(buffer, 0, read);
                    }
                    downloaded += read;
                    totalDownloaded.addAndGet(read);
                    remaining -= read;
                }

                socket.close();
                directory.decrementLoad(currentSource);
                return;

            } catch (Exception e) {
                try { directory.decrementLoad(currentSource); } catch (Exception ignored) {}
                System.out.println("⚠️ Source " + currentSource.getPort() + " failed → switching");

                int idx = currentSources.indexOf(currentSource);
                currentSource = currentSources.get((idx + 1) % currentSources.size());
            }
        }
    }

    private static String computeMD5(String filePath) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try (FileInputStream fis = new FileInputStream(filePath)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                md.update(buffer, 0, bytesRead);
            }
        }
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }
}