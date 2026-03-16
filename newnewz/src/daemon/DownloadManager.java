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

    private static List<ClientInfo> currentSources;
    private static AtomicLong totalDownloaded;
    private static long fileSize;
    private static final boolean USE_COMPRESSION = false; 

    private static final Map<ClientInfo, AtomicLong> bytesPerSource = new ConcurrentHashMap<>();
    private static final Map<ClientInfo, Long> lastBytes = new ConcurrentHashMap<>();
    private static final Map<ClientInfo, Long> lastTime = new ConcurrentHashMap<>();

    public static void download(String filename,
                                DirectoryInterface directory,
                                int myPort,
                                String downloadDir) throws Exception {

        long startTime = System.currentTimeMillis();

        String partFilePath = downloadDir + "/" + filename + ".part";
        String finalFilePath = downloadDir + "/" + filename;

        File partFile = new File(partFilePath);
        File finalFile = new File(finalFilePath);

        if (finalFile.exists()) {
            System.out.println(" File already completed: " + finalFilePath);
            return;
        }

        long alreadyDownloaded = partFile.exists() ? partFile.length() : 0;
        if (alreadyDownloaded > 0) {
            System.out.println(" RESUMING from " + formatBytes(alreadyDownloaded) + " in " + downloadDir);
        } else {
            new File(downloadDir).mkdirs();
        }

        List<ClientInfo> sources = directory.getSourcesSortedByLoad(filename);
        sources.removeIf(c -> c.getPort() == myPort);

        if (sources.isEmpty()) {
            System.out.println(" No source found");
            return;
        }

        int numSources = sources.size();
        int totalThreads = numSources * 2;  

        System.out.println("🚀 Using " + numSources + " sources with " + totalThreads + " threads");
        sources.forEach(c -> System.out.println("  → " + c));

        for (ClientInfo s : sources) {
            bytesPerSource.put(s, new AtomicLong(0));
            lastBytes.put(s, 0L);
            lastTime.put(s, System.currentTimeMillis());
        }

        currentSources = new CopyOnWriteArrayList<>(sources);

        ScheduledExecutorService refresher = Executors.newSingleThreadScheduledExecutor();
        refresher.scheduleAtFixedRate(() -> refreshSources(directory, filename, myPort), 5, 5, TimeUnit.SECONDS);

        fileSize = getFileSize(sources.get(0), filename);
        System.out.println(" File size: " + formatBytes(fileSize));

        if (alreadyDownloaded >= fileSize) {
            partFile.renameTo(finalFile);
            refresher.shutdownNow();
            return;
        }

        RandomAccessFile output = new RandomAccessFile(partFile, "rw");
        totalDownloaded = new AtomicLong(alreadyDownloaded);

        System.out.println(" Measuring real download speed of all sources (5MB sample × 2)...");
        double[] speedsKbps = measureSpeeds(sources, filename);
        double totalSpeed = Arrays.stream(speedsKbps).sum();

        System.out.printf("Total estimated speed: %.1f Kbps%n", totalSpeed);
        for (int i = 0; i < sources.size(); i++) {
            System.out.printf("  → %s : %.1f Kbps%n", sources.get(i), speedsKbps[i]);
        }

        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);

        long remaining = fileSize - alreadyDownloaded;
        long currentStart = alreadyDownloaded;

        for (int i = 0; i < numSources; i++) {
            double ratio = (totalSpeed > 0) ? speedsKbps[i] / totalSpeed : 1.0 / numSources;
            long bytesForThisSource = (long) (remaining * ratio);
            bytesForThisSource = Math.max(2 * 1024 * 1024, Math.min(bytesForThisSource, remaining));

            long subChunkSize = bytesForThisSource / 2;
            for (int sub = 0; sub < 2; sub++) {
                long thisLen = (sub == 1) ? bytesForThisSource - subChunkSize : subChunkSize;

                if (thisLen <= 0) continue;
                if (currentStart + thisLen > fileSize) thisLen = fileSize - currentStart;

                final long fStart = currentStart;
                final long fLength = thisLen;
                final ClientInfo fSource = sources.get(i);

                pool.submit(() -> downloadChunkWithFixedSource(
                        filename, fStart, fLength, output, directory, fSource));

                currentStart += thisLen;
            }
        }

        if (currentStart < fileSize) {
            final long fStart = currentStart;
            final long fLength = fileSize - currentStart;
            final ClientInfo fallback = sources.get(0);
            pool.submit(() -> downloadChunkWithFixedSource(
                    filename, fStart, fLength, output, directory, fallback));
        }

        Thread progressThread = new Thread(() -> {
            long lastTotalBytes = totalDownloaded.get();
            long lastTotalTime = System.currentTimeMillis();

            try {
                while (!pool.isTerminated()) {
                    long done = totalDownloaded.get();
                    int percent = fileSize > 0 ? (int) (done * 100L / fileSize) : 0;
                    long now = System.currentTimeMillis();

                    long deltaTotal = done - lastTotalBytes;
                    long deltaTTotal = now - lastTotalTime;
                    long totalSpeedKBps = (deltaTTotal > 0) ? deltaTotal * 1000 / deltaTTotal / 1024 : 0;

                    StringBuilder speedStr = new StringBuilder();
                    for (ClientInfo s : currentSources) {
                        long curr = bytesPerSource.getOrDefault(s, new AtomicLong(0)).get();
                        long prev = lastBytes.getOrDefault(s, 0L);
                        long dt = now - lastTime.getOrDefault(s, now);
                        long srcSpeedKBps = (dt > 0) ? (curr - prev) * 1000 / dt / 1024 : 0;
                        speedStr.append(s.getPort()).append(":").append(srcSpeedKBps).append("KB/s ");
                        lastBytes.put(s, curr);
                        lastTime.put(s, now);
                    }

                    System.out.printf("\r[Progress] %3d%% | %10d / %10d | Total: %4d KB/s | %s",
                            percent, done, fileSize, totalSpeedKBps, speedStr);

                    lastTotalBytes = done;
                    lastTotalTime = now;

                    Thread.sleep(500);
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

        System.out.println("\n Download complete!");
        System.out.println("   MD5: " + md5);
        System.out.println("   Time: " + (endTime - startTime) + " ms");

        if (partFile.renameTo(finalFile)) {
            System.out.println(" Saved to: " + finalFilePath);
        }
    }

    private static double[] measureSpeeds(List<ClientInfo> sources, String filename) throws Exception {
        double[] speeds = new double[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            speeds[i] = measureSingleSpeed(sources.get(i), filename);
        }
        return speeds;
    }

    private static double measureSingleSpeed(ClientInfo source, String filename) throws Exception {
        long testSize = 5 * 1024 * 1024; 
        double totalBps = 0;
        int trials = 2;

        for (int t = 0; t < trials; t++) {
            long start = System.currentTimeMillis();
            long bytes = 0;

            try (Socket socket = new Socket(source.getHost(), source.getPort());
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 InputStream in = socket.getInputStream()) {

                out.writeUTF("GET");
                out.writeUTF(filename);
                out.writeLong(0);
                out.writeLong(testSize);

                byte[] buf = new byte[131072];
                int r;
                while (bytes < testSize && (r = in.read(buf)) > 0) {
                    bytes += r;
                }
            }

            long ms = System.currentTimeMillis() - start;
            double bps = (ms > 0) ? (bytes * 1000.0) / ms : 0;
            totalBps += bps;
        }

        return (totalBps / trials) / 1024; 
    }

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
        ClientInfo current = fixedSource;

        while (downloaded < chunkLength) {
            try {
                directory.incrementLoad(current);

                Socket socket = new Socket(current.getHost(), current.getPort());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF("GET");
                out.writeUTF(filename);
                out.writeLong(chunkStart + downloaded);
                out.writeLong(chunkLength - downloaded);

                InputStream rawIn = socket.getInputStream();
                InputStream dataIn = USE_COMPRESSION ? new GZIPInputStream(rawIn) : rawIn;

                byte[] buffer = new byte[131072];
                long remaining = chunkLength - downloaded;
                int read;

                while (remaining > 0 && (read = dataIn.read(buffer, 0, (int) Math.min(buffer.length, remaining))) > 0) {
                    synchronized (output) {
                        output.seek(chunkStart + downloaded);
                        output.write(buffer, 0, read);
                    }
                    downloaded += read;
                    totalDownloaded.addAndGet(read);
                    bytesPerSource.get(current).addAndGet(read);
                    remaining -= read;
                }

                socket.close();
                directory.decrementLoad(current);
                return;

            } catch (Exception e) {
                try { directory.decrementLoad(current); } catch (Exception ignored) {}
                System.out.println(" Source " + current.getPort() + " failed → switching");

                int idx = currentSources.indexOf(current);
                if (idx != -1 && !currentSources.isEmpty()) {
                    current = currentSources.get((idx + 1) % currentSources.size());
                } else {
                    return; 
                }
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

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }
}