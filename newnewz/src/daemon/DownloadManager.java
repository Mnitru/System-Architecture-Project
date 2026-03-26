package daemon;

import directory.DirectoryInterface;
import model.ClientInfo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class DownloadManager {

    private static final int MAX_WORKERS = 16;
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final long CHUNK_SIZE = 1L * 1024 * 1024; // 1 MB
    private static final boolean USE_COMPRESSION = true;

    private static List<ClientInfo> currentSources;
    private static AtomicLong totalDownloaded;
    private static long fileSize;

    public static void download(String filename,
            DirectoryInterface directory,
            int myPort,
            String downloadDir) throws Exception {

        long startTime = System.currentTimeMillis();

        Path downloadPath = Path.of(downloadDir);
        Files.createDirectories(downloadPath);

        Path partPath = downloadPath.resolve(filename + ".part");
        Path finalPath = downloadPath.resolve(filename);
        Path metaPath = downloadPath.resolve(filename + ".meta");

        if (Files.exists(finalPath)) {
            System.out.println("File already completed: " + finalPath);
            return;
        }

        List<ClientInfo> sources = directory.getSourcesSortedByLoad(filename);
        sources.removeIf(c -> c.getPort() == myPort);

        if (sources.isEmpty()) {
            System.out.println("No source found for: " + filename);
            return;
        }

        currentSources = new CopyOnWriteArrayList<>(sources);

        ScheduledExecutorService refresher = Executors.newSingleThreadScheduledExecutor();
        refresher.scheduleAtFixedRate(
                () -> refreshSources(directory, filename, myPort),
                5,
                5,
                TimeUnit.SECONDS);

        fileSize = getFileSize(sources.get(0), filename);
        System.out.println("Found " + currentSources.size() + " sources:");
        currentSources.forEach(c -> System.out.println("  -> " + c));
        System.out.println("File size: " + fileSize + " bytes");

        if (!Files.exists(partPath)) {
            Files.createFile(partPath);
        }

        try (FileChannel channel = FileChannel.open(
                partPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {
            channel.truncate(fileSize);

            DownloadState state = loadOrCreateState(metaPath, fileSize);
            totalDownloaded = new AtomicLong(state.completedBytes(fileSize));

            if (state.isComplete()) {
                finalizeDownload(partPath, finalPath, metaPath, startTime);
                refresher.shutdownNow();
                return;
            }

            BlockingQueue<ChunkTask> queue = new LinkedBlockingQueue<>();
            AtomicInteger remainingChunks = new AtomicInteger(0);
            enqueueMissingChunks(state, queue, remainingChunks, fileSize);

            if (remainingChunks.get() == 0) {
                finalizeDownload(partPath, finalPath, metaPath, startTime);
                refresher.shutdownNow();
                return;
            }

            int workerCount = Math.min(Math.min(MAX_WORKERS, currentSources.size()), remainingChunks.get());
            workerCount = Math.max(workerCount, 1);

            System.out.println("Starting parallel download with " + workerCount + " active source workers");

            AtomicBoolean stopSignal = new AtomicBoolean(false);
            ExecutorService pool = Executors.newFixedThreadPool(workerCount);

            for (int i = 0; i < workerCount; i++) {
                ClientInfo source = currentSources.get(i % currentSources.size());
                pool.submit(() -> downloadFromSourceWorker(
                        filename,
                        source,
                        channel,
                        directory,
                        queue,
                        state,
                        metaPath,
                        remainingChunks,
                        stopSignal));
            }

            Thread progressThread = new Thread(() -> printProgress(stopSignal, remainingChunks));
            progressThread.start();

            pool.shutdown();
            boolean finished = pool.awaitTermination(2, TimeUnit.HOURS);
            stopSignal.set(true);
            progressThread.interrupt();
            refresher.shutdownNow();

            if (!finished || remainingChunks.get() > 0 || !state.isComplete()) {
                throw new IOException(
                        "Download did not finish successfully. Remaining chunks: " + remainingChunks.get());
            }

            finalizeDownload(partPath, finalPath, metaPath, startTime);
        }
    }

    private static void downloadFromSourceWorker(String filename,
            ClientInfo initialSource,
            FileChannel channel,
            DirectoryInterface directory,
            BlockingQueue<ChunkTask> queue,
            DownloadState state,
            Path metaPath,
            AtomicInteger remainingChunks,
            AtomicBoolean stopSignal) {

        ClientInfo currentSource = initialSource;

        while (!stopSignal.get()) {
            ChunkTask task;
            try {
                task = queue.poll(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (task == null) {
                if (remainingChunks.get() == 0) {
                    return;
                }
                currentSource = pickAnotherSource(currentSource);
                continue;
            }

            if (state.isChunkDone(task.index)) {
                continue;
            }

            boolean done = false;
            List<ClientInfo> snapshot = new ArrayList<>(currentSources);
            if (!snapshot.contains(currentSource) && !snapshot.isEmpty()) {
                currentSource = snapshot.get(0);
            }

            int attempts = 0;
            int maxAttempts = Math.max(1, snapshot.size());

            while (!done && attempts < maxAttempts && !stopSignal.get()) {
                if (currentSource == null) {
                    break;
                }
                done = tryDownloadChunk(filename, task, currentSource, channel, directory);
                if (!done) {
                    attempts++;
                    currentSource = pickNextSource(snapshot, currentSource);
                }
            }

            if (done) {
                if (state.markChunkDone(task.index)) {
                    totalDownloaded.addAndGet(task.length);
                    remainingChunks.decrementAndGet();
                    persistState(metaPath, state);
                }
            } else {
                queue.offer(task);
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private static boolean tryDownloadChunk(String filename,
            ChunkTask task,
            ClientInfo source,
            FileChannel channel,
            DirectoryInterface directory) {
        try {
            directory.incrementLoad(source);

            try (Socket socket = new Socket(source.getHost(), source.getPort());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

                out.writeUTF("GET");
                out.writeUTF(filename);
                out.writeLong(task.start);
                out.writeLong(task.length);
                out.flush();

                InputStream rawIn = socket.getInputStream();
                InputStream dataIn = USE_COMPRESSION ? new GZIPInputStream(rawIn) : rawIn;

                byte[] buffer = new byte[BUFFER_SIZE];
                long position = task.start;
                long remaining = task.length;

                while (remaining > 0) {
                    int read = dataIn.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (read < 0) {
                        throw new EOFException("Unexpected end of stream from source " + source);
                    }

                    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, read);
                    while (byteBuffer.hasRemaining()) {
                        channel.write(byteBuffer, position);
                    }
                    position += read;
                    remaining -= read;
                }
            } finally {
                try {
                    directory.decrementLoad(source);
                } catch (Exception ignored) {
                }
            }

            return true;
        } catch (Exception e) {
            try {
                directory.decrementLoad(source);
            } catch (Exception ignored) {
            }
            System.out.println("Source " + source.getPort() + " failed for chunk " + task.index + ", switching...");
            return false;
        }
    }

    private static void printProgress(AtomicBoolean stopSignal, AtomicInteger remainingChunks) {
        try {
            while (!stopSignal.get()) {
                long done = totalDownloaded.get();
                int percent = (int) Math.min(100, (done * 100) / Math.max(fileSize, 1));
                System.out.printf(
                        "\r[Progress] %3d%% | %10d / %10d bytes | Sources: %2d | Remaining chunks: %4d ",
                        percent,
                        done,
                        fileSize,
                        currentSources.size(),
                        remainingChunks.get());
                Thread.sleep(500);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        System.out.println();
    }

    private static long getFileSize(ClientInfo source, String filename) throws Exception {
        try (Socket socket = new Socket(source.getHost(), source.getPort());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream())) {
            out.writeUTF("SIZE");
            out.writeUTF(filename);
            out.flush();
            return in.readLong();
        }
    }

    private static void refreshSources(DirectoryInterface directory, String filename, int myPort) {
        try {
            List<ClientInfo> newList = directory.getSourcesSortedByLoad(filename);
            newList.removeIf(c -> c.getPort() == myPort);
            currentSources.clear();
            currentSources.addAll(newList);
        } catch (Exception ignored) {
        }
    }

    private static ClientInfo pickAnotherSource(ClientInfo currentSource) {
        List<ClientInfo> snapshot = new ArrayList<>(currentSources);
        return pickNextSource(snapshot, currentSource);
    }

    private static ClientInfo pickNextSource(List<ClientInfo> snapshot, ClientInfo currentSource) {
        if (snapshot.isEmpty()) {
            return null;
        }
        int currentIndex = snapshot.indexOf(currentSource);
        if (currentIndex < 0) {
            return snapshot.get(0);
        }
        return snapshot.get((currentIndex + 1) % snapshot.size());
    }

    private static void enqueueMissingChunks(DownloadState state,
            BlockingQueue<ChunkTask> queue,
            AtomicInteger remainingChunks,
            long totalSize) {
        long chunkCount = state.chunkCount();
        for (int i = 0; i < chunkCount; i++) {
            if (!state.isChunkDone(i)) {
                long start = i * CHUNK_SIZE;
                long length = Math.min(CHUNK_SIZE, totalSize - start);
                queue.offer(new ChunkTask(i, start, length));
                remainingChunks.incrementAndGet();
            }
        }
    }

    private static DownloadState loadOrCreateState(Path metaPath, long totalSize) throws IOException {
        int chunkCount = (int) ((totalSize + CHUNK_SIZE - 1) / CHUNK_SIZE);
        DownloadState emptyState = new DownloadState(chunkCount);

        if (!Files.exists(metaPath)) {
            persistState(metaPath, emptyState);
            return emptyState;
        }

        try (BufferedReader reader = Files.newBufferedReader(metaPath)) {
            String line = reader.readLine();
            if (line == null || line.isBlank()) {
                return emptyState;
            }
            String[] parts = line.split(":", 2);
            if (parts.length != 2) {
                return emptyState;
            }
            int storedChunkCount = Integer.parseInt(parts[0]);
            if (storedChunkCount != chunkCount) {
                return emptyState;
            }
            BitSet bitSet = new BitSet(chunkCount);
            String bitmap = parts[1].trim();
            for (int i = 0; i < Math.min(bitmap.length(), chunkCount); i++) {
                if (bitmap.charAt(i) == '1') {
                    bitSet.set(i);
                }
            }
            return new DownloadState(chunkCount, bitSet);
        } catch (Exception e) {
            return emptyState;
        }
    }

    private static synchronized void persistState(Path metaPath, DownloadState state) {
        try (BufferedWriter writer = Files.newBufferedWriter(
                metaPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            writer.write(state.chunkCount() + ":" + state.toBitmapString());
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Cannot persist download metadata", e);
        }
    }

    private static void finalizeDownload(Path partPath,
            Path finalPath,
            Path metaPath,
            long startTime) throws Exception {
        String md5 = computeMD5(partPath.toString());
        Files.move(partPath, finalPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        Files.deleteIfExists(metaPath);

        long endTime = System.currentTimeMillis();
        System.out.println("\nDownload complete!");
        System.out.println("  MD5: " + md5);
        System.out.println("  Time: " + (endTime - startTime) + " ms");
        System.out.println("  Saved to: " + finalPath);
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

    private static final class ChunkTask {
        private final int index;
        private final long start;
        private final long length;

        private ChunkTask(int index, long start, long length) {
            this.index = index;
            this.start = start;
            this.length = length;
        }
    }

    private static final class DownloadState {
        private final int chunkCount;
        private final BitSet completed;

        private DownloadState(int chunkCount) {
            this(chunkCount, new BitSet(chunkCount));
        }

        private DownloadState(int chunkCount, BitSet completed) {
            this.chunkCount = chunkCount;
            this.completed = completed;
        }

        private synchronized boolean isChunkDone(int index) {
            return completed.get(index);
        }

        private synchronized boolean markChunkDone(int index) {
            if (completed.get(index)) {
                return false;
            }
            completed.set(index);
            return true;
        }

        private synchronized boolean isComplete() {
            return completed.cardinality() == chunkCount;
        }

        private synchronized long completedBytes(long totalSize) {
            long done = 0;
            for (int i = completed.nextSetBit(0); i >= 0; i = completed.nextSetBit(i + 1)) {
                long start = i * CHUNK_SIZE;
                done += Math.min(CHUNK_SIZE, totalSize - start);
            }
            return done;
        }

        private int chunkCount() {
            return chunkCount;
        }

        private synchronized String toBitmapString() {
            StringBuilder sb = new StringBuilder(chunkCount);
            for (int i = 0; i < chunkCount; i++) {
                sb.append(completed.get(i) ? '1' : '0');
            }
            return sb.toString();
        }
    }
}
