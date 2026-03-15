package daemon;

import java.io.*;
import java.net.Socket;
import java.util.zip.GZIPOutputStream;

public class FileServer implements Runnable {

    private final Socket socket;
    private static final boolean USE_COMPRESSION = true;   

    public FileServer(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try (DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream cmdOut = new DataOutputStream(socket.getOutputStream())) {

            String cmd = in.readUTF();

            if (cmd.equals("SIZE")) {
                String filename = in.readUTF();
                File file = new File(Daemon.SHARED_DIR + "/" + filename);

                long size = file.exists() ? file.length() : 0;
                cmdOut.writeLong(size);
                System.out.println(" SIZE request: " + filename + " → " + size + " bytes");

                socket.close();
                return;
            }

            if (cmd.equals("GET")) {
                String filename = in.readUTF();
                long start = in.readLong();
                long length = in.readLong();

                File file = new File(Daemon.SHARED_DIR + "/" + filename);
                if (!file.exists()) {
                    System.out.println(" File not found: " + filename);
                    socket.close();
                    return;
                }

                try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                    raf.seek(start);

                    OutputStream rawOut = socket.getOutputStream();
                    OutputStream dataOut = USE_COMPRESSION
                            ? new GZIPOutputStream(rawOut)
                            : rawOut;

                    System.out.println(" Sending chunk: " + filename +
                            " | offset=" + start + " | length=" + length +
                            (USE_COMPRESSION ? " (GZIP)" : ""));

                    byte[] buffer = new byte[65536];
                    long remaining = length;

                    while (remaining > 0) {
                        int toRead = (int) Math.min(buffer.length, remaining);
                        int read = raf.read(buffer, 0, toRead);
                        if (read == -1) break;

                        dataOut.write(buffer, 0, read);
                        remaining -= read;
                    }

                    if (USE_COMPRESSION) {
                        ((GZIPOutputStream) dataOut).finish();  
                    }
                    dataOut.flush();
                }

                System.out.println(" Chunk sent successfully");
            }

        } catch (Exception e) {
            System.out.println(" FileServer error: " + e.getMessage());
        } finally {
            try {
                if (!socket.isClosed()) socket.close();
            } catch (Exception ignored) {}
        }
    }
}