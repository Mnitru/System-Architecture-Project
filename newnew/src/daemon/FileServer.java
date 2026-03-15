package daemon;

import java.io.*;
import java.net.Socket;

public class FileServer implements Runnable {

    Socket socket;

    public FileServer(Socket socket) {
        this.socket = socket;
    }

    public void run() {

        try {

            DataInputStream in =
                    new DataInputStream(socket.getInputStream());

            DataOutputStream out =
                    new DataOutputStream(socket.getOutputStream());

            String cmd = in.readUTF();

            if (cmd.equals("SIZE")) {

                String filename = in.readUTF();

                File file =
                        new File(Daemon.SHARED_DIR + "/" + filename);

                out.writeLong(file.length());

                socket.close();
                return;
            }

            if (cmd.equals("GET")) {

                String filename = in.readUTF();
                long start = in.readLong();
                long length = in.readLong();

                RandomAccessFile raf =
                        new RandomAccessFile(
                                Daemon.SHARED_DIR + "/" + filename,
                                "r"
                        );

                raf.seek(start);

                byte[] buffer = new byte[65536];

                long remaining = length;

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

                raf.close();
            }

            socket.close();

        } catch (Exception ignored) {}
    }
}