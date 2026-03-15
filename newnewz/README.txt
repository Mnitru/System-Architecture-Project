# SongSong Project - Parallel File Download Infrastructure

**Course:** System Architecture - USTH 2026  
**Group:** 10
**Submission Date:** March 18, 2026

## Project Overview
This project implements a distributed parallel download system as described in the assignment.  
**We have completed 100% of the mandatory requirements + ALL enhancements + several bonus features.**

### Features Completed

**Mandatory:**
- Full basic prototype (Directory via RMI + Daemon via TCP + Parallel Download)
- Parallelism validation with performance curve

**All Enhancements:**
- Failure & disconnection handling (automatic retry + resume)
- Dynamic adaptation (detect new/disconnected clients)
- Source selection optimized by client load
- Data compression (GZIP)

**Extra & Bonus Features:**
- MD5 checksum verification
- Real-time progress bar
- **Auto-resume partial download** (`.part` files)
- Heartbeat + automatic dead client cleanup
- Clean logging and robust error handling

## Project Structure
src/
├── directory/
│   ├── Directory.java
│   └── DirectoryInterface.java
├── daemon/
│   ├── Daemon.java
│   ├── DownloadManager.java
│   └── FileServer.java
├── model/
│   └── ClientInfo.java
shared_5000/          ← Shared files folder (auto-created)
shared_5001/          ← ...
downloads/            ← Downloaded files (auto-created)

## Role of Each File

| File                        | Location       | Purpose |
|-----------------------------|----------------|---------|
| **ClientInfo.java**         | model/         | Represents a client (host, port, currentLoad). Supports load balancing. |
| **DirectoryInterface.java** | directory/     | RMI remote interface (register, unregister, getSourcesSortedByLoad, increment/decrement load, heartbeat). |
| **Directory.java**          | directory/     | Main RMI server. Manages file index, load balancing, and automatic cleanup of dead clients. |
| **Daemon.java**             | daemon/        | Runs on each client. Registers files, sends heartbeat, starts TCP server, and provides interactive download interface. |
| **FileServer.java**         | daemon/        | Handles TCP requests ("SIZE" and "GET"). Supports GZIP compression. |
| **DownloadManager.java**    | daemon/        | Core of the system. Handles chunk division, parallel download, resume from `.part`, retry on failure, dynamic source refresh, progress bar, and MD5 check. |

## How to Run (Step-by-step Manual)
**WINDOW
### 1. Compile
```bash
cd src
javac -d . directory/*.java model/*.java daemon/*.java
###2. Start Directory Server (only one instance)
java directory.Directory
###3. Start Daemons (open multiple terminals)
java daemon.Daemon 5000
java daemon.Daemon 5001
java daemon.Daemon 5002
###4. Download a file
java daemon.Daemon 5003
type : abc.zip
###Alternative (direct download):
java daemon.DownloadManager bigfile.zip

**LINUX
# 1. Compile
cd src
javac -d . directory/*.java model/*.java daemon/*.java

# 2. Open multiple terminals (recommended way)
gnome-terminal --tab -- bash -c "java directory.Directory; exec bash" &
gnome-terminal --tab -- bash -c "java daemon.Daemon 5000; exec bash" &
gnome-terminal --tab -- bash -c "java daemon.Daemon 5001; exec bash" &
gnome-terminal --tab -- bash -c "java daemon.Daemon 5002; exec bash" &

# 3. Start download in current terminal
java daemon.Daemon 5003
tmux new-session -d -s songsong "java directory.Directory"
tmux split-window -h "java daemon.Daemon 5000"
tmux split-window -v "java daemon.Daemon 5001"
tmux split-window -v "java daemon.Daemon 5002"
tmux attach -t songsong
