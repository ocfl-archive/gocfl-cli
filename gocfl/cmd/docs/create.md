# OCFL Create Operation Documentation

This document explains the `doCreate` function in `gocfl-cli`, which is responsible for initializing a new OCFL storage root and adding an initial object with content.

## doCreate() Function Explanation

The `doCreate()` function is the core of the `create` command. It combines the functionality of `init` (initializing a storage root) and `add` (adding an object version).

### Key Steps:
1.  **Configuration**: Updates internal configurations from command-line flags (digest, message, user info, etc.).
2.  **Target Validation**: Checks if the target path exists. If it exists, it must be an empty directory (unless it's a ZIP file being created).
3.  **Filesystem Setup**:
    *   Prepares the source filesystem (where content comes from).
    *   Prepares the destination filesystem. If the target ends in `.zip`, it initializes a ZIP filesystem wrapper (`zipfsw`).
4.  **Extension Initialization**: Sets up extensions like migration, thumbnails, indexer, and metafiles.
5.  **Storage Root Creation**: Initializes the OCFL Storage Root using `CreateStorageRoot`.
6.  **Object Initialization**:
    *   Maps the Object ID to a folder path.
    *   Initializes the OCFL Object in the destination.
7.  **Version Creation**:
    *   Starts a new version update.
    *   Adds the main content folder to the version.
    *   Adds any additional "area" folders if specified.
8.  **Finalization**: Closes the version writer, the object, and the storage root to flush all data and inventories to the filesystem.

---

## doCreate Flow Sequence Diagram

```mermaid
sequenceDiagram
    participant CLI as Cobra Command
    participant DC as doCreate()
    participant VFS as Virtual Filesystem
    participant SR as StorageRoot
    participant OBJ as OCFL Object
    participant VW as VersionWriter

    CLI->>DC: Execute(args, flags)
    DC->>DC: doInitConf() & doAddConf()
    DC->>VFS: Stat(ocflPath)
    DC->>VFS: Sub(srcPath)
    
    alt is ZIP
        DC->>VFS: Create(zipFile)
        DC->>DC: Initialize zipfsw
    else is Directory
        DC->>VFS: SubCreate(ocflPath)
    end

    DC->>SR: CreateStorageRoot(destFS, ...)
    SR-->>DC: storageRoot
    
    DC->>SR: IdToFolder(objectID)
    SR-->>DC: objPath
    
    DC->>OBJ: InitObject(objectFS, objectID, ...)
    OBJ-->>DC: object
    
    DC->>OBJ: StartUpdate(msg, user, ...)
    OBJ-->>DC: versionWriter
    
    DC->>VW: AddFolder(sourceFS, ...)
    DC->>VW: Close() (Finalize version)
    
    DC->>OBJ: Close() (Flush inventory)
    DC->>SR: Close()
    DC->>VFS: Close(destFS)
    
    DC->>DC: showStatus()
```

---

## Resource Management: Open, Close, and Deferred

The function manages several critical resources. Because `logger.Fatal()` is used for errors (which calls `os.Exit(1)` and skips `defer`), many resources are closed manually in error blocks.

```mermaid
graph TD
    Start([Start doCreate]) --> OpenDest[Open/Create Destination FS]
    OpenDest -- Error --> Fatal1[logger.Fatal]
    
    OpenDest --> OpenSR[Create StorageRoot]
    OpenSR -- Error --> CloseFS1[writefs.Close destFS] --> Fatal2[logger.Fatal]
    
    OpenSR --> OpenSub[Open Object Sub-FS]
    OpenSub -- Error --> CloseSR1[storageRoot.Close] --> CloseFS2[writefs.Close destFS] --> Fatal3[logger.Fatal]
    
    OpenSub --> DefSub[<b>defer</b> closer.Close object sub-FS]
    
    DefSub --> InitObj[Init OCFL Object]
    InitObj -- Error --> CloseSR2[storageRoot.Close] --> CloseFS3[writefs.Close destFS] --> Fatal4[logger.Fatal]
    
    InitObj --> DefObj[<b>defer</b> o.Close object]
    
    DefObj --> StartUpd[StartUpdate version]
    StartUpd -- Error --> CloseObj1[o.Close] --> CloseSR3[storageRoot.Close] --> CloseFS4[writefs.Close destFS] --> Fatal5[logger.Fatal]
    
    StartUpd --> AddFold[AddFolder content]
    AddFold -- Error --> CloseVW1[versionWriter.Close] --> CloseObj2[o.Close] --> CloseSR4[storageRoot.Close] --> CloseFS5[writefs.Close destFS] --> Fatal6[logger.Fatal]
    
    AddFold --> CloseVW2[versionWriter.Close Success]
    CloseVW2 -- Error --> CloseObj3[o.Close] --> CloseSR5[storageRoot.Close] --> CloseFS6[writefs.Close destFS] --> Fatal7[logger.Fatal]
    
    CloseVW2 --> FinalClose[Final Manual Cleanup]
    
    subgraph Final Manual Cleanup
        MCloseObj[o.Close] --> MCloseSR[storageRoot.Close] --> MCloseFS[writefs.Close destFS]
    end
    
    FinalClose --> End([End doCreate])

    style DefSub fill:#f9f,stroke:#333
    style DefObj fill:#f9f,stroke:#333
```

*Note: `defer` functions are registered but only execute if the function returns normally. In case of `logger.Fatal()`, the manual cleanup in each error block is executed instead.*
