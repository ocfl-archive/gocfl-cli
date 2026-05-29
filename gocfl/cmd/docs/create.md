# OCFL Create Operation Documentation

This document explains the `doCreate` function in `gocfl-cli`, which is responsible for initializing a new OCFL storage root and adding an initial object with content.

## doCreate() Function Explanation

The `doCreate()` function is the core of the `create` command. It combines the functionality of `init` (initializing a storage root) and `add` (adding an object version).

### Key Steps:
1.  **Configuration**: Updates internal configurations from command-line flags (digest, message, user info, etc.). Also handles AES encryption settings if provided.
2.  **Target Validation**: Checks if the target path exists. If it exists, it must be an empty directory (unless it's a ZIP file being created).
3.  **Filesystem Setup**:
    *   Prepares the source filesystem (where content comes from).
    *   Prepares the destination filesystem.
    *   If the target ends in `.zip`:
        *   If AES encryption is enabled, it sets up a KMS client (static or KeePass2) and initializes an encrypted ZIP filesystem (`zipfswenc`).
        *   Otherwise, it initializes a standard ZIP filesystem wrapper (`zipfsw`).
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
        alt is Encrypted
            DC->>DC: Setup KMS Client (Static or KeePass2)
            DC->>VFS: NewFSFileChecksumsEncrypted(ocflPath, ...)
        else is Not Encrypted
            DC->>VFS: Create(zipFile)
            VFS-->>DC: zipWriter
            DC->>DC: zipfsw.NewFS(zipWriter, closeWriter=true)
        end
    else is Directory
        DC->>VFS: SubCreate(ocflPath)
    end
    DC-->>DC: destFS

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
    
    Note over DC, VFS: Defer blocks execute in reverse order:
    DC->>OBJ: Close() (Flush inventory)
    DC->>SR: Close()
    DC->>VFS: Close(destFS)
    
    DC->>DC: showStatus()
```

---

## Resource Management: Open, Close, and Deferred

The function manages several critical resources using `defer`. Since `doCreate` returns an `error` instead of calling `logger.Fatal()`, `defer` blocks are guaranteed to run when the function returns, ensuring resources are closed exactly once.

```mermaid
graph TD
    Start([Start doCreate]) --> IsZip{is ZIP?}
    
    IsZip -- Yes --> IsEnc{Encrypted?}
    
    IsEnc -- Yes --> SetupKMS[Setup KMS Client]
    SetupKMS -- Error --> ReturnErrKMS[return error]
    SetupKMS --> InitEncFS[zipfswenc.NewFS...Encrypted]
    InitEncFS -- Error --> ReturnErrFS3[return error]
    InitEncFS --> DefFS

    IsEnc -- No --> CreateZip[writefs.Create zipWriter]
    CreateZip -- Error --> ReturnErrZip[return error]
    CreateZip --> InitZipFS[zipfsw.NewFS]
    InitZipFS -- Error --> CloseZipW[Close zipWriter] --> ReturnErrFS1[return error]
    InitZipFS --> DefFS
    
    IsZip -- No --> SubCreate[writefs.SubCreate destFS]
    SubCreate -- Error --> ReturnErrFS2[return error]
    SubCreate --> DefFS

    DefFS[<b>defer</b> writefs.Close destFS] --> OpenSR[Create StorageRoot]
    OpenSR -- Error --> ReturnErr2[return error]
    
    OpenSR --> DefSR[<b>defer</b> storageRoot.Close]
    DefSR --> OpenSub[Open Object Sub-FS]
    OpenSub -- Error --> ReturnErr3[return error]
    
    OpenSub --> DefSub[<b>defer</b> closer.Close object sub-FS]
    
    DefSub --> InitObj[Init OCFL Object]
    InitObj -- Error --> ReturnErr4[return error]
    
    InitObj --> DefObj[<b>defer</b> o.Close object]
    
    DefObj --> StartUpd[StartUpdate version]
    StartUpd -- Error --> ReturnErr5[return error]
    
    StartUpd --> AddFold[AddFolder content]
    AddFold -- Error --> CloseVW1[versionWriter.Close] --> ReturnErr6[return error]
    
    AddFold --> CloseVW2[versionWriter.Close Success]
    CloseVW2 -- Error --> ReturnErr7[return error]
    
    CloseVW2 --> ShowStatus[showStatus]
    ShowStatus --> End([End doCreate])

    style DefFS fill:#f9f,stroke:#333
    style DefSR fill:#f9f,stroke:#333
    style DefSub fill:#f9f,stroke:#333
    style DefObj fill:#f9f,stroke:#333
```

*Note: `defer` functions ensure that resources are cleaned up in the correct order (Last-In-First-Out) when the function returns, whether it's a success or an error.*
