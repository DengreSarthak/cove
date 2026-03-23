@_exported import CoveCore
import CryptoKit
import Foundation

final class ICloudDriveHelper: @unchecked Sendable {
    static let shared = ICloudDriveHelper()

    private let containerIdentifier = "iCloud.com.covebitcoinwallet"
    private let dataSubdirectory = "Data"
    private let namespacesSubdirectory = csppNamespacesSubdirectory()
    private let defaultTimeout: TimeInterval = 60
    private let pollInterval: TimeInterval = 0.1

    // MARK: - Path mapping

    func containerURL() throws -> URL {
        guard let url = FileManager.default.url(
            forUbiquityContainerIdentifier: containerIdentifier
        ) else {
            throw CloudStorageError.NotAvailable("iCloud Drive is not available")
        }
        return url
    }

    func dataDirectoryURL() throws -> URL {
        let url = try containerURL().appendingPathComponent(dataSubdirectory, isDirectory: true)
        if !FileManager.default.fileExists(atPath: url.path) {
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        }
        return url
    }

    /// Root directory for all namespaces: Data/cspp-namespaces/
    func namespacesRootURL() throws -> URL {
        let url = try dataDirectoryURL()
            .appendingPathComponent(namespacesSubdirectory, isDirectory: true)
        if !FileManager.default.fileExists(atPath: url.path) {
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        }
        return url
    }

    /// Directory for a specific namespace: Data/cspp-namespaces/{namespace}/
    func namespaceDirectoryURL(namespace: String) throws -> URL {
        let url = try namespacesRootURL()
            .appendingPathComponent(namespace, isDirectory: true)
        if !FileManager.default.fileExists(atPath: url.path) {
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        }
        return url
    }

    /// Master key file URL within a namespace
    ///
    /// Filename: masterkey-{SHA256(MASTER_KEY_RECORD_ID)}.json
    func masterKeyFileURL(namespace: String) throws -> URL {
        let recordId = csppMasterKeyRecordId()
        let hash = SHA256.hash(data: Data(recordId.utf8))
        let hexHash = hash.compactMap { String(format: "%02x", $0) }.joined()
        let filename = "masterkey-\(hexHash).json"
        return try namespaceDirectoryURL(namespace: namespace)
            .appendingPathComponent(filename)
    }

    /// Wallet backup file URL within a namespace
    ///
    /// Filename: wallet-{recordId}.json — recordId is already SHA256(wallet_id)
    func walletFileURL(namespace: String, recordId: String) throws -> URL {
        let filename = "wallet-\(recordId).json"
        return try namespaceDirectoryURL(namespace: namespace)
            .appendingPathComponent(filename)
    }

    /// Legacy flat file URL (for migration/cleanup)
    func legacyFileURL(for recordId: String) throws -> URL {
        let hash = SHA256.hash(data: Data(recordId.utf8))
        let filename = hash.compactMap { String(format: "%02x", $0) }.joined() + ".json"
        return try dataDirectoryURL().appendingPathComponent(filename)
    }

    // MARK: - File coordination

    func coordinatedWrite(data: Data, to url: URL) throws {
        var coordinatorError: NSError?
        var writeError: Error?

        let coordinator = NSFileCoordinator()
        coordinator.coordinate(
            writingItemAt: url, options: .forReplacing, error: &coordinatorError
        ) { newURL in
            do {
                try data.write(to: newURL, options: .atomic)
            } catch {
                writeError = error
            }
        }

        if let error = coordinatorError ?? writeError {
            throw CloudStorageError.UploadFailed("write failed: \(error.localizedDescription)")
        }
    }

    func coordinatedDelete(at url: URL) throws {
        var coordinatorError: NSError?
        var deleteError: Error?

        let coordinator = NSFileCoordinator()
        coordinator.coordinate(
            writingItemAt: url, options: .forDeleting, error: &coordinatorError
        ) { newURL in
            do {
                try FileManager.default.removeItem(at: newURL)
            } catch {
                deleteError = error
            }
        }

        if let error = coordinatorError ?? deleteError {
            throw CloudStorageError.UploadFailed("delete failed: \(error.localizedDescription)")
        }
    }

    func coordinatedRead(from url: URL) throws -> Data {
        var coordinatorError: NSError?
        var readResult: Result<Data, Error>?

        let coordinator = NSFileCoordinator()
        coordinator.coordinate(readingItemAt: url, options: [], error: &coordinatorError) { newURL in
            do {
                readResult = try .success(Data(contentsOf: newURL))
            } catch {
                readResult = .failure(error)
            }
        }

        if let error = coordinatorError {
            throw CloudStorageError.DownloadFailed(
                "file coordination error: \(error.localizedDescription)"
            )
        }

        guard let readResult else {
            throw CloudStorageError.DownloadFailed("coordinated read produced no result")
        }

        switch readResult {
        case let .success(data): return data
        case let .failure(error):
            throw CloudStorageError.DownloadFailed(error.localizedDescription)
        }
    }

    // MARK: - Upload verification

    /// Blocks until the file at `url` is confirmed uploaded to iCloud, or times out
    func waitForUpload(url: URL) throws {
        let filename = url.lastPathComponent
        Log.info("waitForUpload: waiting for \(filename)")
        let deadline = Date().addingTimeInterval(defaultTimeout)

        while Date() < deadline {
            let values = try? url.resourceValues(forKeys: [
                .ubiquitousItemIsUploadedKey,
                .ubiquitousItemUploadingErrorKey,
            ])

            if values?.ubiquitousItemIsUploaded == true {
                Log.info("waitForUpload: \(filename) uploaded")
                return
            }

            if let error = values?.ubiquitousItemUploadingError {
                throw CloudStorageError.UploadFailed(
                    "iCloud upload failed for \(filename): \(error.localizedDescription)"
                )
            }

            Thread.sleep(forTimeInterval: pollInterval)
        }

        throw CloudStorageError.UploadFailed(
            "iCloud upload timed out for \(filename) after \(defaultTimeout)s"
        )
    }

    // MARK: - Download

    /// Ensures the file is downloaded locally, triggering a download if evicted
    func ensureDownloaded(url: URL, recordId: String) throws {
        // check if already downloaded
        if FileManager.default.fileExists(atPath: url.path) {
            let values = try? url.resourceValues(forKeys: [.ubiquitousItemDownloadingStatusKey])
            if values?.ubiquitousItemDownloadingStatus == .current {
                return
            }
        }

        // trigger download
        do {
            try FileManager.default.startDownloadingUbiquitousItem(at: url)
        } catch {
            let nsError = error as NSError
            if nsError.domain == NSCocoaErrorDomain,
               nsError.code == NSFileReadNoSuchFileError || nsError.code == 4
            {
                throw CloudStorageError.NotFound(recordId)
            }
            throw CloudStorageError.DownloadFailed(
                "failed to start download: \(error.localizedDescription)"
            )
        }

        // wait for download to complete
        let deadline = Date().addingTimeInterval(defaultTimeout)
        while Date() < deadline {
            let values = try? url.resourceValues(forKeys: [
                .ubiquitousItemDownloadingStatusKey,
                .ubiquitousItemDownloadingErrorKey,
            ])

            if values?.ubiquitousItemDownloadingStatus == .current {
                return
            }

            if let error = values?.ubiquitousItemDownloadingError {
                throw CloudStorageError.DownloadFailed(
                    "iCloud download failed: \(error.localizedDescription)"
                )
            }

            Thread.sleep(forTimeInterval: pollInterval)
        }

        throw CloudStorageError.DownloadFailed(
            "iCloud download timed out after \(defaultTimeout)s"
        )
    }

    // MARK: - Cloud presence via NSMetadataQuery

    /// Runs an NSMetadataQuery and returns all matching items
    ///
    /// Must NOT be called from the main thread
    func metadataQuery(predicate: NSPredicate) throws -> [NSMetadataItem] {
        let semaphore = DispatchSemaphore(value: 0)
        var results: [NSMetadataItem] = []
        var startFailed = false

        let query = NSMetadataQuery()
        query.searchScopes = [NSMetadataQueryUbiquitousDataScope]
        query.predicate = predicate

        class ObserverBox {
            var observer: NSObjectProtocol?
            func remove() {
                if let obs = observer {
                    NotificationCenter.default.removeObserver(obs)
                    observer = nil
                }
            }
        }
        let box = ObserverBox()

        box.observer = NotificationCenter.default.addObserver(
            forName: .NSMetadataQueryDidFinishGathering,
            object: query,
            queue: .main
        ) { _ in
            query.disableUpdates()
            results = (0 ..< query.resultCount).compactMap { query.result(at: $0) as? NSMetadataItem }
            query.stop()
            box.remove()
            semaphore.signal()
        }

        DispatchQueue.main.async {
            if !query.start() {
                startFailed = true
                box.remove()
                semaphore.signal()
            }
        }

        if semaphore.wait(timeout: .now() + defaultTimeout) == .timedOut {
            DispatchQueue.main.async {
                query.stop()
                box.remove()
            }
            throw CloudStorageError.NotAvailable("iCloud metadata query timed out")
        }

        if startFailed {
            throw CloudStorageError.NotAvailable("failed to start iCloud metadata query")
        }

        return results
    }

    /// Authoritatively checks whether a file exists in iCloud (finds evicted files too)
    ///
    /// Must NOT be called from the main thread
    func fileExistsInCloud(name: String) throws -> Bool {
        let predicate = NSPredicate(format: "%K == %@", NSMetadataItemFSNameKey, name)
        let results = try metadataQuery(predicate: predicate)
        return !results.isEmpty
    }

    /// Resolve symlinks so /var and /private/var compare correctly
    private static func resolvedPath(_ path: String) -> String {
        URL(fileURLWithPath: path).resolvingSymlinksInPath().path
    }

    /// Lists subdirectory names within a given directory path using NSMetadataQuery
    ///
    /// Finds items whose path contains the parent directory and filters for directories
    func listSubdirectories(parentPath: String) throws -> [String] {
        let resolvedParent = Self.resolvedPath(parentPath)
        let predicate = NSPredicate(format: "%K BEGINSWITH %@", NSMetadataItemPathKey, resolvedParent)
        let results = try metadataQuery(predicate: predicate)

        let prefix = resolvedParent + "/"
        var subdirs = Set<String>()

        for item in results {
            guard let path = item.value(forAttribute: NSMetadataItemPathKey) as? String else {
                continue
            }

            let resolved = Self.resolvedPath(path)
            guard resolved.hasPrefix(prefix) else { continue }
            let relative = String(resolved.dropFirst(prefix.count))

            if let firstComponent = relative.split(separator: "/").first {
                subdirs.insert(String(firstComponent))
            }
        }

        return Array(subdirs).sorted()
    }

    /// Lists filenames matching a prefix within a namespace directory using NSMetadataQuery
    ///
    /// Uses a single BEGINSWITH on path (which works across /var symlinks), then
    /// filters by prefix in Swift — compound predicates silently return 0 on some devices
    func listFiles(namespacePath: String, prefix: String) throws -> [String] {
        let predicate = NSPredicate(
            format: "%K BEGINSWITH %@",
            NSMetadataItemPathKey, namespacePath
        )
        let results = try metadataQuery(predicate: predicate)

        return results.compactMap { item in
            guard let name = item.value(forAttribute: NSMetadataItemFSNameKey) as? String else {
                return nil
            }
            guard name.hasPrefix(prefix) else { return nil }
            return name
        }.sorted()
    }

    // MARK: - Upload status for UI

    enum UploadStatus {
        case uploaded
        case uploading
        case failed(String)
        case unknown
    }

    func uploadStatus(for url: URL) -> UploadStatus {
        guard FileManager.default.fileExists(atPath: url.path) else {
            return .unknown
        }

        let values = try? url.resourceValues(forKeys: [
            .ubiquitousItemIsUploadedKey,
            .ubiquitousItemUploadingErrorKey,
        ])

        if values?.ubiquitousItemIsUploaded == true {
            return .uploaded
        }

        if let error = values?.ubiquitousItemUploadingError {
            return .failed(error.localizedDescription)
        }

        return .uploading
    }

    /// Checks sync health of all files in namespace directories
    func overallSyncHealth() -> SyncHealth {
        guard let namespacesRoot = try? namespacesRootURL() else {
            return .unavailable
        }

        guard
            let namespaceDirs = try? FileManager.default.contentsOfDirectory(
                at: namespacesRoot, includingPropertiesForKeys: nil,
                options: .skipsHiddenFiles
            )
        else {
            return .unavailable
        }

        var hasFiles = false
        var allUploaded = true
        var anyFailed = false
        var failureMessage: String?

        for nsDir in namespaceDirs where nsDir.hasDirectoryPath {
            guard
                let files = try? FileManager.default.contentsOfDirectory(
                    at: nsDir, includingPropertiesForKeys: nil
                )
            else { continue }

            for file in files where file.pathExtension == "json" {
                hasFiles = true
                let status = uploadStatus(for: file)
                switch status {
                case .uploaded: continue
                case .uploading: allUploaded = false
                case let .failed(msg):
                    anyFailed = true
                    allUploaded = false
                    failureMessage = msg
                case .unknown:
                    allUploaded = false
                }
            }
        }

        if !hasFiles { return .noFiles }
        if anyFailed { return .failed(failureMessage ?? "upload error") }
        if allUploaded { return .allUploaded }
        return .uploading
    }

    enum SyncHealth {
        case allUploaded
        case uploading
        case failed(String)
        case noFiles
        case unavailable
    }
}
