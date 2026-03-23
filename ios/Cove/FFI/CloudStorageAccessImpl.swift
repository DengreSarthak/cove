@_exported import CoveCore
import Foundation

final class CloudStorageAccessImpl: CloudStorageAccess, @unchecked Sendable {
    private let helper = ICloudDriveHelper.shared

    // MARK: - Upload

    func uploadMasterKeyBackup(namespace: String, data: Data) throws {
        let url = try helper.masterKeyFileURL(namespace: namespace)
        try helper.coordinatedWrite(data: data, to: url)
        try helper.waitForUpload(url: url)
    }

    func uploadWalletBackup(namespace: String, recordId: String, data: Data) throws {
        let url = try helper.walletFileURL(namespace: namespace, recordId: recordId)
        try helper.coordinatedWrite(data: data, to: url)
        try helper.waitForUpload(url: url)
    }

    // MARK: - Download

    func downloadMasterKeyBackup(namespace: String) throws -> Data {
        let url = try helper.masterKeyFileURL(namespace: namespace)
        try helper.ensureDownloaded(url: url, recordId: "masterkey-\(namespace)")
        return try helper.coordinatedRead(from: url)
    }

    func downloadWalletBackup(namespace: String, recordId: String) throws -> Data {
        let url = try helper.walletFileURL(namespace: namespace, recordId: recordId)
        try helper.ensureDownloaded(url: url, recordId: recordId)
        return try helper.coordinatedRead(from: url)
    }

    func deleteWalletBackup(namespace: String, recordId: String) throws {
        let url = try helper.walletFileURL(namespace: namespace, recordId: recordId)
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw CloudStorageError.NotFound(recordId)
        }
        try helper.coordinatedDelete(at: url)
    }

    // MARK: - Discovery

    func listNamespaces() throws -> [String] {
        let namespacesRoot = try helper.namespacesRootURL()
        return try helper.listSubdirectories(parentPath: namespacesRoot.path)
    }

    func listWalletFiles(namespace: String) throws -> [String] {
        let nsDir = try helper.namespaceDirectoryURL(namespace: namespace)
        return try helper.listFiles(namespacePath: nsDir.path, prefix: "wallet-")
    }
}
