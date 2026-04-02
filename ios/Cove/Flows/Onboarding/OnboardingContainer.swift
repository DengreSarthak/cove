import SwiftUI

@_exported import CoveCore

extension WeakReconciler: OnboardingManagerReconciler where Reconciler == OnboardingManager {}

@Observable
final class OnboardingManager: AnyReconciler, OnboardingManagerReconciler, @unchecked Sendable {
    let rust: RustOnboardingManager
    let app: AppManager
    var step: OnboardingStep
    var isComplete = false
    var cloudCheckWarning: String?
    var restoreError: String?

    typealias Message = OnboardingReconcileMessage

    init(app: AppManager) {
        self.app = app
        self.step = app.isTermsAccepted ? .cloudCheck : .terms
        self.rust = RustOnboardingManager()
        self.rust.listenForUpdates(reconciler: WeakReconciler(self))
    }

    func dispatch(_ action: OnboardingAction) {
        rust.dispatch(action: action)
    }

    func reconcile(message: OnboardingReconcileMessage) {
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            switch message {
            case let .stepChanged(newStep):
                self.step = newStep
            case .complete:
                self.isComplete = true
            case let .restoreError(error):
                self.cloudCheckWarning = nil
                self.restoreError = error
            }
        }
    }

    func reconcileMany(messages: [OnboardingReconcileMessage]) {
        messages.forEach { reconcile(message: $0) }
    }
}

struct OnboardingContainer: View {
    @State private var manager: OnboardingManager
    let onComplete: () -> Void

    init(manager: OnboardingManager, onComplete: @escaping () -> Void) {
        _manager = State(initialValue: manager)
        self.onComplete = onComplete
    }

    var body: some View {
        stepView(for: manager.step)
            .onChange(of: manager.isComplete) { _, complete in
                if complete {
                    manager.app.reloadWallets()

                    // auto-select first restored wallet so user lands in it
                    if let wallet = manager.app.wallets.first { manager.app.selectWallet(wallet.id) }

                    onComplete()
                }
            }
    }

    @ViewBuilder
    func stepView(for step: OnboardingStep) -> some View {
        switch step {
        case .terms:
            TermsAndConditionsView {
                manager.app.agreeToTerms()
                manager.dispatch(.acceptTerms)
            }

        case .cloudCheck:
            CloudCheckView { result in
                switch result {
                case .backupFound:
                    manager.cloudCheckWarning = nil
                    manager.dispatch(.cloudCheckComplete(hasBackup: true))

                case .noBackup:
                    manager.cloudCheckWarning = nil
                    manager.dispatch(.cloudCheckComplete(hasBackup: false))

                case let .inconclusive(message):
                    manager.cloudCheckWarning = message
                    manager.dispatch(.cloudCheckComplete(hasBackup: true))
                }
            }

        case .restoreOffer:
            CloudRestoreOfferView(
                onRestore: {
                    manager.cloudCheckWarning = nil
                    manager.restoreError = nil
                    manager.dispatch(.startRestore)
                },
                onSkip: {
                    manager.cloudCheckWarning = nil
                    manager.restoreError = nil
                    manager.dispatch(.skipRestore)
                },
                warningMessage: manager.restoreError == nil ? manager.cloudCheckWarning : nil,
                errorMessage: manager.restoreError
            )

        case .restoring:
            DeviceRestoreView(
                onComplete: { manager.dispatch(.restoreComplete) },
                onError: { error in manager.dispatch(.restoreFailed(error: error)) }
            )
        }
    }
}

// MARK: - Cloud Check View

private enum CloudBackupCheckResult {
    case backupFound
    case noBackup
    case inconclusive(String)
}

private struct CloudCheckView: View {
    private static let retryDelays: [Duration] = [.seconds(1), .seconds(2), .seconds(2), .seconds(3), .seconds(5), .seconds(10)]
    private static let inconclusiveMessage =
        "We couldn't confirm iCloud backup availability because connectivity or iCloud may be unavailable. You can try restore now or check Cloud Backup later in Settings."
    private static var maxAttempts: Int {
        retryDelays.count + 1
    }

    let onCloudCheckComplete: (CloudBackupCheckResult) -> Void

    var body: some View {
        CloudCheckContent()
            .task {
                let result = await Self.checkForCloudBackup { _ in }
                guard !Task.isCancelled else { return }
                onCloudCheckComplete(result)
            }
    }

    private static func checkForCloudBackup(
        onAttempt: @Sendable (Int) async -> Void
    ) async -> CloudBackupCheckResult {
        guard FileManager.default.ubiquityIdentityToken != nil else {
            Log.info("[ONBOARDING] iCloud not available")
            return .inconclusive(inconclusiveMessage)
        }

        let cloud = CloudStorage(cloudStorage: CloudStorageAccessImpl())
        for attempt in 1 ... maxAttempts {
            if Task.isCancelled { return .noBackup }
            await onAttempt(attempt)
            Log.info("[ONBOARDING] calling hasAnyCloudBackup attempt=\(attempt)/\(maxAttempts)")
            do {
                let hasBackup = try cloud.hasAnyCloudBackup()
                Log.info("[ONBOARDING] hasAnyCloudBackup returned: \(hasBackup) attempt=\(attempt)/\(maxAttempts)")
                return hasBackup ? .backupFound : .noBackup
            } catch {
                Log.error("[ONBOARDING] hasAnyCloudBackup failed attempt=\(attempt)/\(maxAttempts), error: \(error)")
            }
            guard attempt < maxAttempts else { break }
            do {
                try await Task.sleep(for: retryDelays[attempt - 1])
            } catch is CancellationError {
                return .noBackup
            } catch {
                Log.error("[ONBOARDING] cloud backup retry sleep failed: \(error)")
                return .inconclusive(inconclusiveMessage)
            }
        }

        return .inconclusive(inconclusiveMessage)
    }
}

private struct CloudCheckContent: View {
    var body: some View {
        VStack(spacing: 0) {
            Spacer(minLength: 0)

            OnboardingStatusHero(
                systemImage: "icloud",
                pulse: true,
                iconSize: 22
            )

            Spacer()
                .frame(height: 44)

            VStack(spacing: 10) {
                Text("Looking for iCloud backup...")
                    .font(OnboardingRecoveryTypography.compactTitle)
                    .foregroundStyle(.white)
                    .multilineTextAlignment(.center)

                Text("This only takes a moment")
                    .font(OnboardingRecoveryTypography.body)
                    .foregroundStyle(.coveLightGray.opacity(0.7))
                    .multilineTextAlignment(.center)
            }
            .padding(.horizontal, 24)

            Spacer(minLength: 0)
        }
        .padding(.horizontal, 28)
        .padding(.top, 18)
        .padding(.bottom, 28)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .onboardingRecoveryBackground()
    }
}

#Preview("Cloud Check") {
    CloudCheckContent()
}
