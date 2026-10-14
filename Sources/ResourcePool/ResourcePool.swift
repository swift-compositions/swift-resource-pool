public enum PoolError: Swift::Error, Sendable, Equatable {
    case timeout
    case closed
    case creationFailed(String)
    case resetFailed(String)
    case drainTimeout
    case cancelled
}

public protocol PoolableResource: Sendable, AnyObject {
    associatedtype Config: Sendable
    associatedtype Failure: Swift::Error

    static func create(config: Config) async throws(Failure) -> Self

    func validate() async -> Bool

    func reset() async throws(Failure)
}

private struct ResourceFactory<Resource: PoolableResource>: Sendable {
    let config: Resource.Config

    func create() async throws(Resource.Failure) -> Resource {
        try await Resource.create(config: config)
    }
}

private struct Waiter<Resource: PoolableResource>: Sendable {
    let id: UInt64
    let continuation: CheckedContinuation<Resource, any Swift::Error>
    let deadline: ContinuousClock.Instant

    var isExpired: Bool {
        ContinuousClock.now >= deadline
    }
}

private struct ResourceMetadata: Sendable {
    var usageCount: Int

    init() {
        self.usageCount = 0
    }

    mutating func incrementUsage() {
        usageCount += 1
    }
}

public actor ResourcePool<Resource: PoolableResource> {

    private var available: [Resource] = []

    private var leased: Set<ObjectIdentifier> = []

    private var waitQueue: [Waiter<Resource>] = []

    private let capacity: Int

    private let factory: ResourceFactory<Resource>

    private var totalCreated: Int = 0

    private var isClosed = false

    private var isDraining = false

    private var _metrics = Metrics()

    private var cleanupTask: Task<Void, Never>?

    private var resourceMetadata: [ObjectIdentifier: ResourceMetadata] = [:]

    private let maxUsesBeforeCycling: Int?

    private var nextWaiterId: UInt64 = 0

    public init(
        capacity: Int,
        resourceConfig: Resource.Config,
        warmup: Bool = true,
        maxUsesBeforeCycling: Int? = nil
    ) async throws(PoolError) {
        precondition(capacity > 0, "Capacity must be positive")
        precondition(
            capacity <= Int.max / 2,
            "Capacity must be reasonable (max \(Int.max / 2)) to prevent overflow"
        )

        self.capacity = capacity
        self.factory = ResourceFactory(config: resourceConfig)
        self.maxUsesBeforeCycling = maxUsesBeforeCycling

        startCleanupTask()

        if warmup {
            do {
                let firstResource = try await factory.create()
                totalCreated += 1
                let id = ObjectIdentifier(firstResource)
                resourceMetadata[id] = ResourceMetadata()
                available.append(firstResource)

                if capacity > 1 {
                    startBackgroundWarmup()
                }
            } catch {
                _metrics.recordCreationFailure()
                startBackgroundWarmup()
            }
        }

        verifyAccounting()
    }

    public func withResource<T: Sendable>(
        timeout: Duration = .seconds(30),
        _ operation: (Resource) async throws -> T
    ) async throws -> T {
        precondition(timeout > .zero, "Timeout must be positive")

        let acquisitionStart = ContinuousClock.now

        let resource = try await acquireResource(timeout: timeout)

        let acquisitionDuration = ContinuousClock.now - acquisitionStart
        _metrics.recordAcquisition(waitTime: acquisitionDuration)

        do {
            let result = try await operation(resource)
            await release(resource)
            return result
        } catch {
            await release(resource)
            throw error
        }
    }

    nonisolated public var statistics: Statistics {
        get async {
            await Statistics(
                available: available.count,
                leased: leased.count,
                capacity: capacity,
                waitQueueDepth: waitQueue.count,
                totalCreated: totalCreated
            )
        }
    }

    nonisolated public var metrics: Metrics {
        get async {
            var metricsSnapshot = await _metrics
            metricsSnapshot.currentStatistics = await statistics
            return metricsSnapshot
        }
    }

    public func drain(timeout: Duration = .seconds(30)) async throws(PoolError) {
        isDraining = true

        resumeAllWaitersWithError(PoolError.closed)

        let deadline = ContinuousClock.now + timeout
        var backoff = Duration.milliseconds(10)
        let maxBackoff = Duration.milliseconds(100)

        while !leased.isEmpty && ContinuousClock.now < deadline {
            do {
                try await Task.sleep(for: backoff)
            } catch {
                throw .cancelled
            }
            backoff = min(backoff * 2, maxBackoff)
        }

        if !leased.isEmpty {
            throw PoolError.drainTimeout
        }

    }

    public func close() async {
        isClosed = true
        cleanupTask?.cancel()
        cleanupTask = nil

        resumeAllWaitersWithError(PoolError.closed)

        available.removeAll()
        leased.removeAll()
        totalCreated = 0

        verifyAccounting()
    }

    private func acquireResource(timeout: Duration) async throws(PoolError) -> Resource {
        if isClosed || isDraining {
            throw PoolError.closed
        }

        if let resource = available.popLast() {
            let id = ObjectIdentifier(resource)
            leased.insert(id)
            recordResourceAcquisition(id: id)
            return resource
        }

        if totalCreated < capacity {
            totalCreated += 1
            do {
                let resource = try await factory.create()
                let id = ObjectIdentifier(resource)
                leased.insert(id)
                resourceMetadata[id] = ResourceMetadata()
                recordResourceAcquisition(id: id)
                return resource
            } catch {
                totalCreated -= 1
                _metrics.recordCreationFailure()
                throw PoolError.creationFailed("Failed to create resource: \(error)")
            }
        }

        nextWaiterId += 1
        let waiterId = nextWaiterId

        do {
            return try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    let waiter = Waiter(
                        id: waiterId,
                        continuation: continuation,
                        deadline: ContinuousClock.now + timeout
                    )

                    waitQueue.append(waiter)
                    _metrics.recordWaiterQueued()

                    Task {
                        do {
                            try await Task.sleep(for: timeout)
                        } catch {
                        }
                        handleTimeout(waiterId: waiterId)
                    }
                }
            } onCancel: {
                Task {
                    await handleCancellation(waiterId: waiterId)
                }
            }
        } catch let error as PoolError {
            throw error
        } catch {
            throw .cancelled
        }
    }

    private func handleCancellation(waiterId: UInt64) {
        guard let index = waitQueue.firstIndex(where: { $0.id == waiterId }) else {
            return
        }

        let waiter = waitQueue.remove(at: index)
        waiter.continuation.resume(throwing: CancellationError())
    }

    private func handleTimeout(waiterId: UInt64) {
        guard let index = waitQueue.firstIndex(where: { $0.id == waiterId }) else {
            return
        }

        let waiter = waitQueue.remove(at: index)
        _metrics.recordTimeout()
        waiter.continuation.resume(throwing: PoolError.timeout)
    }

    private func release(_ resource: Resource) async {
        let id = ObjectIdentifier(resource)

        guard leased.remove(id) != nil else {
            return
        }

        guard !isClosed else {
            totalCreated -= 1
            return
        }

        if let maxUses = maxUsesBeforeCycling,
            let metadata = resourceMetadata[id],
            metadata.usageCount >= maxUses
        {
            totalCreated -= 1
            resourceMetadata.removeValue(forKey: id)
            _metrics.recordResourceCycled()
            await tryServeNextWaiter()
            return
        }

        let isValid = await resource.validate()
        guard isValid else {
            totalCreated -= 1
            resourceMetadata.removeValue(forKey: id)
            _metrics.recordValidationFailure()
            await tryServeNextWaiter()
            return
        }

        do {
            try await resource.reset()

            guard !isClosed else {
                totalCreated -= 1
                return
            }

            _metrics.recordSuccessfulReturn()

            handOffOrReturnResource(resource)

        } catch is CancellationError {
            let isValid = await resource.validate()
            guard isValid else {
                totalCreated -= 1
                resourceMetadata.removeValue(forKey: id)
                _metrics.recordResetFailure()
                await tryServeNextWaiter()
                return
            }

            guard !isClosed else {
                totalCreated -= 1
                return
            }

            _metrics.recordSuccessfulReturn()
            handOffOrReturnResource(resource)

        } catch {
            totalCreated -= 1
            resourceMetadata.removeValue(forKey: id)
            _metrics.recordResetFailure()
            await tryServeNextWaiter()
        }
    }

    private func handOffOrReturnResource(_ resource: Resource) {
        let maxRemovalsPerCall = 10
        var removedCount = 0
        var expiredWaiters: [Waiter<Resource>] = []

        waitQueue.removeAll { waiter in
            guard removedCount < maxRemovalsPerCall else { return false }
            if waiter.isExpired {
                expiredWaiters.append(waiter)
                removedCount += 1
                return true
            }
            return false
        }

        for waiter in expiredWaiters {
            _metrics.recordTimeout()
            waiter.continuation.resume(throwing: PoolError.timeout)
        }

        if let waiter = waitQueue.first {
            waitQueue.removeFirst()
            let id = ObjectIdentifier(resource)
            leased.insert(id)
            _metrics.recordDirectHandoff()
            recordResourceAcquisition(id: id)

            waiter.continuation.resume(returning: resource)
        } else {
            available.append(resource)
        }
    }

    private func tryServeNextWaiter() async {
        guard !waitQueue.isEmpty, totalCreated < capacity else {
            return
        }

        totalCreated += 1

        do {
            let resource = try await factory.create()
            let id = ObjectIdentifier(resource)

            resourceMetadata[id] = ResourceMetadata()

            if let waiter = waitQueue.first {
                waitQueue.removeFirst()
                leased.insert(id)
                recordResourceAcquisition(id: id)
                _metrics.recordDirectHandoff()
                waiter.continuation.resume(returning: resource)
            } else {
                available.append(resource)
            }
        } catch {
            totalCreated -= 1
            _metrics.recordCreationFailure()
        }
    }

    private func resumeAllWaitersWithError(_ error: any Swift::Error) {
        for waiter in waitQueue {
            waiter.continuation.resume(throwing: error)
        }
        waitQueue.removeAll()
    }

    private func startCleanupTask() {
        cleanupTask = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    try await Task.sleep(for: .seconds(1))
                } catch {
                }
                await self?.cleanExpiredWaiters()
            }
        }
    }

    private func startBackgroundWarmup() {
        Task { [weak self] in
            guard let self else { return }

            while await shouldCreateWarmupResource() {
                do {
                    let resource = try await factory.create()

                    await addWarmupResource(resource, incrementCount: true)
                } catch {
                    await recordCreationFailure()
                }
            }
        }
    }

    private func shouldCreateWarmupResource() -> Bool {
        return !isClosed && totalCreated < capacity
    }

    private func addWarmupResource(_ resource: Resource, incrementCount: Bool) {
        guard !isClosed else {
            return
        }

        if incrementCount {
            totalCreated += 1

            guard totalCreated <= capacity else {
                totalCreated -= 1
                return
            }
        }

        let id = ObjectIdentifier(resource)
        if resourceMetadata[id] == nil {
            resourceMetadata[id] = ResourceMetadata()
        }

        handOffOrReturnResource(resource)
    }

    private func recordCreationFailure() {
        _metrics.recordCreationFailure()
    }

    private func cleanExpiredWaiters() {
        var expiredWaiters: [Waiter<Resource>] = []

        waitQueue.removeAll { waiter in
            if waiter.isExpired {
                expiredWaiters.append(waiter)
                return true
            }
            return false
        }

        for waiter in expiredWaiters {
            _metrics.recordTimeout()
            waiter.continuation.resume(throwing: PoolError.timeout)
        }
    }

    private func recordResourceAcquisition(id: ObjectIdentifier) {
        resourceMetadata[id, default: ResourceMetadata()].incrementUsage()
    }

    #if DEBUG
        private func verifyAccounting() {
            let actual = available.count + leased.count
            assert(
                actual == totalCreated,
                """
                Accounting mismatch:
                  available: \(available.count)
                  leased: \(leased.count)
                  sum: \(actual)
                  totalCreated: \(totalCreated)
                """
            )
        }

        internal func testVerifyAccounting() {
            verifyAccounting()
        }
    #else
        private func verifyAccounting() {}
    #endif

    internal func waitForWarmupCompletion(timeout: Duration = .seconds(5)) async throws(PoolError) {
        let deadline = ContinuousClock.now + timeout
        while totalCreated < capacity && ContinuousClock.now < deadline {
            do {
                try await Task.sleep(for: .milliseconds(10))
            } catch {
                throw .cancelled
            }
        }
        if totalCreated < capacity {
            throw PoolError.timeout
        }
    }

    deinit {
        cleanupTask?.cancel()
    }
}

public struct Statistics: Sendable, Equatable {
    public let available: Int

    public let leased: Int

    public let capacity: Int

    public let waitQueueDepth: Int

    public let totalCreated: Int

    public var inUse: Int { leased }

    public var utilization: Double {
        guard capacity > 0 else { return 0 }
        return Double(leased) / Double(capacity)
    }

    public var hasBackpressure: Bool {
        waitQueueDepth > 0
    }
}

public struct Metrics: Sendable {
    public var currentStatistics: Statistics = Statistics(
        available: 0,
        leased: 0,
        capacity: 0,
        waitQueueDepth: 0,
        totalCreated: 0
    )

    public private(set) var totalAcquisitions: Int = 0

    public private(set) var timeouts: Int = 0

    public private(set) var validationFailures: Int = 0

    public private(set) var resetFailures: Int = 0

    public private(set) var creationFailures: Int = 0

    public private(set) var successfulReturns: Int = 0

    public private(set) var waitersQueued: Int = 0

    public private(set) var directHandoffs: Int = 0

    public private(set) var resourcesCycled: Int = 0

    private var totalWaitTime: Duration = .zero

    public var averageWaitTime: Duration? {
        guard totalAcquisitions > 0 else { return nil }
        return totalWaitTime / totalAcquisitions
    }

    public var handoffRate: Double {
        guard successfulReturns > 0 else { return 0 }
        return Double(directHandoffs) / Double(successfulReturns)
    }

    mutating func recordAcquisition(waitTime: Duration) {
        totalAcquisitions += 1
        totalWaitTime += waitTime
    }

    mutating func recordTimeout() {
        timeouts += 1
    }

    mutating func recordValidationFailure() {
        validationFailures += 1
    }

    mutating func recordResetFailure() {
        resetFailures += 1
    }

    mutating func recordCreationFailure() {
        creationFailures += 1
    }

    mutating func recordSuccessfulReturn() {
        successfulReturns += 1
    }

    mutating func recordWaiterQueued() {
        waitersQueued += 1
    }

    mutating func recordDirectHandoff() {
        directHandoffs += 1
    }

    mutating func recordResourceCycled() {
        resourcesCycled += 1
    }
}

extension Duration {
    private static func / (lhs: Duration, rhs: Int) -> Duration {
        let totalNanoseconds =
            (lhs.components.seconds * 1_000_000_000) + (lhs.components.attoseconds / 1_000_000_000)
        let avgNanoseconds = totalNanoseconds / Int64(rhs)
        return .nanoseconds(avgNanoseconds)
    }
}
