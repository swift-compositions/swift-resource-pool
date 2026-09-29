// swift-tools-version: 6.4

import PackageDescription

let package = Package(
    name: "swift-resource-pool",
    platforms: [
        .macOS(.v27),
        .iOS(.v27),
        .tvOS(.v27),
        .watchOS(.v27),
        .visionOS(.v27),
    ],
    products: [
        .library(
            name: "ResourcePool",
            targets: ["ResourcePool"]
        )
    ],
    targets: [
        .target(
            name: "ResourcePool"
        ),
        .testTarget(
            name: "ResourcePoolTests",
            dependencies: ["ResourcePool"]
        )
    ],
    swiftLanguageModes: [.v6]
)

let swiftSettings: [SwiftSetting] = [
    .enableUpcomingFeature("MemberImportVisibility"),
    .enableUpcomingFeature("StrictUnsafe"),
    .enableUpcomingFeature("NonisolatedNonsendingByDefault")
//    .unsafeFlags(["-warnings-as-errors"]),
]

for index in package.targets.indices {
    package.targets[index].swiftSettings = (package.targets[index].swiftSettings ?? []) + swiftSettings
}
