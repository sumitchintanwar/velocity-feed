// Package attributes enforces standard span attributes across the platform.
package attributes

import (
	"go.opentelemetry.io/otel/attribute"
)

// Standardized OpenTelemetry Attribute Keys
const (
	KeyEnvironment = attribute.Key("deployment.environment")
	KeyService     = attribute.Key("service.name")
	KeyComponent   = attribute.Key("component")
	KeyExchange    = attribute.Key("exchange")
	KeyAssetClass  = attribute.Key("asset_class")
	KeyTopic       = attribute.Key("topic")
	KeyGateway     = attribute.Key("gateway")
	KeyWorkerPool  = attribute.Key("worker_pool")
	KeyReplayMode  = attribute.Key("replay_mode")
	KeySnapshotType= attribute.Key("snapshot_type")
	KeyDBSystem    = attribute.Key("db.system")
	KeyDBOperation = attribute.Key("db.operation")
	KeyMsgSystem   = attribute.Key("messaging.system")
	KeyError       = attribute.Key("error")
)

// Environment returns a key-value pair for deployment.environment
func Environment(env string) attribute.KeyValue {
	return KeyEnvironment.String(env)
}

// Service returns a key-value pair for service.name
func Service(name string) attribute.KeyValue {
	return KeyService.String(name)
}

// Component returns a key-value pair for component
func Component(name string) attribute.KeyValue {
	return KeyComponent.String(name)
}

// Exchange returns a key-value pair for exchange
func Exchange(exchange string) attribute.KeyValue {
	return KeyExchange.String(exchange)
}

// AssetClass returns a key-value pair for asset_class
func AssetClass(assetClass string) attribute.KeyValue {
	return KeyAssetClass.String(assetClass)
}

// Topic returns a key-value pair for topic
func Topic(topic string) attribute.KeyValue {
	return KeyTopic.String(topic)
}

// Gateway returns a key-value pair for gateway
func Gateway(gateway string) attribute.KeyValue {
	return KeyGateway.String(gateway)
}

// WorkerPool returns a key-value pair for worker_pool
func WorkerPool(pool string) attribute.KeyValue {
	return KeyWorkerPool.String(pool)
}

// ReplayMode returns a key-value pair for replay_mode
func ReplayMode(mode string) attribute.KeyValue {
	return KeyReplayMode.String(mode)
}

// SnapshotType returns a key-value pair for snapshot_type
func SnapshotType(snapType string) attribute.KeyValue {
	return KeySnapshotType.String(snapType)
}

// DBSystem returns a key-value pair for db.system
func DBSystem(system string) attribute.KeyValue {
	return KeyDBSystem.String(system)
}

// DBOperation returns a key-value pair for db.operation
func DBOperation(operation string) attribute.KeyValue {
	return KeyDBOperation.String(operation)
}

// MessagingSystem returns a key-value pair for messaging.system
func MessagingSystem(system string) attribute.KeyValue {
	return KeyMsgSystem.String(system)
}
