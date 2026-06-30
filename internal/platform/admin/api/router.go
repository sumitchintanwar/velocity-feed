package api

import (
	"net/http"
	"net/http/pprof"
	"time"

	"go.uber.org/zap"

	"github.com/sumit/rtmds/internal/platform/admin/audit"
	"github.com/sumit/rtmds/internal/platform/admin/commands"
	"github.com/sumit/rtmds/internal/platform/admin/handlers"
	"github.com/sumit/rtmds/internal/platform/admin/middleware"
	"github.com/sumit/rtmds/internal/platform/lifecycle"
)

type RouterConfig struct {
	Manager               *lifecycle.Manager
	Version               string
	Authenticator         middleware.Authenticator
	AuditLogger           audit.Logger
	CommandBus            *commands.CommandBus
	PublisherController   commands.PublisherController
	MaintenanceController commands.MaintenanceController
	ReplayController      commands.ReplayController
	AtomicLogLevel        zap.AtomicLevel
}

// NewRouter constructs a multiplexer dedicated entirely to operational and administrative endpoints.
func NewRouter(cfg RouterConfig) *http.ServeMux {
	mux := http.NewServeMux()

	// Register commands into the bus
	cfg.CommandBus.Register("publisher/pause", func(payload map[string]interface{}) (commands.Command, error) {
		return &commands.PausePublisherCommand{Controller: cfg.PublisherController}, nil
	})
	cfg.CommandBus.Register("publisher/resume", func(payload map[string]interface{}) (commands.Command, error) {
		return &commands.ResumePublisherCommand{Controller: cfg.PublisherController}, nil
	})
	cfg.CommandBus.Register("maintenance/enable", func(payload map[string]interface{}) (commands.Command, error) {
		return &commands.EnableMaintenanceCommand{Controller: cfg.MaintenanceController}, nil
	})
	cfg.CommandBus.Register("maintenance/disable", func(payload map[string]interface{}) (commands.Command, error) {
		return &commands.DisableMaintenanceCommand{Controller: cfg.MaintenanceController}, nil
	})
	cfg.CommandBus.Register("replay/pause", func(payload map[string]interface{}) (commands.Command, error) {
		return &commands.PauseReplayCommand{Controller: cfg.ReplayController}, nil
	})
	cfg.CommandBus.Register("replay/resume", func(payload map[string]interface{}) (commands.Command, error) {
		return &commands.ResumeReplayCommand{Controller: cfg.ReplayController}, nil
	})
	cfg.CommandBus.Register("replay/seek", func(payload map[string]interface{}) (commands.Command, error) {
		tsStr, _ := payload["timestamp"].(string)
		ts, _ := time.Parse(time.RFC3339Nano, tsStr)
		return &commands.SeekReplayCommand{Controller: cfg.ReplayController, Timestamp: ts}, nil
	})
	cfg.CommandBus.Register("configuration/log-level", func(payload map[string]interface{}) (commands.Command, error) {
		level, _ := payload["level"].(string)
		return &commands.SetLogLevelCommand{Level: cfg.AtomicLogLevel, Requested: level}, nil
	})
	cfg.CommandBus.Register("configuration/profiling", func(payload map[string]interface{}) (commands.Command, error) {
		mutexFraction := -1
		blockRate := -1
		
		if v, ok := payload["mutex_fraction"].(float64); ok {
			mutexFraction = int(v)
		}
		if v, ok := payload["block_rate"].(float64); ok {
			blockRate = int(v)
		}
		
		return &commands.SetProfilingRatesCommand{
			MutexFraction: mutexFraction,
			BlockRate:     blockRate,
		}, nil
	})

	// Initialize handlers
	inspectionHandler := handlers.NewInspectionHandler(cfg.Manager, cfg.Version)
	diagnosticsHandler := handlers.NewDiagnosticsHandler()
	operationsHandler := handlers.NewOperationsHandler(cfg.CommandBus)

	// Middlewares
	authn := middleware.Authenticate(cfg.Authenticator)
	requireViewer := middleware.RequireRole(middleware.RoleViewer)
	requireOperator := middleware.RequireRole(middleware.RoleOperator)
	requireAdmin := middleware.RequireRole(middleware.RoleAdministrator)

	// 1. Inspection API (Read-only, Requires Viewer)
	mux.Handle("/inspection/runtime", authn(requireViewer(http.HandlerFunc(inspectionHandler.HandleRuntime))))
	mux.Handle("/inspection/configuration", authn(requireViewer(http.HandlerFunc(inspectionHandler.HandleConfiguration))))

	// 2. Diagnostics API (Read-only, Requires Viewer)
	mux.Handle("/diagnostics/goroutines", authn(requireViewer(http.HandlerFunc(diagnosticsHandler.HandleGoroutines))))
	mux.Handle("/diagnostics/memory", authn(requireViewer(http.HandlerFunc(diagnosticsHandler.HandleMemory))))

	// 3. Pprof (Extremely sensitive, Requires Administrator)
	mux.Handle("/diagnostics/debug/pprof/", authn(requireAdmin(http.HandlerFunc(pprof.Index))))
	mux.Handle("/diagnostics/debug/pprof/cmdline", authn(requireAdmin(http.HandlerFunc(pprof.Cmdline))))
	mux.Handle("/diagnostics/debug/pprof/profile", authn(requireAdmin(http.HandlerFunc(pprof.Profile))))
	mux.Handle("/diagnostics/debug/pprof/symbol", authn(requireAdmin(http.HandlerFunc(pprof.Symbol))))
	mux.Handle("/diagnostics/debug/pprof/trace", authn(requireAdmin(http.HandlerFunc(pprof.Trace))))
	
	// Explicitly register sub-profiles to bypass pprof.Index path prefix bug
	mux.Handle("/diagnostics/debug/pprof/heap", authn(requireAdmin(pprof.Handler("heap"))))
	mux.Handle("/diagnostics/debug/pprof/allocs", authn(requireAdmin(pprof.Handler("allocs"))))
	mux.Handle("/diagnostics/debug/pprof/goroutine", authn(requireAdmin(pprof.Handler("goroutine"))))
	mux.Handle("/diagnostics/debug/pprof/mutex", authn(requireAdmin(pprof.Handler("mutex"))))
	mux.Handle("/diagnostics/debug/pprof/block", authn(requireAdmin(pprof.Handler("block"))))
	mux.Handle("/diagnostics/debug/pprof/threadcreate", authn(requireAdmin(pprof.Handler("threadcreate"))))

	// 4. Operations API (Mutative, Requires Operator, Audited, Generic Router)
	// We map the prefix `/operations/` to the single Dispatcher.
	// We still apply audit logging, but the generic action name inside the audit logger will be derived from the path.
	mux.Handle("/operations/", authn(requireOperator(
		middleware.Audit(cfg.AuditLogger, "DynamicDispatch", "platform")(
			http.HandlerFunc(operationsHandler.HandleDispatch),
		),
	)))

	return mux
}
