package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config chứa toàn bộ cấu hình benchmark, được đọc từ file YAML.
// Xem file config.yaml mẫu để biết format cấu hình.
type Config struct {
	Mode      string       // "load" hoặc "run" (được set qua flag)
	Database  DatabaseCfg  `yaml:"database"`
	Redis     RedisCfg     `yaml:"redis"`
	Aerospike AerospikeCfg `yaml:"aerospike"`
	Data      DataCfg      `yaml:"data"`
	Load      LoadCfg      `yaml:"load"`
	Workload  WorkloadCfg  `yaml:"workload"`
	Warmup    PhaseCfg     `yaml:"warmup"`
	Benchmark PhaseCfg     `yaml:"benchmark"`
	Shutdown  ShutdownCfg  `yaml:"shutdown"`
}

// DatabaseCfg chọn loại database backend.
type DatabaseCfg struct {
	Type string `yaml:"type"` // "redis" hoặc "aerospike"
}

// RedisCfg chứa cấu hình kết nối Redis Cluster.
type RedisCfg struct {
	Addresses []string `yaml:"addresses"`
}

// AerospikeCfg chứa cấu hình kết nối Aerospike.
// Placeholder cho Phase 2, chưa được implement.
type AerospikeCfg struct {
	Hosts     []string `yaml:"hosts"`
	Namespace string   `yaml:"namespace"`
	Set       string   `yaml:"set"`
}

// DataCfg cấu hình dữ liệu benchmark.
type DataCfg struct {
	KeyCount    int   `yaml:"key_count"`    // Số lượng key khác nhau
	PayloadSize int   `yaml:"payload_size"` // Kích thước payload (bytes) cho mỗi SET
	Seed        int64 `yaml:"seed"`         // Seed cho random - đảm bảo reproducibility
}

// LoadCfg cấu hình tải benchmark.
type LoadCfg struct {
	TargetTPS      int `yaml:"target_tps"`      // Số request/giây mục tiêu
	MaxConcurrency int `yaml:"max_concurrency"` // Số worker tối đa (giới hạn in-flight requests)
	QueueSize      int `yaml:"queue_size"`      // Kích thước request queue
}

// WorkloadCfg cấu hình tỉ lệ các loại thao tác.
// Giá trị là trọng số tương đối (ví dụ: get=50, set=20, delete=30
// tương đương 50% GET, 20% SET, 30% DELETE).
type WorkloadCfg struct {
	Get    int `yaml:"get"`
	Set    int `yaml:"set"`
	Delete int `yaml:"delete"`
	Query  int `yaml:"query"`
}

// PhaseCfg cấu hình thời gian cho warmup hoặc measurement phase.
type PhaseCfg struct {
	Duration Duration `yaml:"duration"`
}

// ShutdownCfg cấu hình chính sách tắt sau khi benchmark kết thúc.
type ShutdownCfg struct {
	GracePeriod Duration `yaml:"grace_period"` // Thời gian chờ in-flight requests hoàn thành
}

// Duration là wrapper của time.Duration hỗ trợ parse từ YAML string.
// Ví dụ: "10s", "1m30s", "500ms" đều hợp lệ.
type Duration struct {
	time.Duration
}

// UnmarshalYAML parse chuỗi thời gian từ YAML (ví dụ: "10s") thành time.Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = dur
	return nil
}

// LoadConfig đọc cấu hình từ file YAML được chỉ định qua flag --config.
// Nếu không chỉ định file, sử dụng giá trị mặc định phù hợp cho chạy thử local.
func LoadConfig() (Config, error) {
	configPath := flag.String("config", "", "Path to YAML configuration file")
	tpsOverride := flag.Int("tps", 0, "Override Target TPS (0 to use config file value)")
	concurrencyOverride := flag.Int("concurrency", 0, "Override max concurrency / number of workers (0 to use config file value)")
	modeStr := flag.String("mode", "run", "Chế độ chạy: 'load' (đổ dữ liệu) hoặc 'run' (benchmark)")
	flag.Parse()

	cfg := defaultConfig()

	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			return cfg, fmt.Errorf("đọc file config %s: %w", *configPath, err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config YAML: %w", err)
		}
	}

	// Gán Mode từ cờ command line sau khi load config để đảm bảo ưu tiên cờ
	cfg.Mode = *modeStr

	if *tpsOverride > 0 {
		cfg.Load.TargetTPS = *tpsOverride
	}

	if *concurrencyOverride > 0 {
		cfg.Load.MaxConcurrency = *concurrencyOverride
	}

	return cfg, nil
}

// defaultConfig trả về cấu hình mặc định dùng cho chạy thử nhanh trên local.
func defaultConfig() Config {
	return Config{
		Database: DatabaseCfg{Type: "redis"},
		Redis:    RedisCfg{Addresses: []string{"localhost:6379"}},
		Data: DataCfg{
			KeyCount:    100000,
			PayloadSize: 64,
			Seed:        12345,
		},
		Load: LoadCfg{
			TargetTPS:      1000,
			MaxConcurrency: 10,
			QueueSize:      10000,
		},
		Workload: WorkloadCfg{
			Get:    40,
			Set:    30,
			Delete: 15,
			Query:  15,
		},
		Warmup:    PhaseCfg{Duration: Duration{5 * time.Second}},
		Benchmark: PhaseCfg{Duration: Duration{10 * time.Second}},
		Shutdown:  ShutdownCfg{GracePeriod: Duration{5 * time.Second}},
	}
}
