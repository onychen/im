package config

import (
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	MySQL    MySQLConfig    `yaml:"mysql"`
	Redis    RedisConfig    `yaml:"redis"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	JWT      JWTConfig      `yaml:"jwt"`
	File     FileConfig     `yaml:"file"`
	Moment   MomentConfig   `yaml:"moment"`
	Message  MessageConfig  `yaml:"message"`
}

type ServerConfig struct {
	Port      int    `yaml:"port"`
	PprofPort int    `yaml:"pprof_port"`
	WsPath    string `yaml:"ws_path"`
	UploadDir string `yaml:"upload_dir"`
}

type MySQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"db_name"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// KafkaConfig 描述 Kafka 集群与客户端参数。
type KafkaConfig struct {
	// Brokers 是 bootstrap broker 列表，如 ["localhost:9092"]。
	// 也可以在 YAML 中写成逗号分隔的单个字符串（便于用环境变量注入）。
	Brokers StringList `yaml:"brokers"`
	// ClientID 用于 broker 端日志与配额识别。默认 "goim"。
	ClientID string `yaml:"client_id"`
	// GroupPrefix 会拼接到各消费者组名前，便于多环境共用一个集群。默认 "goim-"。
	GroupPrefix string `yaml:"group_prefix"`
	// Partitions 是自动创建 Topic 时的分区数。默认 3。
	Partitions int `yaml:"partitions"`
	// ReplicationFactor 是自动创建 Topic 时的副本数。单机开发为 1，生产建议 3。
	ReplicationFactor int `yaml:"replication_factor"`
	// StartOffset 是消费者组首次启动时的起始位置："earliest"（默认）或 "latest"。
	StartOffset string `yaml:"start_offset"`
	// CommitIntervalMs 是 offset 异步提交间隔（毫秒）。未配置或为 0 时默认 1000；
	// 设为负数表示每条消息同步提交（吞吐较低，仅用于调试）。
	CommitIntervalMs int `yaml:"commit_interval_ms"`
}

// StringList 同时支持 YAML 序列和逗号分隔字符串两种写法。
type StringList []string

func (s *StringList) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		var raw string
		if err := value.Decode(&raw); err != nil {
			return err
		}
		*s = splitAndTrim(raw)
		return nil
	}
	var list []string
	if err := value.Decode(&list); err != nil {
		return err
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, splitAndTrim(item)...)
	}
	*s = out
	return nil
}

func splitAndTrim(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

type MessageConfig struct {
	// AtomicPipeline enables Redis Lua + Stream relay.  Set false during a
	// rollback to use the legacy Kafka consumers temporarily.
	AtomicPipeline bool `yaml:"atomic_pipeline"`
}

type JWTConfig struct {
	Secret         string `yaml:"secret"`
	AccessExpHours int    `yaml:"access_exp_hours"`
	RefreshExpDays int    `yaml:"refresh_exp_days"`
}

type FileConfig struct {
	MaxSizeMB   int      `yaml:"max_size_mb"`
	AllowedExts []string `yaml:"allowed_exts"`
	UploadDir   string   `yaml:"upload_dir"`
}

// MomentConfig 控制朋友圈 Feed 的推拉结合策略与点赞缓存/落库。
type MomentConfig struct {
	// BigUserFriendThreshold 是"大V"判定的好友数阈值。
	// 好友数 > 阈值的作者发布动态时不再写扩散到好友收件箱，
	// 仅存入作者自己的寄件箱，由好友读取时拉取合并。默认 500。
	BigUserFriendThreshold int `yaml:"big_user_friend_threshold"`
	// TimelineMaxLen 是每个用户收件箱/寄件箱 ZSet 的最大保留条数，
	// 扇出后按此长度裁剪最旧的条目，防止无限膨胀。默认 1000。
	TimelineMaxLen int `yaml:"timeline_max_len"`
	// LikePersistBatchSize 是点赞落库消费者的攒批阈值（攒够即 flush）。默认 200。
	LikePersistBatchSize int `yaml:"like_persist_batch_size"`
	// LikePersistFlushMs 是点赞落库消费者的最大攒批间隔（毫秒）。默认 500。
	LikePersistFlushMs int `yaml:"like_persist_flush_ms"`
	// LikeCacheTTLHours 是点赞 Redis 缓存（集合/计数/预热标记）的 TTL（小时），
	// 冷动态到期后自动淘汰、下次交互重新预热。默认 168（7 天）。
	LikeCacheTTLHours int `yaml:"like_cache_ttl_hours"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	data = []byte(os.ExpandEnv(string(data)))
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	// The new pipeline is the default for deployments that do not yet have a
	// message section in their config file.
	if !strings.Contains(string(data), "atomic_pipeline:") {
		cfg.Message.AtomicPipeline = true
	}

	// Kafka 默认值（未配置时生效）
	if cfg.Kafka.ClientID == "" {
		cfg.Kafka.ClientID = "goim"
	}
	if cfg.Kafka.GroupPrefix == "" {
		cfg.Kafka.GroupPrefix = "goim-"
	}
	if cfg.Kafka.Partitions <= 0 {
		cfg.Kafka.Partitions = 3
	}
	if cfg.Kafka.ReplicationFactor <= 0 {
		cfg.Kafka.ReplicationFactor = 1
	}
	if cfg.Kafka.StartOffset == "" {
		cfg.Kafka.StartOffset = "earliest"
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = 1000
	}

	// 朋友圈 Feed 默认值（未配置时生效）
	if cfg.Moment.BigUserFriendThreshold <= 0 {
		cfg.Moment.BigUserFriendThreshold = 500
	}
	if cfg.Moment.TimelineMaxLen <= 0 {
		cfg.Moment.TimelineMaxLen = 1000
	}
	if cfg.Moment.LikePersistBatchSize <= 0 {
		cfg.Moment.LikePersistBatchSize = 200
	}
	if cfg.Moment.LikePersistFlushMs <= 0 {
		cfg.Moment.LikePersistFlushMs = 500
	}
	if cfg.Moment.LikeCacheTTLHours <= 0 {
		cfg.Moment.LikeCacheTTLHours = 168
	}

	return &cfg, nil
}
