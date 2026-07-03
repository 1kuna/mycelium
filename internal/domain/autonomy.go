package domain

type AutonomyPolicy struct {
	AutoScale                  bool `json:"auto_scale"`
	AutoStage                  bool `json:"auto_stage"`
	MaxReplicasPerModelPerNode int  `json:"max_replicas_per_model_per_node"`
	MaxReplicasPerModel        int  `json:"max_replicas_per_model"`
	ScaleCooldownMS            int  `json:"scale_cooldown_ms"`
	IdleTTLMS                  int  `json:"idle_ttl_ms"`
	MaxWaitDefaultMS           int  `json:"max_wait_default_ms"`
}
