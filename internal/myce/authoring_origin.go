package myce

const (
	OriginUnknown        = "unknown"
	OriginProfile        = "profile"
	OriginAdapter        = "adapter"
	OriginJobSpec        = "job_spec"
	OriginAPI            = "api"
	OriginCommand        = "command"
	OriginExtension      = "extension"
	OriginExtensionCheck = "extension_check"
	OriginFanOutChild    = "fan_out_child"
)

// Origin exposes only fixed diagnostic labels. Absence is historical unknown;
// never infer an old reader from payloads, names, paths or extension identity.
func (s Spec) Origin() string {
	switch s.AuthoringOrigin {
	case OriginProfile, OriginAdapter, OriginJobSpec, OriginAPI, OriginCommand,
		OriginExtension, OriginExtensionCheck, OriginFanOutChild:
		return s.AuthoringOrigin
	default:
		return OriginUnknown
	}
}

func submittedOrigin(s Spec) string {
	if s.ExtensionCheck != nil {
		return OriginExtensionCheck
	}
	if s.AuthoringOrigin == "" {
		return OriginAPI
	}
	return s.Origin()
}
