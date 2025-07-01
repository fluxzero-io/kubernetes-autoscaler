package exoscale

type MachineType struct {
	family       string
	exoscaleSize string
	cpu          string
	memory       string
	platform     bool
	size         string
}

func (m *MachineType) SizeLabelKey() string {
	if m.platform {
		return clusterSizeLabelKey
	} else {
		return applicationSizeLabelKey
	}
}

func (m *MachineType) Scope() string {
	if m.platform {
		return "flux-platform"
	} else {
		return "customer"
	}
}

func (m *MachineType) Id() string {
	return m.Scope() + "-" + m.size
}

var machineTypes = map[string]MachineType{
	"p-tiny":   {"standard", "large", "4", "8Gi", true, "tiny"},
	"p-xsmall": {"standard", "large", "4", "8Gi", true, "xsmall"},
	"p-small":  {"cpu", "extra-large", "8", "16Gi", true, "small"},
	"p-medium": {"cpu", "huge", "16", "32Gi", true, "medium"},
	"p-large":  {"cpu", "mega", "32", "64Gi", true, "large"},
	"p-xlarge": {"cpu", "mega", "32", "64Gi", true, "xlarge"},
	"p-huge":   {"cpu", "mega", "32", "64Gi", true, "huge"},
	"c-tiny":   {"standard", "small", "2", "2Gi", false, "tiny"},
	"c-xsmall": {"standard", "medium", "2", "4Gi", false, "xsmall"},
	"c-small":  {"standard", "large", "4", "8Gi", false, "small"},
	"c-medium": {"cpu", "extra-large", "8", "16Gi", false, "medium"},
	"c-large":  {"cpu", "huge", "16", "32Gi", false, "large"},
	"c-xlarge": {"cpu", "mega", "32", "64Gi", false, "xlarge"},
	"c-huge":   {"cpu", "titan", "40", "128Gi", false, "huge"},
}
