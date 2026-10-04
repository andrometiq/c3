package mappings

import (
	"encoding/json"
	"testing"
)

func TestAutoModeApprovalSettings(t *testing.T) {
	defaults := AutoModeApprovalConfig{WaitSeconds: 300, GrantTTLSeconds: 120, MaxPending: 5}
	var nilFile *MappingsFile
	cases := []struct {
		name string
		file *MappingsFile
		want AutoModeApprovalConfig
	}{
		{"nil file is off", nilFile, defaults},
		{"absent block is off", &MappingsFile{}, defaults},
		{"zeros mean defaults", &MappingsFile{AutoModeApproval: &AutoModeApprovalConfig{Enabled: true}},
			AutoModeApprovalConfig{Enabled: true, WaitSeconds: 300, GrantTTLSeconds: 120, MaxPending: 5}},
		{"upper clamps", &MappingsFile{AutoModeApproval: &AutoModeApprovalConfig{WaitSeconds: 900, GrantTTLSeconds: 900, MaxPending: 50}},
			AutoModeApprovalConfig{WaitSeconds: 300, GrantTTLSeconds: 120, MaxPending: 50}},
		{"lower clamps", &MappingsFile{AutoModeApproval: &AutoModeApprovalConfig{WaitSeconds: 1, GrantTTLSeconds: -5, MaxPending: -1}},
			AutoModeApprovalConfig{WaitSeconds: 30, GrantTTLSeconds: 30, MaxPending: 1}},
	}
	for _, test := range cases {
		if got := test.file.AutoModeApprovalSettings(); got != test.want {
			t.Errorf("%s: got %+v, want %+v", test.name, got, test.want)
		}
	}
}

func TestAutoModeApprovalJSONAndClone(t *testing.T) {
	var mf MappingsFile
	if err := json.Unmarshal([]byte(`{"schema_version":1,"auto_mode_approval":{"enabled":true,"wait_seconds":60}}`), &mf); err != nil {
		t.Fatal(err)
	}
	if got := mf.AutoModeApprovalSettings(); !got.Enabled || got.WaitSeconds != 60 || got.GrantTTLSeconds != 120 {
		t.Fatalf("parsed settings %+v", got)
	}
	cloned := mf.Clone()
	cloned.AutoModeApproval.Enabled = false
	if !mf.AutoModeApproval.Enabled {
		t.Fatal("Clone aliases the auto_mode_approval block")
	}
}
