package myce

import (
	"strings"
	"testing"
)

func TestLocalExtensionReportReason(t *testing.T) {
	for _, tc := range []struct {
		name                                          string
		record                                        bool
		digest, savedFingerprint, fingerprint, device string
		want                                          string
	}{
		{name: "never checked", fingerprint: "driver", want: "not yet checked on this device; run the extension check"},
		{name: "different device never checked", record: true, savedFingerprint: "driver", fingerprint: "driver", device: "1", want: "not yet checked on this device; run the extension check"},
		{name: "driver mismatch", record: true, savedFingerprint: "old-driver", fingerprint: "driver", want: "hardware/driver fingerprint changed; setup and check required"},
		{name: "extension mismatch", record: true, digest: "old-extension", savedFingerprint: "driver", fingerprint: "driver", want: "extension fingerprint (digest) changed; setup and check required"},
		{name: "current fingerprint missing", record: true, savedFingerprint: "driver", want: "hardware/driver fingerprint unavailable; setup and check required"},
		{name: "saved fingerprint missing", record: true, fingerprint: "driver", want: "hardware/driver fingerprint unavailable; setup and check required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			e := fakeExtension(t)
			if err := InstallExtension(root, e); err != nil {
				t.Fatal(err)
			}
			if tc.record {
				digest := e.Digest
				if tc.digest != "" {
					digest = tc.digest
				}
				records := []ExtensionReport{}
				for _, work := range extensionWorks(e) {
					records = append(records, ExtensionReport{Name: e.Name, Version: e.Version, Platform: "fake", Work: work, Digest: digest, Device: tc.device, Fingerprint: tc.savedFingerprint, Setup: true, Qualified: true})
				}
				if err := writeExtensionReports(root, e, "fake", records); err != nil {
					t.Fatal(err)
				}
			}
			reports := LocalExtensionReports(root, "fake", tc.fingerprint, &Machine{Device: "0"})
			if len(reports) != len(extensionWorks(e)) {
				t.Fatal(reports)
			}
			for _, r := range reports {
				if r.Setup || r.Qualified || r.Reason != tc.want {
					t.Fatalf("report: %+v; want %q", r, tc.want)
				}
				if extensionReportDiagnostic(r.Reason) != tc.want {
					t.Fatal("public status lost reason")
				}
				if !strings.Contains(ExtensionStatus(r), tc.want) || !strings.Contains(FormatExtensionReport(r), tc.want) {
					t.Fatal("display lost reason")
				}
			}
		})
	}
}
