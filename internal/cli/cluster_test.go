package cli

import (
	"testing"
)

func TestParseNameservers(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		want    []string
		wantErr bool
	}{
		{
			name:    "single nameserver",
			arg:     "8.8.8.8",
			want:    []string{"8.8.8.8"},
			wantErr: false,
		},
		{
			name:    "multiple nameservers",
			arg:     "8.8.8.8,8.8.4.4",
			want:    []string{"8.8.8.8", "8.8.4.4"},
			wantErr: false,
		},
		{
			name:    "with spaces",
			arg:     "8.8.8.8, 8.8.4.4",
			want:    []string{"8.8.8.8", "8.8.4.4"},
			wantErr: false,
		},
		{
			name:    "invalid IP",
			arg:     "invalid",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty string",
			arg:     "",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid characters",
			arg:     "8.8.8.8; rm -rf /",
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNameservers(tt.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseNameservers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("parseNameservers() = %v, want %v", got, tt.want)
					return
				}
				for i := range got {
					if got[i] != tt.want[i] {
						t.Errorf("parseNameservers()[%d] = %v, want %v", i, got[i], tt.want[i])
					}
				}
			}
		})
	}
}

