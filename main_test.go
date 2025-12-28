package main

import "testing"

func TestParseS3URI(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		bucket  string
		key     string
		wantErr bool
	}{
		{
			name:   "valid simple",
			uri:    "s3://my-bucket/my-key",
			bucket: "my-bucket",
			key:    "my-key",
		},
		{
			name:   "valid nested key",
			uri:    "s3://bucket/dir/file.txt",
			bucket: "bucket",
			key:    "dir/file.txt",
		},
		{
			name:    "missing scheme",
			uri:     "https://bucket/key",
			wantErr: true,
		},
		{
			name:    "missing bucket",
			uri:     "s3:///key",
			wantErr: true,
		},
		{
			name:    "missing key",
			uri:     "s3://bucket/",
			wantErr: true,
		},
		{
			name:    "missing bucket and key",
			uri:     "s3://",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := parseS3URI(tt.uri)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tt.uri)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.uri, err)
			}
			if bucket != tt.bucket || key != tt.key {
				t.Fatalf("got bucket=%q key=%q, want bucket=%q key=%q", bucket, key, tt.bucket, tt.key)
			}
		})
	}
}

func TestCalculatePartsCount(t *testing.T) {
	tests := []struct {
		name      string
		inputSize int64
		partSize  int
		want      int
	}{
		{
			name:      "single byte",
			inputSize: 1,
			partSize:  5,
			want:      1,
		},
		{
			name:      "exact multiple",
			inputSize: 10,
			partSize:  5,
			want:      2,
		},
		{
			name:      "one over multiple",
			inputSize: 11,
			partSize:  5,
			want:      3,
		},
		{
			name:      "exact one part",
			inputSize: 5,
			partSize:  5,
			want:      1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculatePartsCount(tt.inputSize, tt.partSize)
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}
