package tools

import "testing"

func TestCleanEtcdPrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{name: "empty", prefix: "", want: "/testp"},
		{name: "spaces", prefix: "   ", want: "/testp"},
		{name: "adds leading slash", prefix: "orders", want: "/orders"},
		{name: "trims spaces", prefix: " /orders ", want: "/orders"},
		{name: "cleans path", prefix: "/orders//prod/", want: "/orders/prod"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CleanEtcdPrefix(tt.prefix)
			if got != tt.want {
				t.Fatalf("CleanEtcdPrefix(%q) = %q, want %q", tt.prefix, got, tt.want)
			}
		})
	}
}
