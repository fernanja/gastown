package cmd

import (
	"testing"

	"github.com/steveyegge/gastown/internal/polecat"
)

func TestEffectivePolecatState(t *testing.T) {
	tests := []struct {
		name string
		item PolecatListItem
		want polecat.State
	}{
		{
			name: "session-running-done-becomes-working",
			item: PolecatListItem{
				State:          polecat.StateDone,
				SessionRunning: true,
			},
			want: polecat.StateWorking,
		},
		{
			// Kaizen 2026-03-23: without town.log, session-dead+working → crashed
			// (no [done] entry means polecat didn't complete)
			name: "session-dead-working-becomes-crashed-without-townlog",
			item: PolecatListItem{
				State:          polecat.StateWorking,
				SessionRunning: false,
			},
			want: polecat.StateCrashed,
		},
		{
			name: "zombie-is-never-rewritten",
			item: PolecatListItem{
				State:          polecat.StateZombie,
				SessionRunning: false,
				Zombie:         true,
			},
			want: polecat.StateZombie,
		},
		{
			name: "idle-session-dead-stays-idle",
			item: PolecatListItem{
				State:          polecat.StateIdle,
				SessionRunning: false,
			},
			want: polecat.StateIdle,
		},
		{
			name: "idle-session-running-becomes-working",
			item: PolecatListItem{
				State:          polecat.StateIdle,
				SessionRunning: true,
			},
			want: polecat.StateWorking,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pass empty townRoot — without a valid town.log, crashed is the
			// correct inference for session-dead+working (safer than assuming done).
			got := effectivePolecatState(tt.item, "")
			if got != tt.want {
				t.Fatalf("effectivePolecatState() = %q, want %q", got, tt.want)
			}
		})
	}
}
