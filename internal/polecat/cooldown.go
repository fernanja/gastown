package polecat

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// KillCooldown is the minimum time that must elapse after a polecat session
// is killed before it can be re-slung. Slinging immediately after a kill can
// produce a dead session where tmux starts but Claude never initializes.
// Kaizen v5 2026-03-23: observed 78-second gap between kill and sling caused crash.
const KillCooldown = 15 * time.Second

// WriteKillTimestamp records when a polecat session was last killed.
// This is used by SpawnPolecatForSling to enforce a cooldown before reuse.
func WriteKillTimestamp(rigPath string, polecatName string) {
	dir := filepath.Join(rigPath, "polecats", polecatName)
	if _, err := os.Stat(dir); err != nil {
		return // Polecat dir doesn't exist, nothing to write
	}
	ts := fmt.Sprintf("%d", time.Now().UnixMilli())
	_ = os.WriteFile(filepath.Join(dir, ".last_killed"), []byte(ts), 0644)
}

// CheckKillCooldown checks if a polecat was recently killed and returns the
// remaining cooldown duration. Returns 0 if no cooldown is needed.
func CheckKillCooldown(rigPath string, polecatName string) time.Duration {
	data, err := os.ReadFile(filepath.Join(rigPath, "polecats", polecatName, ".last_killed"))
	if err != nil {
		return 0 // No timestamp file, no cooldown needed
	}
	ms, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	killed := time.UnixMilli(ms)
	elapsed := time.Since(killed)
	if elapsed < KillCooldown {
		return KillCooldown - elapsed
	}
	return 0
}
