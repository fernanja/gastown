package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/daemon"
	"github.com/steveyegge/gastown/internal/style"
	"github.com/steveyegge/gastown/internal/workspace"
)

var rigDispatchPrepCmd = &cobra.Command{
	Use:   "dispatch-prep [keep-rig]",
	Short: "Free session budget for polecat dispatch",
	Long: `Prepare the town for batch polecat dispatch by freeing API session slots.

Docks all rigs except the optional keep-rig (the target for dispatch),
stops the daemon to prevent auto-restart, and reports available slots.

After dispatch is complete, run 'gt rig dispatch-done' to restore services.

Example:
  gt rig dispatch-prep ascent    # Free slots, keep ascent undocked
  gt sling as-abc ascent --no-boot  # Sling without booting rig agents
  gt rig dispatch-done           # Restore all services`,
	Args: cobra.MaximumNArgs(1),
	RunE: runRigDispatchPrep,
}

var rigDispatchDoneCmd = &cobra.Command{
	Use:   "dispatch-done",
	Short: "Restore services after polecat dispatch",
	Long: `Undock all rigs and restart the daemon after a dispatch session.

This reverses 'gt rig dispatch-prep' by undocking all docked rigs
and restarting the daemon.`,
	RunE: runRigDispatchDone,
}

func init() {
	rigCmd.AddCommand(rigDispatchPrepCmd)
	rigCmd.AddCommand(rigDispatchDoneCmd)
}

func runRigDispatchPrep(cmd *cobra.Command, args []string) error {
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return err
	}

	var keepRig string
	if len(args) > 0 {
		keepRig = args[0]
	}

	// 1. List current sessions
	sessions := daemon.ListAgentSessions()
	fmt.Printf("Current sessions: %d\n", len(sessions))
	for _, s := range sessions {
		fmt.Printf("  %s\n", s)
	}

	// 2. Dock all rigs except keepRig
	rigs, err := listRigNames(townRoot)
	if err != nil {
		return fmt.Errorf("listing rigs: %w", err)
	}

	dockedCount := 0
	for _, rig := range rigs {
		if rig == keepRig {
			fmt.Printf("%s Keeping %s undocked (dispatch target)\n", style.Bold.Render("→"), rig)
			// Stop rig agents but don't dock — polecats need the rig undocked
			_ = runGTCommand("rig", "stop", rig)
			continue
		}
		if err := runGTCommand("rig", "dock", rig); err != nil {
			style.PrintWarning("could not dock %s: %v", rig, err)
		} else {
			dockedCount++
		}
	}

	// 3. Stop daemon
	if pid := findDaemonPID(); pid > 0 {
		fmt.Printf("Stopping daemon (PID %d)...\n", pid)
		_ = killProcess(pid)
	}

	// 4. Kill remaining non-essential sessions
	for _, s := range daemon.ListAgentSessions() {
		if strings.Contains(s, "mayor") {
			continue // Keep mayor
		}
		_ = killTmuxSession(s)
	}

	// 5. Report
	remaining := daemon.CountAgentSessions()
	freeSlots := DefaultMaxSessions - remaining
	fmt.Printf("\n%s Dispatch prep complete\n", style.Bold.Render("✓"))
	fmt.Printf("  Docked: %d rigs\n", dockedCount)
	fmt.Printf("  Sessions: %d remaining → %d polecat slots available\n", remaining, freeSlots)
	if keepRig != "" {
		fmt.Printf("  Target: %s (undocked, agents stopped)\n", keepRig)
		fmt.Printf("\n  Sling with: gt sling <bead> %s --no-boot\n", keepRig)
	}
	fmt.Printf("  Restore with: gt rig dispatch-done\n")

	return nil
}

func runRigDispatchDone(cmd *cobra.Command, args []string) error {
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return err
	}

	// 1. Undock all rigs
	rigs, err := listRigNames(townRoot)
	if err != nil {
		return fmt.Errorf("listing rigs: %w", err)
	}

	for _, rig := range rigs {
		_ = runGTCommand("rig", "undock", rig)
	}

	// 2. Start daemon
	fmt.Printf("Starting daemon...\n")
	_ = runGTCommand("daemon", "start")

	// 3. Report
	fmt.Printf("%s Services restored\n", style.Bold.Render("✓"))
	return nil
}

// listRigNames returns the names of all rigs in the town.
func listRigNames(townRoot string) ([]string, error) {
	entries, err := os.ReadDir(townRoot)
	if err != nil {
		return nil, err
	}

	var rigs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// A rig has a .beads directory or a mayor/rig subdirectory
		rigPath := townRoot + "/" + e.Name()
		if _, err := os.Stat(rigPath + "/mayor/rig"); err == nil {
			rigs = append(rigs, e.Name())
		}
	}
	return rigs, nil
}

// findDaemonPID returns the daemon's PID or 0 if not running.
func findDaemonPID() int {
	// Check the PID file first
	data, err := os.ReadFile(os.Getenv("HOME") + "/gt/daemon.pid")
	if err == nil {
		var pid int
		if _, err := fmt.Sscanf(string(data), "%d", &pid); err == nil && pid > 0 {
			return pid
		}
	}
	return 0
}

// killProcess sends SIGTERM to a process.
func killProcess(pid int) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Signal(os.Kill)
}

// killTmuxSession kills a tmux session by name.
func killTmuxSession(name string) error {
	return runGTCommandSilent("tmux", "kill-session", "-t", name)
}

// runGTCommand runs a gt subcommand, printing output.
func runGTCommand(args ...string) error {
	cmd := exec.Command("gt", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// runGTCommandSilent runs a command silently.
func runGTCommandSilent(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	return cmd.Run()
}
