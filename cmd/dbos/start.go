package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start your DBOS application using the start commands in 'dbos-config.yaml'",
	RunE:  runStart,
}

func runStart(cmd *cobra.Command, args []string) error {

	if config == nil {
		return fmt.Errorf("no config provided")
	}

	if len(config.RuntimeConfig.Start) == 0 {
		return fmt.Errorf("no start commands found in config file")
	}

	logger.Info("Executing start commands from config file")

	for _, command := range config.RuntimeConfig.Start {
		logger.Info("Executing command", "command", command)

		var process *exec.Cmd
		if runtime.GOOS == "windows" {
			process = exec.Command("cmd", "/C", command)
		} else {
			process = exec.Command("sh", "-c", command)
		}

		process.Stdout = os.Stdout
		process.Stderr = os.Stderr
		process.Stdin = os.Stdin

		if runtime.GOOS != "windows" {
			process.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true,
			}
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		if err := process.Start(); err != nil {
			return fmt.Errorf("failed to start command: %w", err)
		}

		done := make(chan error, 1)
		go func() {
			done <- process.Wait()
		}()

		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("command failed: %w", err)
			}
		case sig := <-sigChan:
			logger.Info("Received signal, stopping...", "signal", sig.String())

			if runtime.GOOS != "windows" {
				syscall.Kill(-process.Process.Pid, syscall.SIGTERM)
			} else {
				process.Process.Kill()
			}

			os.Exit(0)
		}
	}

	return nil
}
