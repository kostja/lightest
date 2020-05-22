package main

import (
	llog "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
)

func main() {

	llog.SetOutput(os.Stdout)

	formatter := new(llog.TextFormatter)
	// Stackoverflow wisdom
	formatter.TimestampFormat = "Jan _2 15:04:05.000"
	formatter.FullTimestamp = true
	formatter.ForceColors = true
	llog.SetFormatter(formatter)
	var log_level string

	var rootCmd = &cobra.Command{
		Use:   "lightest [pop|pay]",
		Short: "lightest - a sample LWT application implementing an account ledger",
		Long: `
This program models an automatic banking system.  It implements 3 model
workloads, for populating the database with accounts, making transfers, and
checking correctness. It collects client-side metrics for latency and
bandwidth along the way.`,
		Version: "0.9",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if l, err := llog.ParseLevel(log_level); err != nil {
				return err
			} else {
				llog.SetLevel(l)
			}
			return nil
		},
	}
	rootCmd.PersistentFlags().StringVarP(&log_level,
		"log-level", "v",
		llog.InfoLevel.String(),
		"Log level (trace, debug, info, warn, error, fatal, panic")

	var popCmd = &cobra.Command{
		Use:     "pop",
		Aliases: []string{"populate"},
		Short:   "Create and populate the accounts database",
		Example: "./lightest populate -n 100000000",

		Run: func(cmd *cobra.Command, args []string) {
			var n, _ = cmd.Flags().GetInt("accounts")
			if err := populate(cmd, n); err != nil {
				llog.Fatalf("%v", err)
			}
		},
	}
	popCmd.PersistentFlags().IntP("accounts", "n", 100, "Number of accounts to create")

	var payCmd = &cobra.Command{
		Use:     "pay",
		Aliases: []string{"transfer"},
		Short:   "Run the payments workload",
		Run: func(cmd *cobra.Command, args []string) {
			var n, _ = cmd.Flags().GetInt("transfers")
			if err := pay(cmd, n); err != nil {
				llog.Fatalf("%v", err)
			}
		},
	}
	StatsInit()
	payCmd.PersistentFlags().IntP("transfers", "n", 100, "Number of transfers to make")
	rootCmd.AddCommand(popCmd, payCmd)
	rootCmd.Execute()
	StatsReportSummaries()
}
