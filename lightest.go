package main

import (
	llog "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"runtime"
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
	var host = "localhost"
	var workers = 4 * runtime.NumCPU()
	var count = 100

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
			if workers > count && count > 0 {
				workers = count
			}
			return nil
		},
	}
	rootCmd.PersistentFlags().StringVarP(&log_level,
		"log-level", "v",
		llog.InfoLevel.String(),
		"Log level (trace, debug, info, warn, error, fatal, panic")
	rootCmd.PersistentFlags().StringVarP(&host,
		"host", "",
		host,
		"Cassandra host to connect to")
	rootCmd.PersistentFlags().IntVarP(&workers,
		"workers", "w",
		workers,
		"Number of workers, 4 * NumCPU if not set.")

	var popCmd = &cobra.Command{
		Use:     "pop",
		Aliases: []string{"populate"},
		Short:   "Create and populate the accounts database",
		Example: "./lightest populate -n 100000000",

		Run: func(cmd *cobra.Command, args []string) {
			StatsSetTotal(count)
			if err := populate(cmd, count, workers); err != nil {
				llog.Fatalf("%v", err)
			}
			StatsReportSummary()
			llog.Infof("Total balance: %v", check(nil))
		},
	}
	popCmd.PersistentFlags().IntVarP(&count,
		"count", "n",
		count,
		"Number of accounts to create")

	var payCmd = &cobra.Command{
		Use:     "pay",
		Aliases: []string{"transfer"},
		Short:   "Run the payments workload",
		Run: func(cmd *cobra.Command, args []string) {
			sum := check(nil)
			llog.Infof("Initial balance: %v", sum)

			StatsSetTotal(count)
			if err := pay(cmd, count, workers); err != nil {
				llog.Fatalf("%v", err)
			}
			StatsReportSummary()
			llog.Infof("Final balance: %v", check(sum))
		},
	}
	StatsInit()
	payCmd.PersistentFlags().IntVarP(&count,
		"count", "n", count,
		"Number of transfers to make")
	rootCmd.AddCommand(popCmd, payCmd)
	rootCmd.Execute()
}
