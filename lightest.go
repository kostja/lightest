package main

import (
	"github.com/spf13/cobra"
	"log"
	"os"
)

var llog *log.Logger

func main() {

	llog = log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	var rootCmd = &cobra.Command{
		Use:   "lightest [pop|pay]",
		Short: "lightest - a sample LWT application implementing an account ledger",
		Long: `
This program models an automatic banking system.  It implements 3 model
workloads, for populating the database with accounts, making transfers, and
checking correctness. It collects client-side metrics for latency and
bandwidth along the way.`,
		Version: "0.9",
	}
	var popCmd = &cobra.Command{
		Use:     "pop",
		Aliases: []string{"populate"},
		Short:   "Create and populate the accounts database",
		Example: "./lightest populate -n 100000000",

		Run: func(cmd *cobra.Command, args []string) {
			var n, _ = cmd.Flags().GetInt("n")
			if err := populate(cmd, n); err != nil {
				llog.Fatalf("%v", err)
			}
		},
	}
	popCmd.PersistentFlags().IntP("number", "n", 100, "Number of accounts to create")

	var payCmd = &cobra.Command{
		Use:     "pay",
		Aliases: []string{"transfer"},
		Short:   "Run the payments workload",
		Run: func(cmd *cobra.Command, args []string) {
			if err := pay(cmd, args); err != nil {
				llog.Fatalf("%v", err)
			}
		},
	}
	rootCmd.AddCommand(popCmd, payCmd)
	rootCmd.Execute()
}
