package main

import (
	"fmt"
	"github.com/spf13/cobra"
)

func main() {
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
		Use: "pop",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Inside pop with args: %v\n", args)
		},
	}
	var payCmd = &cobra.Command{
		Use:   "pay",
		Short: "run the payments workload",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Inside pay with args: %v\n", args)
		},
	}
	rootCmd.AddCommand(popCmd, payCmd)
	rootCmd.Execute()
}
