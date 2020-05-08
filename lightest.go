package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
	"os"
	"time"
)

func populate(cmd *cobra.Command, args []string) error {

	fmt.Printf("Inside pop with args: %v\n", args)
	cluster := gocql.NewCluster("localhost")
	cluster.Timeout, _ = time.ParseDuration("30s")
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	err = session.Query(CREATE_KS).Exec()
	if err != nil {
		return err
	}
	cluster.Keyspace = "yacht"
	return nil
}

func transfer(cmd *cobra.Command, args []string) error {
	fmt.Printf("Inside pay with args: %v\n", args)
	return nil
}

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
			if err := populate(cmd, args); err != nil {
				fmt.Printf("%v\n", err)
				os.Exit(-1)
			}
		},
	}
	var payCmd = &cobra.Command{
		Use:   "pay",
		Short: "run the payments workload",
		Run: func(cmd *cobra.Command, args []string) {
			if err := transfer(cmd, args); err != nil {
				fmt.Printf("%v\n", err)
				os.Exit(-1)
			}
		},
	}
	rootCmd.AddCommand(popCmd, payCmd)
	rootCmd.Execute()
}
