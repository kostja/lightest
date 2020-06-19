package main

import (
	"github.com/ansel1/merry"
	llog "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"runtime"
	"strings"
	"time"
)

type Settings struct {
	log_level   string
	workers     int
	count       int
	host        string
	user        string
	password    string
	consistency string
	seed        int64
	zipfian     bool
	oracle      bool
}

func Defaults() Settings {
	s := Settings{}
	s.log_level = llog.InfoLevel.String()
	s.workers = 4 * runtime.NumCPU()
	s.count = 100
	s.host = "localhost"
	s.user = "cassandra"
	s.password = "cassandra"
	s.consistency = "serial"
	s.seed = time.Now().UnixNano()
	s.zipfian = false
	s.oracle = false
	return s
}

func main() {

	llog.SetOutput(os.Stdout)

	formatter := new(llog.TextFormatter)
	// Stackoverflow wisdom
	formatter.TimestampFormat = "Jan _2 15:04:05.000"
	formatter.FullTimestamp = true
	formatter.ForceColors = true
	llog.SetFormatter(formatter)
	settings := Defaults()

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
			if l, err := llog.ParseLevel(settings.log_level); err != nil {
				return err
			} else {
				llog.SetLevel(l)
			}
			if settings.workers > settings.count && settings.count > 0 {
				settings.workers = settings.count
			}
			if !strings.EqualFold(settings.consistency, "serial") &&
				!strings.EqualFold(settings.consistency, "quorum") {
				return merry.New("--consitency must be serial or quorum")
			}
			settings.consistency = strings.ToLower(settings.consistency)
			StatsSetTotal(settings.count)
			return nil
		},
	}
	rootCmd.PersistentFlags().StringVarP(&settings.log_level,
		"log-level", "v",
		settings.log_level,
		"Log level (trace, debug, info, warn, error, fatal, panic")
	rootCmd.PersistentFlags().StringVarP(&settings.host,
		"host", "",
		settings.host,
		"Cassandra host to connect to")
	rootCmd.PersistentFlags().StringVarP(&settings.user,
		"user", "u",
		settings.user,
		"Cassandra user")
	rootCmd.PersistentFlags().StringVarP(&settings.password,
		"password", "p",
		settings.password,
		"Cassandra password")
	rootCmd.PersistentFlags().IntVarP(&settings.workers,
		"workers", "w",
		settings.workers,
		"Number of workers, 4 * NumCPU if not set.")

	var popCmd = &cobra.Command{
		Use:     "pop",
		Aliases: []string{"populate"},
		Short:   "Create and populate the accounts database",
		Example: "./lightest populate -n 100000000",

		Run: func(cmd *cobra.Command, args []string) {
			if err := populate(&settings); err != nil {
				llog.Fatalf("%v", err)
			}
			llog.Infof("Total balance: %v", check(&settings, nil))
		},
	}
	popCmd.PersistentFlags().IntVarP(&settings.count,
		"count", "n",
		settings.count,
		"Number of accounts to create")
	popCmd.PersistentFlags().StringVarP(&settings.consistency,
		"consistency", "c",
		settings.consistency,
		"Consistency level (serial or quorum)")

	var payCmd = &cobra.Command{
		Use:     "pay",
		Aliases: []string{"transfer"},
		Short:   "Run the payments workload",
		Run: func(cmd *cobra.Command, args []string) {
			sum := check(&settings, nil)
			llog.Infof("Initial balance: %v", sum)

			if err := pay(&settings); err != nil {
				llog.Fatalf("%v", err)
			}
			llog.Infof("Final balance: %v", check(&settings, sum))
		},
	}
	payCmd.PersistentFlags().IntVarP(&settings.count,
		"count", "n", settings.count,
		"Number of transfers to make")
	payCmd.PersistentFlags().BoolVarP(&settings.zipfian,
		"zipfian", "z", settings.zipfian,
		"Use zipfian distribution for payments")
	payCmd.PersistentFlags().BoolVarP(&settings.oracle,
		"oracle", "o", settings.oracle,
		"Check all payments against the built-in oracle.")
	rootCmd.AddCommand(popCmd, payCmd)
	StatsInit()
	rootCmd.Execute()
}
