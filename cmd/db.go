package cmd

import (
	"fmt"

	"github.com/aita/godb/db"
	"github.com/spf13/cobra"
	"go.uber.org/multierr"
)

var createCmd = &cobra.Command{
	Use:   "create [file name]",
	Short: "Create a new database",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := db.Create(args[0])
		if err != nil {
			return err
		}
		return db.Close()
	},
}

var insertCmd = &cobra.Command{
	Use:   "insert [file name] [data]",
	Short: "Insert a new record into a database",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := db.Open(args[0])
		if err != nil {
			return err
		}
		err = db.Insert([]byte(args[1]))
		return multierr.Append(err, db.Close())
	},
}

var selectCmd = &cobra.Command{
	Use:   "select [file name]",
	Short: "select records from a database",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := db.Open(args[0])
		if err != nil {
			return err
		}
		records := db.Select()
		for i, rec := range records {
			fmt.Printf("%d: %s\n", i, rec)
		}
		return db.Close()
	},
}

func init() {
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(insertCmd)
	rootCmd.AddCommand(selectCmd)
}
