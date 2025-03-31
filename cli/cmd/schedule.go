/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"time"

	"foobar/postumus/proto"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

// scheduleCmd represents the schedule command
var scheduleCmd = &cobra.Command{
	Use:   "schedule",
	Short: "Schedule a workflow",
	Long: `Schedule a workflow on a master. For example:

$ cli schedule --master=localhost:50000 --workflow='{}'

Schedules a new workflow.`,
	Run: func(cmd *cobra.Command, args []string) {
		master, err := cmd.Flags().GetString("master")
		if err != nil {
			fmt.Println(err)
			return
		}
		workflowFlag, err := cmd.Flags().GetString("workflow")
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("schedule called args: %v master: %s workflow: %s\n", args, master, workflowFlag)

		var workflow proto.Workflow
		err = protojson.Unmarshal([]byte(workflowFlag), &workflow)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("workflow: %s\n", protojson.Format(&workflow))

		conn, err := grpc.NewClient(master, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println(err)
			return
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := proto.NewMasterClient(conn).CreateWorkflow(ctx, &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("response: %s\n", protojson.Format(resp))
	},
}

func init() {
	rootCmd.AddCommand(scheduleCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// scheduleCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// scheduleCmd.Flags().StringP("master", "m", "localhost:50000", "master address")
	scheduleCmd.Flags().StringP("workflow", "w", "", "workflow definition JSON")
	// scheduleCmd.MarkFlagRequired("master")
	scheduleCmd.MarkFlagRequired("workflow")
}
