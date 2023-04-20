package main

import (
	"context"
	"fmt"
	"os"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"google.golang.org/api/option"
)

func main() {
	keyJson := os.Getenv("KEY_JSON")
	projectID := os.Getenv("PROJECT_ID")
	region := os.Getenv("DF_REGION")
	fmt.Println(keyJson)
	ctx := context.Background()
	c, err := dataflow.NewTemplatesClient(ctx, option.WithCredentialsJSON([]byte(keyJson)))
	if err != nil {
		fmt.Println(err)
	}
	defer c.Close()
	req := &dataflowpb.CreateJobFromTemplateRequest{
		ProjectId: projectID,
		JobName:   "test3",
		Location:  region,
		Template: &dataflowpb.CreateJobFromTemplateRequest_GcsPath{
			GcsPath: "gs://dataflow-templates-us-central1/latest/Word_Count",
		},
		Parameters: map[string]string{
			"inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
			"output":    "gs://danny-df-test/counts",
		},
		Environment: &dataflowpb.RuntimeEnvironment{TempLocation: "gs://danny-df-test/temp"},
	}
	res, err := c.CreateJobFromTemplate(ctx, req)
	if err != nil {
		fmt.Println(err)
	}
	_ = res
	fmt.Println("end")
}
