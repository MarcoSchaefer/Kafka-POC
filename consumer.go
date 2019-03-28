package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"
)

type Job struct {
	Duration time.Duration
}

var (
	ConsumedJobs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "kafka",
			Subsystem: "jobs",
			Name:      "produced",
			Help:      "Total number of jobs produced",
		},
		[]string{"duration"},
	)
)

func main() {
	prometheus.MustRegister(ConsumedJobs)

	c := getKafkaConsumer("jobs")
	defer c.Close()
	go receiveJobs(c)

	server := http.NewServeMux()
	server.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":9001", server)
}

func getKafkaConsumer(topic string) *kafka.Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{topic, "^aRegex.*[Tt]opic"}, nil)

	return c
}

func processJob(job Job) {
	time.Sleep(job.Duration)
}

func receiveJobs(c *kafka.Consumer) {
	for {
		msg, err := c.ReadMessage(-1)
		job := Job{}
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			json.Unmarshal(msg.Value, &job)
			processJob(job)
			ConsumedJobs.WithLabelValues(job.Duration.String()).Inc()
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
