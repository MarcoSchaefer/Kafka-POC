package main

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"math/rand"
	"net/http"
	"time"
)

type Job struct {
	Duration time.Duration
}

var (
	ProducedJobs = prometheus.NewCounterVec(
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
	prometheus.MustRegister(ProducedJobs)

	p := getKafkaProducer()
	defer p.Close()
	go produceJobs(p, "jobs")
	go monitorSentMessages(p)

	server := http.NewServeMux()
	server.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":9001", server)
}

func getKafkaProducer() *kafka.Producer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	return p
}

func sendMessage(p *kafka.Producer, job Job, topic string) {
	b, err := json.Marshal(job)
	if err != nil {
		log.Println(err)
		return
	}
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(b),
	}, nil)

}

func monitorSentMessages(p *kafka.Producer) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				log.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		}
	}
}

func produceJobs(p *kafka.Producer, topic string) {
	for {
		job := Job{
			time.Duration(rand.Intn(4)+1) * 1000 * time.Millisecond,
		}
		ProducedJobs.WithLabelValues(job.Duration.String()).Inc()
		log.Println("New jobs produced. Sleep time: ", job.Duration, " seconds")
		sendMessage(p, job, topic)
		time.Sleep(1000 * time.Millisecond)
	}
}
