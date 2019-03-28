package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"time"
)

type Worker struct {
	id   string
	jobs <-chan Job
}

func NewWorker(id string, jobChannel <-chan Job) (w Worker) {
	return Worker{id, jobChannel}
}

func (w *Worker) start() {
	for {
		w.getAndProcessFirstJob()
	}
}

func (w *Worker) getAndProcessFirstJob() {
	job := <-w.jobs
	TotalCounterVec.WithLabelValues(w.id, job.Type).Inc()
	log.Println("Worker ", w.id, "processing a job for ", job.Sleep, " seconds")
	time.Sleep(job.Sleep)
}

var (
	TotalCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Subsystem: "jobs",
			Name:      "processed_total",
			Help:      "Total number of jobs processed by the workers",
		},
		// We will want to monitor the worker ID that processed the
		// job, and the type of job that was processed
		[]string{"worker_id", "type"},
	)
)
