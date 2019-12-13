package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const fileForm = `
<!DOCTYPE html>
<html lang="en">
  <head>
    <title>Upload JSON Lines</title>
  </head>
  <body>
    <form enctype="multipart/form-data" action="/upload" method="post">
      <input type="file" name="jsonlines" />
      <input type="submit" value="upload" />
    </form>
  </body>
</html>
`

func uploadFile(w http.ResponseWriter, r *http.Request) {
	log.Print("File Upload Endpoint Hit")

	r.ParseMultipartForm(10 << 20)

	file, _, err := r.FormFile("jsonlines")
	if err != nil {
		fmt.Println("Error Retrieving the File")
		fmt.Println(err)
		return
	}
	defer file.Close()

	// scan each line
	reader := bufio.NewReader(file)
	line := make([]byte, 0)
	records := 0
	for {
		bytes, prefix, err := reader.ReadLine()
		if err != nil {
			break
		}
		line = append(line, bytes...)
		if !prefix {
			err = writeToKafka(line)
			records++
			line = make([]byte, 0)
		}
	}
	if err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	fmt.Fprint(w, fmt.Sprintf("%d records written", records))
}

func writeToKafka(line []byte) error {
	return producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          line,
	}, nil)
}

func webForm(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, fileForm)
}

var topic string
var producer *kafka.Producer

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic = os.Getenv("KAFKA_TOPIC")
	if brokers == "" || topic == "" {
		log.Panic("KAFKA_BROKERS and KAFKA_TOPIC must be set")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	producer = p

	if err != nil {
		log.Panicf("Failed to create producer: %s\n", err)
	}

	http.HandleFunc("/upload", uploadFile)
	http.HandleFunc("/", webForm)
	http.ListenAndServe(":8080", nil)
}
