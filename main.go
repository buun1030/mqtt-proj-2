package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func main() {
	// Set up MQTT client options
	mqttOptions := mqtt.NewClientOptions().AddBroker("tcp://test.mosquitto.org:1883")
	mqttOptions.SetClientID("go-mqtt-influxdb-example-2")

	// Set up InfluxDB client options
	influxdbUrl := "http://localhost:8086"
	influxdbToken := "jAgJSZ566m8JSx3Addeh0ZZd8HmnlTVLti4W7nEZv1AN4ikrMKg2e_idBjBns2ylkpfzaQfSM28RTakj7iMzvA=="
	influxdbOrg := "gideon-one"
	influxdbBucket := "random-number"
	influxOptions := influxdb2.DefaultOptions()
	influxOptions.SetBatchSize(5000)
	influxOptions.SetFlushInterval(1500)

	// Create a new InfluxDB client
	influxdbClient := influxdb2.NewClientWithOptions(influxdbUrl, influxdbToken, influxdb2.DefaultOptions().SetBatchSize(20))

	// Set up InfluxDB write API options
	writeAPI := influxdbClient.WriteAPI(influxdbOrg, influxdbBucket)

	// Create a new MQTT mqttClient
	mqttClient := mqtt.NewClient(mqttOptions)

	// Connect to the MQTT broker
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("MQTT connection failed: %v", token.Error())
	}

	// Subscribe to the MQTT topic
	if token := mqttClient.Subscribe("g194/gendata", 0, func(client mqtt.Client, message mqtt.Message) {
		// Extract data from the received message
		payload := string(message.Payload())
		// Assuming that the payload is a comma-separated string of values
		values := strings.Split(payload, ",")

		// Create a new InfluxDB point
		point := influxdb2.NewPoint(
			"measurement-name",
			map[string]string{
				"tag-key": "tag-value",
			},
			map[string]interface{}{
				"field-key-1": values[0],
				"field-key-2": values[1],
				"field-key-3": values[2],
				"field-key-4": values[3],
				"field-key-5": values[4],
				"field-key-6": values[5],
			},
			time.Now(),
		)

		// Write the point to InfluxDB
		fmt.Println(point)
		writeAPI.WritePoint(point)
	}); token.Wait() && token.Error() != nil {
		log.Fatalf("MQTT subscription failed: %v", token.Error())
	}

	// Wait for interrupts
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// Disconnect from the MQTT broker
	mqttClient.Disconnect(250)

	// Close the InfluxDB client
	influxdbClient.Close()
}
