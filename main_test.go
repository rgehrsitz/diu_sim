package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestSetupRedisClient(t *testing.T) {
	client := setupRedisClient()

	if client == nil {
		t.Errorf("Redis client was not set up correctly")
		return
	}

	err := client.Ping(context.Background()).Err()
	if err != nil {
		t.Errorf("Redis client ping failed: %v", err)
	}
}

func TestMarshalSensorData(t *testing.T) {
	data := SensorData{
		SensorID:  "sensor_001",
		Channel:   "temperature",
		Timestamp: time.Now().Format(time.RFC3339),
		Value:     23.5,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		t.Errorf("Error marshaling SensorData to JSON: %v", err)
	}

	expected := fmt.Sprintf(`{"sensor_id":"sensor_001","channel":"temperature","timestamp":"%s","value":23.5}`, data.Timestamp)
	if string(jsonData) != expected {
		t.Errorf("Expected JSON: %s, got: %s", expected, jsonData)
	}
}

func TestParseArguments(t *testing.T) {
	os.Args = []string{"cmd", "--num-sensors=10", "--min-rate=5.0", "--max-rate=10.0", "--config=config.yaml"}

	numSensors, minRate, maxRate := parseArguments()

	if numSensors != 10 {
		t.Errorf("Expected numSensors to be 10, got %d", numSensors)
	}

	if minRate != 5.0 {
		t.Errorf("Expected minRate to be 5.0, got %f", minRate)
	}

	if maxRate != 10.0 {
		t.Errorf("Expected maxRate to be 10.0, got %f", maxRate)
	}

}

func TestPublishSensorData(t *testing.T) {
	ctx := context.Background()
	client := setupRedisClient()

	sensorID := 1
	minRate := 4.0
	maxRate := 5.0

	go publishSensorData(ctx, client, sensorID, minRate, maxRate)

	channel := channels[sensorID%len(channels)]
	pubsub := client.Subscribe(ctx, channel)
	defer pubsub.Close()

	timeout := time.After(5 * time.Second)
	ch := pubsub.Channel()

	for {
		select {
		case msg := <-ch:
			if msg == nil {
				continue
			}
			var data SensorData
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				t.Errorf("Error unmarshaling message: %v", err)
			}
			if data.SensorID != fmt.Sprintf("sensor_%03d", sensorID) {
				t.Errorf("Expected sensor ID %s, got %s", fmt.Sprintf("sensor_%03d", sensorID), data.SensorID)
			}
			if data.Channel != channel {
				t.Errorf("Expected channel %s, got %s", channel, data.Channel)
			}
			return
		case <-timeout:
			t.Fatalf("Did not receive message in time")
		}
	}
}

func TestLoadConfig(t *testing.T) {
	configContent := `
num-sensors: 20
min-rate: 6.0
max-rate: 12.0
`
	configFile, err := os.CreateTemp("", "config.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp config file: %v", err)
	}
	defer os.Remove(configFile.Name())

	if _, err := configFile.Write([]byte(configContent)); err != nil {
		t.Fatalf("Failed to write to temp config file: %v", err)
	}

	configFile.Close()

	viper.SetConfigFile(configFile.Name())
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		t.Fatalf("Error reading config file, %s", err)
	}

	if viper.GetInt("num-sensors") != 20 {
		t.Errorf("Expected num-sensors to be 20, got %d", viper.GetInt("num-sensors"))
	}

	if viper.GetFloat64("min-rate") != 6.0 {
		t.Errorf("Expected min-rate to be 6.0, got %f", viper.GetFloat64("min-rate"))
	}

	if viper.GetFloat64("max-rate") != 12.0 {
		t.Errorf("Expected max-rate to be 12.0, got %f", viper.GetFloat64("max-rate"))
	}
}
