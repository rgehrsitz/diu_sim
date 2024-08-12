// c:\code\diu_sim\main.go

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"log"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

type SensorData struct {
	SensorID  string  `json:"sensor_id"`
	Channel   string  `json:"channel"`
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value"`
}

var channels = []string{"temperature", "pressure", "humidity"}

func parseArguments() (int, float64, float64) {
	numSensors := flag.Int("num-sensors", 1000, "Number of sensors to simulate")
	minRate := flag.Float64("min-rate", 4.0, "Minimum publish rate in Hz")
	maxRate := flag.Float64("max-rate", 4.0, "Maximum publish rate in Hz")

	flag.Parse()

	return *numSensors, *minRate, *maxRate
}

func setupRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	return client
}

func generateSensorValue(sensorID int, channel string) float64 {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(sensorID)))
	switch channel {
	case "temperature":
		return 25.0 + r.Float64()*10.0 // Range: 25.0 to 35.0
	case "pressure":
		return 0.8 + r.Float64()*0.4 // Range: 0.8 to 1.2
	case "humidity":
		return 70.0 + r.Float64()*20.0 // Range: 70.0 to 90.0
	default:
		return r.Float64() * 100.0 // Default range: 0 to 100
	}
}

func publishSensorData(ctx context.Context, client *redis.Client, sensorID int, minRate, maxRate float64) {
	channel := channels[sensorID%len(channels)]
	sensorName := fmt.Sprintf("%s:sensor_%03d", channel, sensorID)

	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(sensorID)))

	// Start with an initial rate
	rate := minRate + r.Float64()*(maxRate-minRate)
	ticker := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer ticker.Stop()

	for range ticker.C {
		value := generateSensorValue(sensorID, channel)
		message := fmt.Sprintf("%s=%f", sensorName, value)

		err := client.Publish(ctx, channel, message).Err()
		if err != nil {
			log.Printf("Error publishing data for %s: %v\n", sensorName, err)
		} else {
			log.Printf("Published data for %s to channel %s: %s\n", sensorName, channel, message)
		}

		// Calculate and set the next tick duration
		rate = minRate + r.Float64()*(maxRate-minRate)
		nextTickDuration := time.Duration(float64(time.Second) / rate)
		ticker.Reset(nextTickDuration)

		// Check if the context has been cancelled
		select {
		case <-ctx.Done():
			return
		default:
			// Continue to the next iteration
		}
	}
}

func startSensorSimulations(ctx context.Context, numSensors int, minRate, maxRate float64) {
	client := setupRedisClient()

	for i := 0; i < numSensors; i++ {
		go publishSensorData(ctx, client, i, minRate, maxRate)
	}
}

func loadConfig() {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // YAML format
	viper.AddConfigPath(".")      // look for config in the working directory

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if desired
			log.Println("No config file found, using default values")
		} else {
			// Config file was found but another error was produced
			log.Fatalf("Error reading config file: %s", err)
		}
	} else {
		log.Println("Using config file:", viper.ConfigFileUsed())
	}

	log.Printf("Loaded configuration: %+v", viper.AllSettings())
}

func setupLogging() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	setupLogging()

	numSensors, minRate, maxRate := parseArguments()

	// Load config (will use config.yaml if it exists)
	loadConfig()

	// Override with config file values if they exist
	if viper.IsSet("num-sensors") {
		numSensors = viper.GetInt("num-sensors")
	}
	if viper.IsSet("min-rate") {
		minRate = viper.GetFloat64("min-rate")
	}
	if viper.IsSet("max-rate") {
		maxRate = viper.GetFloat64("max-rate")
	}

	// Validate rate values
	if minRate <= 0 || maxRate <= 0 {
		log.Fatalf("Error: min-rate and max-rate must be greater than 0")
	}
	if minRate > maxRate {
		log.Fatalf("Error: min-rate cannot be greater than max-rate")
	}

	log.Printf("Starting simulation with %d sensors, publishing at rates between %.6f and %.6f Hz\n", numSensors, minRate, maxRate)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startSensorSimulations(ctx, numSensors, minRate, maxRate)

	// Wait for interrupt signal to gracefully shutdown the simulator
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	log.Println("Shutting down simulator...")
	cancel()
	// Wait a bit for goroutines to finish
	time.Sleep(time.Second)
	log.Println("Simulator stopped")
}
