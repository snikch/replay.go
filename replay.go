package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

func main() {
	startString := os.Getenv("START_SCORE")
	if startString == "" {
		log.Fatal(fmt.Errorf("No START_SCORE defined"))
	}

	start, err := strconv.ParseFloat(startString, 64)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Starting from score %f", start)

	processor := newProcessor(start)
	primaryQueue.Processor = processor

	go func() {
		err := processor.Run()
		if err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		err := primaryQueue.Run()
		if err != nil {
			log.Fatal(err)
		}
	}()

	waitForShutdown()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		processor.Stop()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		primaryQueue.Stop()
		wg.Done()
	}()

	wg.Wait()
}

func waitForShutdown() {
	// Create a channel to watch for quit syscalls
	quitCh := make(chan os.Signal, 2)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGQUIT)

	// Wait on a quit signal
	sig := <-quitCh
	log.Printf("Signal received: %s", sig)

	// Start a goroutine that can force quit the app if second signal received
	go func() {
		sig := <-quitCh
		log.Printf("Second signal received: %s", sig)
		log.Printf("Forcing exit")
		os.Exit(1)
	}()
}
