package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	start := 0.0

	processor := newProcessor(start)

	go func() {
		err := processor.Run()
		if err != nil {
			log.Fatal(err)
		}
	}()

	waitForShutdown()
	processor.Stop()
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
