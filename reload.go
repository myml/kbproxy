package main

import (
	"fmt"
	"os"
	"time"
)

func watchConfigFile(path string, interval time.Duration, onChange func() error) {
	go func() {
		var lastMod time.Time
		if info, err := os.Stat(path); err == nil {
			lastMod = info.ModTime()
		}

		for range time.NewTicker(interval).C {
			info, err := os.Stat(path)
			if err != nil {
				fmt.Printf("[reload] ERROR: stat %s: %v\n", path, err)
				continue
			}
			if info.ModTime().After(lastMod) {
				lastMod = info.ModTime()
				fmt.Printf("[reload] config file changed, reloading...\n")
				if err := onChange(); err != nil {
					fmt.Printf("[reload] ERROR: %v\n", err)
				} else {
					fmt.Printf("[reload] config reloaded successfully\n")
				}
			}
		}
	}()
}
