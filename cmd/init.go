package main

import (
	"embed"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

var (
	//go:embed config.sample.toml
	efs embed.FS
)

func generateConfig() error {
	if _, err := os.Stat("config.toml"); !os.IsNotExist(err) {
		return errors.New("config.toml exists. Remove it to generate a new one.")
	}

	// Generate config file.
	b, err := efs.ReadFile("config.sample.toml")
	if err != nil {
		return fmt.Errorf("error reading sample config: %v", err)
	}

	if err := ioutil.WriteFile("config.toml", b, 0644); err != nil {
		return err
	}

	return nil
}
