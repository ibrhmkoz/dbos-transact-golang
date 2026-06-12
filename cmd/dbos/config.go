package main

type Config struct {
	Name          string        `mapstructure:"name"`
	DatabaseUrl   string        `mapstructure:"database_url"`
	RuntimeConfig RuntimeConfig `mapstructure:"runtimeConfig"`
	Database      Database      `mapstructure:"database"`
}

type RuntimeConfig struct {
	Start []string `mapstructure:"start"`
}

type Database struct {
	Migrate []string `mapstructure:"migrate"`
}
