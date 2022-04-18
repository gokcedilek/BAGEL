package database

import (
	"io"
	"log"
	"os"
	"project/database"
	"project/util"
)

const (
	PROD = "prod"
	DEV  = "dev"
)

func main() {

	logFile, err := os.OpenFile("bagel.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	log.SetPrefix("Database" + ": ")

	if len(os.Args) != 2 || os.Args[1] != DEV && os.Args[1] != PROD {
		log.Printf("Usage: ./bin/database [%s|%s]", PROD, DEV)
		log.Println("Example: ./bin/database dev")
		return
	}

	var configPath string
	if os.Args[1] == PROD {
		configPath = "config/database_config.json"
	} else {
		configPath = "config/database_test_config.json"
	}

	var config database.DatabaseConfig
	err = util.ReadJSONConfig(configPath, &config)

	if err != nil {
		log.Fatal(err)
	}

}