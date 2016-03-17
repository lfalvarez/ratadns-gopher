package util

import (
	"encoding/json"
	"os"
	"fmt"
)

type Configuration struct {
	Redis struct{
		Address string
	      }
	Log struct{
		FileName string
		MaxSize int
		MaxBackups int
		MaxAge int
	    }
	TopK struct{
		Times []string
	     }
	Geo struct{
		    Address string
	    }
	Servers []struct{
		Name string
		Data struct{
			Longitude float64
			Latitude float64
			CountryName string
		     }
	}
}


func InitConfig() Configuration {
	file, _ := os.Open("config.json")
	decoder := json.NewDecoder(file)

	configuration := Configuration{}
	err := decoder.Decode(&configuration)

	if err != nil {
		fmt.Println("Can't initialize desired configurations")
	}

	return configuration
}
