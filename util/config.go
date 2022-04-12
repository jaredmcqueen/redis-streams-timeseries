package util

import "github.com/spf13/viper"

type Config struct {
	RedisStreamsEndpoint    string `mapstructure:"REDIS_STREAMS_ENDPOINT"`
	RedisTimeseriesEndpoint string `mapstructure:"REDIS_TIMESERIES_ENDPOINT"`
	StartID                 string `mapstructure:"REDIS_STREAM_START"`
}

// LoadConfig loads app.env if it exists and sets envars
func LoadConfig(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}
