package pkg

import "github.com/segmentio/kafka-go"

type Option interface {
	Apply(option)
}

type ConsumerOption interface {
	Option
	Apply(option)
}

type ProducerOption interface {
	Option
	Apply(option)
}

type option struct {
	authentication
}

type consumerOption struct {
	option
	kafka.Balancer
}

type producerOption struct {
	option
}

type authentication struct {
	Username string
	Password string
}

func (auth *authentication) Apply(opt option) {
	opt.authentication = *auth
}

func WithAuthentication(username string, password string) Option {
	return &authentication{
		Username: username,
		Password: password,
	}
}
