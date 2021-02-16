package resolver

import (
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"strings"
)

type Resolver interface {
	Resolve(dbName string) (string, error)
}

type resolver struct {
	sentinels []*redis.SentinelClient
	replaceIpAddress bool
}

func NewResolver(sentinels []*redis.SentinelClient, replaceIpAddress bool) Resolver {
	return &resolver{
		sentinels: sentinels,
		replaceIpAddress: replaceIpAddress,
	}
}

func (r *resolver) Resolve(dbName string) (string, error) {
	for _, sentinel := range r.sentinels {
		result, err := sentinel.GetMasterAddrByName(dbName).Result()
		if err != nil {
			continue
		}

		ip := strings.Join(result, ":")

		log.Info().Msgf("'%s' resolved to master: %s", dbName, ip)

		if r.replaceIpAddress {
			ip = strings.Replace(ip, "192.168.1", "192.168.2", 1)

			log.Info().Msgf("'%s' resolved to master (after IP Address replacement): %s", dbName, ip)
		}

		return ip, nil
	}

	return "", errors.New("all sentinels failed")
}
