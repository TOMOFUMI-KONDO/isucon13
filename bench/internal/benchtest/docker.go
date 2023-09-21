package benchtest

import (
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type CleanupFn = func() error

var (
	cleanupDatabase CleanupFn
	cleanupWebapp   CleanupFn
)

func runContainer(pool *dockertest.Pool, opts *dockertest.RunOptions, retryFunc func(*dockertest.Resource) error) (*dockertest.Resource, error) {
	resource, err := pool.RunWithOptions(opts, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	if err != nil {
		return nil, err
	}
	if err := pool.Retry(func() error {
		return retryFunc(resource)
	}); err != nil {
		return nil, err
	}

	return resource, nil
}

func findProjectRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	cwdParts := strings.Split(cwd, "/")

	var projectRootIdx int
	for idx, part := range cwdParts {
		if part == "isucon13" {
			projectRootIdx = idx
			break
		}
	}

	var sliceEnd int
	if projectRootIdx+1 <= len(cwdParts) {
		sliceEnd = projectRootIdx + 1
	} else {
		sliceEnd = projectRootIdx
	}

	return strings.Join(cwdParts[:sliceEnd], "/"), nil
}

func Setup() (string, error) {
	baseDir, err := findProjectRoot()
	if err != nil {
		return "", err
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		return "", err
	}
	pool.MaxWait = 10 * time.Second
	if err := pool.Client.Ping(); err != nil {
		return "", err
	}

	network, err := pool.CreateNetwork("isupipe")
	if err != nil {
		return "", err
	}

	// run database
	databaseResource, err := runContainer(pool, &dockertest.RunOptions{
		Repository: "mysql/mysql-server",
		Tag:        "8.0.31",
		Name:       "mysql",
		Env: []string{
			"MYSQL_ROOT_HOST=%",
			"MYSQL_ROOT_PASSWORD=root",
		},
		Mounts: []string{
			strings.Join([]string{
				baseDir + "/webapp/sql/initdb.d",
				"/docker-entrypoint-initdb.d",
			}, ":"),
		},
		Networks: []*dockertest.Network{
			network,
		},
	}, func(resource *dockertest.Resource) error {
		db, err := sql.Open("mysql", fmt.Sprintf("isucon:isucon@(localhost:%s)/isupipe", resource.GetPort("3306/tcp")))
		if err != nil {
			return err
		}
		if err := db.Ping(); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return "", err
	}
	databaseIp := databaseResource.GetIPInNetwork(network)
	cleanupDatabase = func() error {
		if err := pool.Purge(databaseResource); err != nil {
			return err
		}
		return nil
	}

	// run webapp
	webappResource, err := runContainer(pool, &dockertest.RunOptions{
		Repository: "isupipe",
		Tag:        "latest",
		Name:       "isupipe",
		Env: []string{
			fmt.Sprintf("ISUCON13_MYSQL_DIALCONFIG_ADDRESS=%s", net.JoinHostPort(databaseIp, "3306")),
		},
		Networks: []*dockertest.Network{
			network,
		},
	}, func(resource *dockertest.Resource) error {
		addr := net.JoinHostPort("localhost", resource.GetPort("12345/tcp"))
		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/initialize", addr), nil)
		if err != nil {
			return err
		}

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("initialize endpoint responses unexpected status code (%d)", resp.StatusCode)
		}

		return nil
	})
	if err != nil {
		return "", err
	}
	webappIp := webappResource.GetIPInNetwork(network)
	cleanupWebapp = func() error {
		if err := pool.Purge(webappResource); err != nil {
			return err
		}
		return nil
	}

	return fmt.Sprintf("http://%s:12345", webappIp), nil
}

func Teardown() error {
	if err := cleanupDatabase(); err != nil {
		return err
	}

	if err := cleanupWebapp(); err != nil {
		return err
	}

	return nil
}
