package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type LogDirConfig struct {
	ServerKey string `json:"serverKey"`
	Folder    string `json:"folder"`
}

// Config 定义配置文件的结构体
type Config struct {
	LogDirs []LogDirConfig `json:"log_dirs"`
}

// LogEntry 定义日志条目的结构体
// 用于 ClickHouse 的数据表
type LogEntry struct {
	ID        uint64    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Tag       string    `json:"tag"`
	Content   string    `json:"content"`
	ServerKey string    `json:"serverKey"`
	FilePath  string    `json:"filePath"`
}

func main() {
	// 读取配置文件
	config, err := loadConfig("config.json")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return
	}
	fmt.Printf("Loaded config: %+v\n", config)

	// 初始化 ClickHouse 连接
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout: 5 * time.Second,
	})
	fmt.Printf("Connecting to ClickHouse...\n")
	if err != nil {
		fmt.Printf("Failed to connect to ClickHouse: %v\n", err)
		return
	}
	defer conn.Close()

	// 日志正则表达式
	logRegex := regexp.MustCompile(`(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[.*?\] (\w+) (.*)`)

	// 定时任务：每5秒检查一次
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := processAndInsertLogs(config.LogDirs, logRegex, conn); err != nil {
			fmt.Printf("Error processing logs: %v\n", err)
		}
	}
}

// 读取配置文件
func loadConfig(path string) (Config, error) {
	var config Config
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		return config, err
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return config, err
	}
	return config, nil
}

// 查询 ClickHouse 获取每个 serverKey+console.log 的最大时间戳
func getMaxTimestamp(conn clickhouse.Conn, serverKey, filePath string) (time.Time, error) {
	ctx := context.Background()
	var maxTime time.Time
	err := conn.QueryRow(ctx,
		"SELECT max(timestamp) FROM logs WHERE serverKey=? AND filePath=?",
		serverKey, filePath,
	).Scan(&maxTime)
	if err != nil {
		return time.Time{}, err
	}
	return maxTime, nil
}

// 处理多个日志文件夹并插入 ClickHouse
func processAndInsertLogs(
	logDirs []LogDirConfig,
	regex *regexp.Regexp,
	conn clickhouse.Conn,
) error {
	logChan := make(chan LogEntry, 1000)
	done := make(chan struct{})
	errChan := make(chan error, 1)

	go produceLogEntries(logDirs, regex, conn, logChan, done, errChan)

	batchSize := 10000
	var batch []LogEntry
	var insertedCount int
	timeout := time.After(50 * time.Second)

	for {
		select {
		case entry, ok := <-logChan:
			if !ok {
				if len(batch) > 0 {
					if err := batchInsert(conn, batch); err != nil {
						logFailedBatch(batch, err)
						return fmt.Errorf("final batch insert error: %v", err)
					}
					insertedCount += len(batch)
				}
				fmt.Printf("Inserted %d log entries\n", insertedCount)
				return nil
			}
			batch = append(batch, entry)
			if len(batch) >= batchSize {
				if err := batchInsert(conn, batch); err != nil {
					logFailedBatch(batch, err)
					return fmt.Errorf("batch insert error: %v", err)
				}
				insertedCount += len(batch)
				batch = batch[:0]
			}
		case err := <-errChan:
			return fmt.Errorf("producer error: %v", err)
		case <-timeout:
			return fmt.Errorf("processing timeout after 50 seconds")
		case <-done:
			for len(logChan) > 0 {
				entry := <-logChan
				batch = append(batch, entry)
				if len(batch) >= batchSize {
					if err := batchInsert(conn, batch); err != nil {
						logFailedBatch(batch, err)
						return fmt.Errorf("batch insert error: %v", err)
					}
					insertedCount += len(batch)
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				if err := batchInsert(conn, batch); err != nil {
					logFailedBatch(batch, err)
					return fmt.Errorf("final batch insert error: %v", err)
				}
				insertedCount += len(batch)
			}
			fmt.Printf("Inserted %d log entries\n", insertedCount)
			return nil
		}
	}
}

// 只采集 console.log 并用最大时间戳断点续传
func produceLogEntries(
	logDirs []LogDirConfig,
	regex *regexp.Regexp,
	conn clickhouse.Conn,
	logChan chan<- LogEntry,
	done chan<- struct{},
	errChan chan<- error,
) {
	defer close(done)
	var wg sync.WaitGroup

	for _, dirCfg := range logDirs {
		wg.Add(1)
		go func(dirCfg LogDirConfig) {
			defer wg.Done()
			dir := dirCfg.Folder
			serverKey := dirCfg.ServerKey
			filePath := filepath.Join(dir, "console.log")

			// 查询 ClickHouse 获取最大时间戳
			maxTimestamp, err := getMaxTimestamp(conn, serverKey, filePath)
			if err != nil {
				errChan <- fmt.Errorf("error querying max timestamp for %s: %v", filePath, err)
				return
			}

			f, err := os.Open(filePath)
			if err != nil {
				fmt.Printf("Error opening file %s: %v\n", filePath, err)
				return
			}
			defer f.Close()

			scanner := bufio.NewScanner(f)
			scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
			var buffer []string
			for scanner.Scan() {
				line := scanner.Text()
				if regex.MatchString(line) {
					if len(buffer) > 0 {
						processLogLine(strings.Join(buffer, "\n"), regex, logChan, filePath, serverKey, maxTimestamp)
						buffer = buffer[:0]
					}
					buffer = append(buffer, line)
				} else if len(buffer) > 0 {
					buffer = append(buffer, line)
				}
			}
			if len(buffer) > 0 {
				processLogLine(strings.Join(buffer, "\n"), regex, logChan, filePath, serverKey, maxTimestamp)
			}
		}(dirCfg)
	}

	wg.Wait()
	close(logChan)
}

// 处理单条日志（含多行），只采集大于最大时间戳的日志
func processLogLine(
	line string,
	regex *regexp.Regexp,
	logChan chan<- LogEntry,
	filePath string,
	serverKey string,
	maxTimestamp time.Time,
) {
	matches := regex.FindStringSubmatch(line)
	if len(matches) != 4 {
		return
	}
	t, err := time.Parse("2006-01-02 15:04:05", matches[1])
	if err != nil {
		fmt.Printf("Error parsing timestamp in %s: %v\n", line, err)
		return
	}
	if !t.After(maxTimestamp) {
		return
	}
	logChan <- LogEntry{
		ID:        uint64(t.UnixNano()),
		Timestamp: t,
		Tag:       matches[2],
		Content:   matches[3],
		ServerKey: serverKey,
		FilePath:  filePath,
	}
}

// 批量插入到 ClickHouse，带重试
func batchInsert(conn clickhouse.Conn, entries []LogEntry) error {
	ctx := context.Background()
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO logs (id, timestamp, tag, content, serverKey, filePath) VALUES")
		if err != nil {
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}
		for _, entry := range entries {
			if err := batch.Append(entry.ID, entry.Timestamp, entry.Tag, entry.Content, entry.ServerKey, entry.FilePath); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to insert batch after %d retries", maxRetries)
}

// 记录失败日志
func logFailedBatch(entries []LogEntry, err error) {
	f, ferr := os.OpenFile("failed_logs.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if ferr != nil {
		fmt.Printf("Error opening failed_logs.json: %v\n", ferr)
		return
	}
	defer f.Close()
	data, _ := json.Marshal(entries)
	f.WriteString(string(data) + "\n")
}
