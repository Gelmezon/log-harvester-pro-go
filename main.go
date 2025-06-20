package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Config struct {
	LogDirs []string `json:"log_dirs"`
}

type LogEntry struct {
	ID        uint64    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Tag       string    `json:"tag"`
	Content   string    `json:"content"`
}

func main() {
	// 读取配置文件
	config, err := loadConfig("config.json")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return
	}

	// 初始化 ClickHouse 连接
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "", // 默认空密码
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Printf("Failed to connect to ClickHouse: %v\n", err)
		return
	}
	defer conn.Close()

	// 日志正则表达式
	logRegex := regexp.MustCompile(`(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[.*?\] (\w+) (.*)`)

	// 定时任务：每分钟检查一次
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// 记录已处理的文件偏移
	processedFiles := make(map[string]int64)

	for {
		select {
		case <-ticker.C:
			// 处理日志文件并插入
			if err := processAndInsertLogs(config.LogDirs, logRegex, processedFiles, conn); err != nil {
				fmt.Printf("Error processing logs: %v\n", err)
			}
		}
	}
}

// loadConfig 读取 config.json 文件
func loadConfig(path string) (Config, error) {
	var config Config
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		return config, err
	}
	// 解析 JSON 数据
	if err := json.Unmarshal(data, &config); err != nil {
		return config, err
	}
	return config, nil
}

// processAndInsertLogs 处理多个日志文件夹并插入 ClickHouse
func processAndInsertLogs(logDirs []string, regex *regexp.Regexp, processedFiles map[string]int64, conn clickhouse.Conn) error {
	// 创建带缓冲的 channel（1000 条日志，约 1MB）
	logChan := make(chan LogEntry, 1000)
	done := make(chan struct{})
	errChan := make(chan error, 1)

	// 启动生产者 goroutines
	go produceLogEntries(logDirs, regex, processedFiles, logChan, done, errChan)

	// 消费者：主线程接收并批量插入
	batchSize := 10000 // 每批 10,000 条
	var batch []LogEntry
	var insertedCount int

	// 超时控制：50 秒
	timeout := time.After(50 * time.Second)

	for {
		select {
		case entry, ok := <-logChan:
			if !ok { // channel 关闭
				if len(batch) > 0 {
					if err := batchInsert(conn, batch); err != nil {
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
						return fmt.Errorf("batch insert error: %v", err)
					}
					insertedCount += len(batch)
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				if err := batchInsert(conn, batch); err != nil {
					return fmt.Errorf("final batch insert error: %v", err)
				}
				insertedCount += len(batch)
			}
			fmt.Printf("Inserted %d log entries\n", insertedCount)
			return nil
		}
	}
}

// produceLogEntries 并行读取多个文件夹的文件
func produceLogEntries(logDirs []string, regex *regexp.Regexp, processedFiles map[string]int64, logChan chan<- LogEntry, done chan<- struct{}, errChan chan<- error) {
	defer close(done)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 4) // 限制并发 4 个文件

	for _, dir := range logDirs {
		files, err := os.ReadDir(dir)
		if err != nil {
			errChan <- fmt.Errorf("error reading dir %s: %v", dir, err)
			return
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			wg.Add(1)
			semaphore <- struct{}{}
			go func(dir, fileName string) {
				defer wg.Done()
				defer func() { <-semaphore }()

				filePath := filepath.Join(dir, fileName)
				offset, exists := processedFiles[filePath]
				if !exists {
					offset = 0
				}

				f, err := os.Open(filePath)
				if err != nil {
					fmt.Printf("Error opening file %s: %v\n", filePath, err)
					return
				}
				defer f.Close()

				if _, err := f.Seek(offset, 0); err != nil {
					fmt.Printf("Error seeking file %s: %v\n", filePath, err)
					return
				}

				scanner := bufio.NewScanner(f)
				scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB 缓冲
				for scanner.Scan() {
					line := scanner.Text()
					matches := regex.FindStringSubmatch(line)
					if len(matches) != 4 {
						continue
					}

					t, err := time.Parse("2006-01-02 15:04:05", matches[1])
					if err != nil {
						fmt.Printf("Error parsing timestamp in %s: %v\n", line, err)
						continue
					}

					id := uint64(t.UnixNano())
					logChan <- LogEntry{
						ID:        id,
						Timestamp: t,
						Tag:       matches[2],
						Content:   matches[3],
					}
				}

				newOffset, _ := f.Seek(0, 1)
				processedFiles[filePath] = newOffset
			}(dir, file.Name())
		}
	}

	wg.Wait()
	close(logChan)
}

// batchInsert 批量插入到 ClickHouse
func batchInsert(conn clickhouse.Conn, entries []LogEntry) error {
	ctx := context.Background()
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO logs (id, timestamp, tag, content) VALUES")
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := batch.Append(
			entry.ID,
			entry.Timestamp,
			entry.Tag,
			entry.Content,
		); err != nil {
			return err
		}
	}

	return batch.Send()
}
