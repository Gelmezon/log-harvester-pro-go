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

// LogDirConfig 定义日志目录的配置结构体
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

	// 启动时从 ClickHouse 查询每个日志文件的最大 timestamp
	clickhouseOffsets := loadOffsetsFromClickHouse(conn, config.LogDirs)
	fmt.Printf("Loaded ClickHouse offsets: %+v\n", clickhouseOffsets)

	// 加载本地偏移量（可选）
	processedFiles, err := loadOffsets("offsets.json")
	fmt.Printf("Loaded processed files: %+v\n", processedFiles)
	if err != nil {
		fmt.Printf("Failed to load offsets: %v\n", err)
	}

	// 日志正则表达式
	logRegex := regexp.MustCompile(`(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[.*?\] (\w+) (.*)`)

	// 定时任务：每5秒检查一次
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// 定期清理偏移量
	go func() {
		for range time.Tick(1 * time.Hour) {
			cleanupOffsets(processedFiles, 7*24*time.Hour)
			if err := saveOffsets(processedFiles, "offsets.json"); err != nil {
				fmt.Printf("Failed to save offsets: %v\n", err)
			}
		}
	}()
	fmt.Printf("Starting periodic cleanup of offsets...\n")

	for {
		select {
		// 每5秒处理一次日志 集中把日志处理逻辑放在这里
		case <-ticker.C:
			if err := processAndInsertLogs(config.LogDirs, logRegex, processedFiles, clickhouseOffsets, conn); err != nil {
				fmt.Printf("Error processing logs: %v\n", err)
			}
			if err := saveOffsets(processedFiles, "offsets.json"); err != nil {
				fmt.Printf("Failed to save offsets: %v\n", err)
			}
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

// 加载偏移量
func loadOffsets(path string) (map[string]int64, error) {
	processedFiles := make(map[string]int64)
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return processedFiles, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &processedFiles); err != nil {
			return processedFiles, err
		}
	}
	return processedFiles, nil
}

// 保存偏移量
func saveOffsets(processedFiles map[string]int64, path string) error {
	data, err := json.Marshal(processedFiles)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// 清理过期偏移量
func cleanupOffsets(processedFiles map[string]int64, maxAge time.Duration) {
	for filePath := range processedFiles {
		if info, err := os.Stat(filePath); err == nil && time.Since(info.ModTime()) > maxAge {
			delete(processedFiles, filePath)
		}
	}
}

// 启动时从 ClickHouse 查询每个日志文件的最大 timestamp
func loadOffsetsFromClickHouse(conn clickhouse.Conn, logDirs []LogDirConfig) map[string]time.Time {
	offsets := make(map[string]time.Time)
	ctx := context.Background()
	for _, dirCfg := range logDirs {
		serverKey := dirCfg.ServerKey
		query := `
            SELECT filePath, max(timestamp)
            FROM logs
            WHERE serverKey = ?
            GROUP BY filePath
        `
		rows, err := conn.Query(ctx, query, serverKey)
		if err != nil {
			fmt.Printf("ClickHouse query error: %v\n", err)
			continue
		}
		for rows.Next() {
			var filePath string
			var maxTime time.Time
			if err := rows.Scan(&filePath, &maxTime); err == nil {
				offsets[serverKey+"|"+filePath] = maxTime
			}
		}
	}
	return offsets
}

// 处理多个日志文件夹并插入 ClickHouse
func processAndInsertLogs(
	logDirs []LogDirConfig,
	regex *regexp.Regexp,
	processedFiles map[string]int64,
	clickhouseOffsets map[string]time.Time,
	conn clickhouse.Conn,
) error {
	// 创建日志通道和完成信号
	logChan := make(chan LogEntry, 1000)
	// 用于通知生产者完成
	done := make(chan struct{})
	// 错误通道
	errChan := make(chan error, 1)

	// 打印开始处理日志的消息
	fmt.Printf("Starting log processing...\n")

	// 启动生产者
	go produceLogEntries(logDirs, regex, processedFiles, clickhouseOffsets, logChan, done, errChan)

	// 消费者：批量插入
	batchSize := 10000
	var batch []LogEntry
	var insertedCount int
	// 处理日志通道中的日志条目
	// 设置超时时间为50秒
	timeout := time.After(50 * time.Second)

	// 等待日志条目并批量插入到 ClickHouse
	for {
		select {
		case entry, ok := <-logChan:
			if !ok {
				if len(batch) > 0 {
					// 如果还有剩余的日志条目，进行最后一次批量插入
					if err := batchInsert(conn, batch); err != nil {
						// 如果批量插入失败，记录失败的日志
						logFailedBatch(batch, err)
						return fmt.Errorf("final batch insert error: %v", err)
					}
					// 打印插入的日志条目数量
					insertedCount += len(batch)
				}
				// 打印总共插入的日志条目数量
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

// 并行读取日志文件
func produceLogEntries(
	logDirs []LogDirConfig,
	regex *regexp.Regexp,
	processedFiles map[string]int64,
	clickhouseOffsets map[string]time.Time,
	logChan chan<- LogEntry,
	done chan<- struct{},
	errChan chan<- error,
) {
	defer close(done)
	fmt.Printf("Starting log file processing...\n")

	// 使用 WaitGroup 和信号量来限制并发数
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 4)

	// 正则表达式匹配错误日志和信息日志
	// 这里假设错误日志格式为 error.2023-10-01.log，信息日志格式为 info.2023-10-01
	errorPattern := regexp.MustCompile(`^error\.\d{4}-\d{2}-\d{2}\.log$`)
	infoPattern := regexp.MustCompile(`^info\.\d{4}-\d{2}-\d{2}$`)

	// 遍历每个日志目录
	for _, dirCfg := range logDirs {
		dir := dirCfg.Folder
		serverKey := dirCfg.ServerKey

		files, err := os.ReadDir(dir)
		if err != nil {
			errChan <- fmt.Errorf("error reading dir %s: %v", dir, err)
			return
		}
		fmt.Printf("Processing directory: %s\n", dir)
		// log 每一个文件
		fmt.Printf("Found %d files in directory %s\n", len(files), dir)
		if len(files) == 0 {
			fmt.Printf("No files found in directory %s\n", dir)
			continue
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}
			fileName := file.Name()
			filePath := filepath.Join(dir, fileName)

			if fileName == "console.log" ||
				errorPattern.MatchString(fileName) ||
				infoPattern.MatchString(fileName) {

				info, err := file.Info()
				if err == nil && time.Since(info.ModTime()) > 7*24*time.Hour {
					continue
				}
				// 限制并发数
				wg.Add(1)
				semaphore <- struct{}{}
				go func(filePath, fileName, serverKey string) {
					defer wg.Done()
					defer func() { <-semaphore }()

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
					scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
					var buffer []string
					for scanner.Scan() {
						line := scanner.Text()
						if regex.MatchString(line) {
							if len(buffer) > 0 {
								processLogLine(strings.Join(buffer, "\n"), regex, logChan, filePath, processedFiles, serverKey, clickhouseOffsets)
								buffer = buffer[:0]
							}
							buffer = append(buffer, line)
						} else if len(buffer) > 0 {
							buffer = append(buffer, line)
						}
					}
					if len(buffer) > 0 {
						processLogLine(strings.Join(buffer, "\n"), regex, logChan, filePath, processedFiles, serverKey, clickhouseOffsets)
					}

					newOffset, _ := f.Seek(0, 1)
					processedFiles[filePath] = newOffset
				}(filePath, fileName, serverKey)
			}
		}
	}

	wg.Wait()
	close(logChan)
}

// 处理单条日志（含多行）
func processLogLine(
	line string,
	regex *regexp.Regexp,
	logChan chan<- LogEntry,
	filePath string,
	processedFiles map[string]int64,
	serverKey string,
	clickhouseOffsets map[string]time.Time,
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

	// 跳过已同步过的日志
	offsetKey := serverKey + "|" + filePath
	if maxT, ok := clickhouseOffsets[offsetKey]; ok && !t.After(maxT) {
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

	processedFiles[filePath] += int64(len([]byte(line)) + 1)
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
