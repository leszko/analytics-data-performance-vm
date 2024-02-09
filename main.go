package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"time"
)

const msgTemplate = `
{
    "session_id": "%s",
    "timestamp": %d,
    "playback_id": "%s",
    "ip":"73.152.182.50",
    "protocol":"video/mp4",
    "page_url": "https://www.fishtank.live/",
    "source_url": "https://vod-cdn.lp-playback.studio/raw/jxf4iblf6wlsyor6526t4tcmtmqa/catalyst-vod-com/hls/362f9l7ekeoze518/1080p0.mp4?tkn=8b140ec6b404a",
    "player": "video-@livepeer/react@3.1.9",
    "timestamp_ts": "2023-08-27 10:11:02.957000 UTC",
    "user_id": "%s",
    "d_storage_url": "",
    "source":"stream/asset/recording",
    "creator_id": "%s",
    "deviceType": "%s",
    "device_model": "iPhone 12",
    "device_brand": "Apple",
    "browser": "%s",
    "os": "iOS",
    "cpu": "amd64",
    "playback_geo_hash": "eyc",
    "playback_continent_name": "%s",
    "playback_country_code": "US",
    "playback_country_name": "%s",
    "playback_subdivision_name": "Calirfornia",
    "playback_timezone": "America/Los_Angeles",
    "data": {
        "errors": %d, 
        "playtime_ms": 4500,
        "ttff_ms": 300,
        "preload_time_ms": 1000,
        "autoplay_status": "auto",
        "buffer_ms": 50,
        "event": {
            "type": "heartbeat",
            "timestamp":%d,
            "payload": "heartbeat message"
        }
    }
}
`

var (
	deviceTypes = []string{
		"Macintosh",
		"Android",
		"iPhone",
		"iPad",
		"Windows",
	}
	browsers = []string{
		"Chrome",
		"Safari",
		"Edge",
		"Explorer",
		"Firefox",
	}
	countries = []string{
		"Poland",
		"Germany",
		"Ukraine",
		"France",
		"Spain",
		"Czechia",
		"Sweden",
		"Norway",
		"Finland",
		"Denmark",
		"Switzerland",
		"United Kingdom",
		"Ireland",
		"Netherlands",
		"Belgium",
		"Austria",
		"Portugal",
		"Greece",
	}
	playbackIds = []string{
		"abcdefgh-1",
		"abcdefgh-2",
		"abcdefgh-3",
		"abcdefgh-4",
		"abcdefgh-5",
		"abcdefgh-6",
		"abcdefgh-7",
		"abcdefgh-8",
		"abcdefgh-9",
		"abcdefgh-10",
		"abcdefgh-11",
		"abcdefgh-12",
		"abcdefgh-13",
		"abcdefgh-14",
		"abcdefgh-15",
		"abcdefgh-16",
		"abcdefgh-17",
		"abcdefgh-18",
		"abcdefgh-19",

		"ijklmnop-1",
		"ijklmnop-2",
		"ijklmnop-3",
		"ijklmnop-4",
		"ijklmnop-5",
		"ijklmnop-6",
		"ijklmnop-7",
		"ijklmnop-8",
		"ijklmnop-9",
		"ijklmnop-10",
		"ijklmnop-11",
		"ijklmnop-12",
		"ijklmnop-13",
		"ijklmnop-14",
		"ijklmnop-15",
		"ijklmnop-16",
		"ijklmnop-17",
		"ijklmnop-18",
		"ijklmnop-19",
		"ijklmnop-20",
		"ijklmnop-21",
		"ijklmnop-22",
		"ijklmnop-23",
		"ijklmnop-24",

		"qrstuvwx-1",
		"qrstuvwx-2",
		"qrstuvwx-3",
		"qrstuvwx-4",
		"qrstuvwx-5",
		"qrstuvwx-6",
		"qrstuvwx-7",
		"qrstuvwx-8",
		"qrstuvwx-9",
		"qrstuvwx-10",
		"qrstuvwx-11",
		"qrstuvwx-12",
		"qrstuvwx-13",
		"qrstuvwx-14",
		"qrstuvwx-15",
		"qrstuvwx-16",
		"qrstuvwx-17",
		"qrstuvwx-18",
	}
)

type playbackLog struct {
	playbackID string
	sessionID  string
	deviceType string
	browser    string
	country    string
	creatorId  string
}

var (
	metrics = map[string]map[string]map[string]map[string]map[string]map[string]struct{}{}
)

func main() {
	var (
		playbackID    = "abcdefgh-1"
		sessionNumber = 10000
		kafkaTopic    = "playbackLogs4"
	)
	flag.StringVar(&playbackID, "playback-id", playbackID, "playbackID")
	flag.IntVar(&sessionNumber, "session-number", sessionNumber, "number of concurrent sessions")
	flag.StringVar(&kafkaTopic, "kafka-topic", kafkaTopic, "kafka topic")
	flag.Parse()

	tooks := make(chan time.Duration, 10000)

	done := make(chan bool)

	for i := 0; i < sessionNumber; i++ {
		cli := newVictorMetricsClient()

		go func() {
			// Start each goroutine with a random delay
			time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
			for {
				tooks <- queryVM(cli)
				time.Sleep(5 * time.Second)
			}
		}()
	}

	t := time.NewTicker(5 * time.Second)
	var count, sum int64
	go func() {
		for {
			select {
			case tookSingle := <-tooks:
				sum += int64(tookSingle)
				count++
			case <-t.C:
				fmt.Printf("avg: %.2fms, count: %v\n", float64(sum)/float64(count)/1000000, count)
				count = 0
				sum = 0
				t.Reset(5 * time.Second)
			}
		}
	}()

	<-done
}

var queries = []string{
	`sum(views)`,
	`sum(views{playbackID="abcdefgh-1"})`,
	`sum(views{userID="userid-12345"})`,
	`sum(views{country="Poland",browser="Chrome"})`,
}

func queryVM(c *vmClient) time.Duration {
	//fmt.Println("QUERY")

	// Define the query to send
	query := queries[len(queries)-1]

	// Create the HTTP client
	client := &http.Client{}

	// Create a GET request with the query parameters
	req, err := http.NewRequest("GET", c.url, nil)
	if err != nil {
		fmt.Println("Error creating HTTP request:", err)
		return 0
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	// Add the query as a URL parameter
	q := req.URL.Query()
	q.Add("query", query)
	req.URL.RawQuery = q.Encode()

	// Send the request
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending HTTP request:", err)
		return 0
	}
	defer resp.Body.Close()

	// Read the response body
	_, err = io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return 0
	}
	end := time.Now()

	// Print the response body
	//fmt.Println("Response:", string(body))
	took := end.Sub(start)
	//fmt.Println("Took: %v", took)
	return took
}

type vmClient struct {
	url   string
	token string
}

func (c *vmClient) send(msg string) {
	var jsonStr = []byte(msg)
	client := &http.Client{
		Timeout: time.Second * 10,
	}

	req, err := http.NewRequest("POST", c.url, bytes.NewBuffer(jsonStr))
	if err != nil {
		fmt.Println("Error creating HTTP request:", err)
		return
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending HTTP request:", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Received non-OK status code:", resp.StatusCode)
		return
	}

	fmt.Println("Metric sent successfully")
}

func newVictorMetricsClient() *vmClient {
	url, _ := os.LookupEnv("VM_URL")
	token, _ := os.LookupEnv("VM_TOKEN")
	return &vmClient{
		url,
		token,
	}
}
