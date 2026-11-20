package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

type ChatRequest struct {
	Model   string    `json:"model"`
	Message []Message `json:"message"`
	Stream  bool      `json:"stream"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type StreamChunk struct {
	ID     string `json:"id"`
	Choice []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

var (
	upstreamURL string
	apiKey      string
	httpClient  *http.Client
)

func init() {
	upstreamURL = os.Getenv("UPSTREAM_URL")
	if upstreamURL == "" {
		upstreamURL = "https://api.openai.com/v1/chat/completions"
	}
	apiKey = os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("请设置环境变量 OPENAI_API_KEY")
	}
	// 复用同一个 Client，避免每次请求都新建连接
	httpClient = &http.Client{
		Timeout: 120 * time.Second, // 非流式请求给足超时
	}
}

// handleStream 把上游的 SSE 流逐行透传给客户端
func handleStream(w http.ResponseWriter, r *http.Request) {
	// 1. 设置 SSE 响应头
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // 防止 nginx 缓冲

	// 2. 断言 Flusher，没有它就无法实时推送
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	// 3. 先把 200 写出去，让客户端知道连接已建立
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// 4. 逐行读取上游 SSE
	scanner := bufio.NewScanner(r.Body)
	// 默认单行上限 64KB，SSE 单条 chunk 一般远小于此，
	// 但模型返回长内容时可能超限，这里放宽到 1MB
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		// 原样写出，保留 SSE 的 "data: ..." 前缀和空行分隔
		if _, err := w.Write(line); err != nil {
			// 客户端断开，停止读取，关闭上游 body 由上层 defer 负责
			return
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return
		}
		flusher.Flush()
		// 遇到 [DONE] 后上游通常还会关闭连接，这里可以提前结束
		if bytes.Contains(line, []byte("[DONE]")) {
			return
		}
	}
	if err := scanner.Err(); err != nil {
		// 此时已经向客户端写过数据，无法再改状态码，只能记日志
		log.Printf("stream read error: %v", err)
	}
}

func handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 1. 读取并解析请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req ChatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 第二步先只处理非流式；流式的分支先明确拒绝，避免误以为已经支持
	if req.Stream {
		http.Error(w, "streaming not implemented yet (step 3)", http.StatusNotImplemented)
		return
	}

	// 2.构造发往上游的请求
	upstreamReq, err := http.NewRequestWithContext(
		r.Context(), // 继承客户端请求的 context，客户端断开时上游请求也会取消
		http.MethodPost,
		upstreamURL,
		bytes.NewReader(body), // 直接把原始 body 转发过去
	)
	if err != nil {
		http.Error(w, "failed to build upstream request", http.StatusInternalServerError)
		return
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("Authorization", "Bearer "+apiKey)

	// 3. 发起上游请求
	resp, err := httpClient.Do(upstreamReq)
	if err != nil {
		log.Printf("upstream request failed: %v", err)
		http.Error(w, "upstream request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// 4. 把上游的状态码和响应体原样返回
	//    注意：Content-Type 也透传，这样客户端拿到的还是 application/json
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		// 此时响应头已经写出，只能记日志，不能再改状态码
		log.Printf("failed to copy upstream response: %v", err)
	}
}

func main() {
	http.HandleFunc("/v1/chat/completions", handleChat)
	addr := ":8080"
	log.Printf("AI gateway (step 2, non-streaming) listening on %s, upstream=%s", addr, upstreamURL)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
