package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type envelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func main() {
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.GET("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(200, `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>WebSocket 回声实验</title><h1>回声连接</h1><p>这是未鉴权的本机传输实验。每条消息会返回给自己。</p><input id="text" aria-label="消息内容" value="你好 GoIM"><button id="send" disabled>发送</button><pre id="output"></pre><script>const out=document.getElementById('output'),button=document.getElementById('send'),input=document.getElementById('text');const ws=new WebSocket('ws://'+location.host+'/ws');ws.onopen=()=>{button.disabled=false;out.textContent+='已连接\n'};ws.onmessage=e=>out.textContent+=e.data+'\n';ws.onclose=()=>{button.disabled=true;out.textContent+='已断开\n'};button.onclick=()=>ws.send(JSON.stringify({type:'msg',data:{content:input.value}}));</script></html>`)
	})
	upgrader := websocket.Upgrader{} // 默认检查同源；从本服务页面连接。
	r.GET("/ws", func(c *gin.Context) {
		connection, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		connection.SetReadLimit(4096)
		for {
			_, raw, err := connection.ReadMessage()
			if err != nil {
				return
			}
			var msg envelope
			if json.Unmarshal(raw, &msg) != nil || msg.Type != "msg" {
				connection.WriteJSON(map[string]string{"type": "error"})
				continue
			}
			connection.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if connection.WriteJSON(envelope{Type: "msg", Data: msg.Data}) != nil {
				return
			}
		}
	})
	log.Println("实验 04：http://localhost:18100；未鉴权回声实验，下一步将接入真实项目鉴权和读写协程")
	log.Fatal((&http.Server{Addr: "127.0.0.1:18100", Handler: r, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}
