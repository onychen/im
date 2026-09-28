# 压测工具

每个压测程序是独立的可执行入口，使用构建标签隔离，以保证 `go test ./...` 不会将多个 `main` 编译进同一包。

```powershell
go run -tags benchmark_register ./benchmark -count=2000 -url=http://localhost:18080
go run -tags benchmark_friends ./benchmark -pairs=1000 -url=http://localhost:18080
go run -tags benchmark_messages ./benchmark -url=ws://localhost:18080/ws -conns=100 -duration=15s
go run -tags benchmark_debug ./benchmark
```

`tokens.csv` and `pairs.csv` are generated locally and intentionally ignored by Git: they contain short-lived access tokens for benchmark users. Regenerate them with the first two commands rather than sharing or committing them.

WebSocket k6 脚本仍可直接执行，例如：`k6 run ws-conn-hold.js`。
