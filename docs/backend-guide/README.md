# GoIM 后端从零到一

在线教程：https://onychen.github.io/im/

本目录是完整静态网站，包含 19 章教程、代码阅读与开发播放工作台、五个独立实验和可下载讲义。
GitHub Pages 只托管教程，实验中的 Go 后端仍需在本机启动。

发布配置位于 ../../.github/workflows/backend-guide-pages.yml。
向 master 推送本目录或部署配置的修改后自动发布；也可在 Actions 手动运行 Deploy backend guide to GitHub Pages。
上传产物以本目录为根，因此网页资源使用相对路径，可直接在 /im/ 下访问。

首次迁移来源：已修订的网站静态产物，版本 ea47b17e0d034608122566c8dfb9d9fc990dbeec。
源码快照及校验摘要保存在 source-manifest.json；后端源码更新不会自动改变教程内容。

本地预览：

```sh
python -m http.server 18120 --directory docs/backend-guide
```

打开 http://localhost:18120/。
