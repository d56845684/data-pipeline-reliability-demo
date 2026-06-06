#!/usr/bin/env bash
# 啟動 Cloudflare Quick Tunnel（免帳號），並印出要貼到 LINE Developers Console 的 webhook URL。
#
# 用法：./scripts/line-webhook-url.sh
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose --profile line up -d cloudflared

echo "等待 tunnel 建立..."
url=""
for _ in $(seq 1 30); do
  url=$(docker compose logs cloudflared 2>&1 \
        | grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' | tail -1 || true)
  [ -n "$url" ] && break
  sleep 2
done

if [ -z "$url" ]; then
  echo "❌ tunnel 啟動失敗，請查看：docker compose logs cloudflared"
  exit 1
fi

echo ""
echo "✅ Webhook URL（貼到 LINE Developers Console > Messaging API > Webhook settings）："
echo ""
echo "    $url/line/webhook"
echo ""
echo "貼上後記得："
echo "  1. 按 Verify（應回 Success）"
echo "  2. 開啟 Use webhook"
echo "  3. 用 LINE 掃 QR code 加 bot 好友，傳任意訊息 → bot 回覆你的 userId"
echo "  4. 把 userId 填入 .env 的 LINE_TARGET_ID，然後："
echo "     docker compose up -d --build alert-logger"
echo ""
echo "⚠️  Quick Tunnel 的網址每次重啟都會變，重啟後需重新貼到 LINE Console。"
