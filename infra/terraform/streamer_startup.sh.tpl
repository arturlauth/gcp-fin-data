#!/bin/bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -q
apt-get install -y -q python3-pip python3-venv

mkdir -p /opt/streamer
python3 -m venv /opt/streamer/venv

gsutil cp gs://${gcs_bucket}/streamer-source/app.py /opt/streamer/
gsutil cp gs://${gcs_bucket}/streamer-source/requirements.txt /opt/streamer/

/opt/streamer/venv/bin/pip install --quiet -r /opt/streamer/requirements.txt

cat > /opt/streamer/.env << 'ENVEOF'
GCP_PROJECT_ID=${project_id}
GCS_BUCKET=${gcs_bucket}
GCS_LANDING_PREFIX=landing/binance
BINANCE_STREAMS=${binance_streams}
ENVEOF

cat > /etc/systemd/system/streamer.service << 'SVCEOF'
[Unit]
Description=Binance WebSocket Streamer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/streamer
ExecStart=/opt/streamer/venv/bin/python app.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable streamer
systemctl start streamer
