# Phoenix Server Deployment

This folder contains a minimal deployment path for running Project Phoenix on a fixed-IP server.

## Recommended Topology

- Run Binance-signed Phoenix commands on a VPS with a static public IP.
- Keep local development on your Mac.
- Let local OpenClaw call the VPS over SSH instead of sending signed Binance traffic from your laptop.
- Whitelist only the VPS public IP in Binance API management.

## Provider Recommendation

### Fastest path

- Amazon Lightsail with a static IP.
- Good when you want the simplest fixed-IP setup and a small Linux box.

### More control

- Amazon EC2 with an Elastic IP.
- Good when you want finer network and instance control.

### China-cloud alternative

- Alibaba Cloud ECS with an Elastic IP Address.
- Good if you prefer a mainland-friendly vendor workflow.

## Suggested Server Spec

- Ubuntu 24.04 LTS
- 2 vCPU
- 2 to 4 GB RAM
- 40 GB SSD
- Region: Singapore or Tokyo first

## Server Layout

- Repo path: `/opt/phoenix`
- Runtime user: `phoenix`
- App env file: `/etc/phoenix/phoenix.env`
- Optional SSH key for remote operation stored on your Mac

## 1. Bootstrap the Server

```bash
sudo adduser --disabled-password --gecos "" phoenix
sudo mkdir -p /opt/phoenix /etc/phoenix
sudo chown -R phoenix:phoenix /opt/phoenix
```

Copy the repo:

```bash
sudo -u phoenix git clone <your-repo-or-local-copy> /opt/phoenix
cd /opt/phoenix
sudo -u phoenix python3 -m venv .venv
sudo -u phoenix .venv/bin/pip install -r requirements.txt
```

## 2. Add Secrets

Create `/etc/phoenix/phoenix.env`:

```bash
PHOENIX_BINANCE_API_KEY=...
PHOENIX_BINANCE_API_SECRET=...
PHOENIX_BINANCE_ENV=prod
PHOENIX_BINANCE_ACCOUNT_API=auto
PHOENIX_ALL_PROXY=
PHOENIX_HTTP_PROXY=
PHOENIX_HTTPS_PROXY=
PHOENIX_WS_PROXY=
PHOENIX_WSS_PROXY=
PHOENIX_NO_PROXY=
```

Permissions:

```bash
sudo chown root:phoenix /etc/phoenix/phoenix.env
sudo chmod 640 /etc/phoenix/phoenix.env
```

## 3. Verify Fixed-IP Operation

From the server:

```bash
curl https://api.ipify.org ; echo
cd /opt/phoenix
.venv/bin/python phoenix_probe.py --symbol BTCUSDT
```

If the public IP matches your Binance whitelist, signed checks should remain stable across runs.

## 4. Install systemd Units

Copy the unit files in `deploy/systemd/` to `/etc/systemd/system/`.

Update `WorkingDirectory` and paths if your repo is not at `/opt/phoenix`.

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now phoenix-cycle.timer
sudo systemctl start phoenix-cycle.service
sudo journalctl -u phoenix-cycle.service -n 200 --no-pager
```

For the autonomous control-plane loop, also install:

```bash
sudo systemctl enable --now phoenix-autocycle.timer
sudo systemctl start phoenix-autocycle.service
sudo journalctl -u phoenix-autocycle.service -n 200 --no-pager
```

## 5. What the Timer Does

- Every 5 minutes, run a bounded `cycle`
- Generate `phoenix_snapshot.openclaw.json`
- Generate `phoenix_candidates.openclaw.json`
- Keep all Binance-signed traffic on the server

The optional `phoenix-autocycle.timer` adds:
- bounded `scan + judge`
- significant-change detection against the previous top candidate
- conditional `dispatch`
- in `MANUAL_CONFIRM` mode, a short-lived Telegram confirmation token with reason text
- in `AUTO_CONFIRM_WHEN_RULES_PASS`, automatic promotion from preflight to live entry

## 6. Remote OpenClaw Pattern

The safest remote-control pattern is:

1. Local OpenClaw runs on your Mac.
2. OpenClaw executes `deploy/ssh/phoenix_remote.sh`.
3. The script SSHes into the VPS.
4. The VPS runs `skills/phoenix-operator/phoenix_openclaw.py`.

Example:

```bash
./deploy/ssh/phoenix_remote.sh phoenix-vps probe --symbol BTCUSDT --env prod
./deploy/ssh/phoenix_remote.sh phoenix-vps cycle --scan-duration-sec 20 --top-n 10
./deploy/ssh/phoenix_remote.sh phoenix-vps preflight --symbol BTCUSDT --quote-allocation 200 --leverage 5 --env prod
```

This keeps OpenClaw interactive on your laptop while the only Binance-visible IP remains the server IP.

## 7. First-Cut Production Rules

- Keep `preflight` and `preview` as the only authenticated actions until the VPS proves stable.
- Do not enable live order placement from OpenClaw yet.
- Run `phoenix_probe.py` after any cloud networking change.
- If you replace the VPS, reassign the static IP first, then update Binance only if the IP changed.

## 8. Proxy Notes

If the server cannot reach Binance directly, configure a fixed egress HTTP proxy in `/etc/phoenix/phoenix.env`.

Typical minimal setup:

```bash
PHOENIX_ALL_PROXY=http://user:pass@proxy-host:port
```

If your proxy needs split configuration, set `PHOENIX_HTTPS_PROXY` and `PHOENIX_WSS_PROXY` explicitly instead.
