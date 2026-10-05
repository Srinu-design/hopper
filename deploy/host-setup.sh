#!/usr/bin/env bash
# One-time setup of a fresh Ubuntu 24.04 LTS server (the EC2 instance) for Hopper. As root:
#
#   sudo DEPLOY_PUBKEY="ssh-ed25519 AAAA... github-deploy" SITE_ADDRESS=:80 bash host-setup.sh
#
# It installs Docker Engine with the Compose plugin (from Docker's own apt repository), adds
# 2 GB of swap, turns SSH password and root logins off, creates /opt/hopper with deploy.sh and
# an .env of freshly generated secrets (mode 600), and lets the CI deploy key run deploy.sh and
# nothing else. Safe to run again: whatever is already in place (including .env and its
# secrets) is left alone, except deploy.sh, which is replaced by the copy next to this script,
# and the deploy key, which a new DEPLOY_PUBKEY replaces.
#
#   DEPLOY_USER    the account that deploys (default ubuntu, the AMI's login user)
#   DEPLOY_PUBKEY  public half of the CI deploy key; installed as a forced command
#   SITE_ADDRESS   ":80" for plain HTTP on the bare IP (default), or a domain name pointing at
#                  this host, for automatic HTTPS
#   HOPPER_REF     the git ref deploy.sh is fetched from when it is not next to this script
set -euo pipefail

DEPLOY_USER="${DEPLOY_USER:-ubuntu}"
ROOT=/opt/hopper
REF="${HOPPER_REF:-main}"
RAW="https://raw.githubusercontent.com/Srinu-design/hopper/$REF"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IN_CONTAINER=0
[ -f /.dockerenv ] && IN_CONTAINER=1   # only for testing this script; never on a real server

say() { printf '==> %s\n' "$*"; }
[ "$(id -u)" -eq 0 ] || {
	echo "run as root: sudo bash $0" >&2
	exit 1
}
id "$DEPLOY_USER" >/dev/null 2>&1 || {
	echo "no user $DEPLOY_USER (set DEPLOY_USER)" >&2
	exit 1
}
home="$(getent passwd "$DEPLOY_USER" | cut -d: -f6)"
keys="$home/.ssh/authorized_keys"
forced="command=\"$ROOT/deploy.sh --from-ssh\""
# Check the deploy key before changing anything.
if [ -n "${DEPLOY_PUBKEY:-}" ]; then
	pubkey_re='^(ssh-[a-z0-9-]+|ecdsa-sha2-[a-z0-9-]+|sk-[a-z0-9@.-]+) [A-Za-z0-9+/]+={0,3}( [^[:cntrl:]]*)?$'
	if ! [[ "$DEPLOY_PUBKEY" =~ $pubkey_re ]]; then
		echo "DEPLOY_PUBKEY must be one line, the public key (hopper-deploy.pub), never the private key" >&2
		exit 1
	fi
	# sshd uses the first line that matches a key: if this key already logs in unrestricted
	# (someone's own login key), the forced command would never apply to it.
	if [ -f "$keys" ] && awk -v f="$forced" -v k="$(cut -d' ' -f2 <<<"$DEPLOY_PUBKEY")" \
		'index($0, f) == 0 && index($0, k) > 0 { found = 1 } END { exit !found }' "$keys"; then
		echo "this key already logs in as $DEPLOY_USER without restrictions; make a separate key" \
			"for CI (docs/deploy.md, step 2)" >&2
		exit 1
	fi
fi

say "packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q ca-certificates curl rsync python3 util-linux openssl >/dev/null

restart_docker=0
if ! command -v docker >/dev/null || ! docker compose version >/dev/null 2>&1; then
	say "Docker Engine and the Compose plugin, from download.docker.com"
	install -m 0755 -d /etc/apt/keyrings
	curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
	chmod a+r /etc/apt/keyrings/docker.asc
	# shellcheck disable=SC1091  # /etc/os-release is there on every Ubuntu
	codename="$(. /etc/os-release && echo "$VERSION_CODENAME")"
	echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc]" \
		"https://download.docker.com/linux/ubuntu $codename stable" >/etc/apt/sources.list.d/docker.list
	apt-get update -q
	apt-get install -y -q docker-ce docker-ce-cli containerd.io docker-buildx-plugin \
		docker-compose-plugin >/dev/null
	restart_docker=1
fi
# Capped logs for every container, and containers that keep running while dockerd restarts
# (an unattended upgrade of Docker itself then leaves the stack running).
if [ ! -f /etc/docker/daemon.json ]; then
	mkdir -p /etc/docker
	cat >/etc/docker/daemon.json <<'EOF'
{
  "log-driver": "json-file",
  "log-opts": { "max-size": "10m", "max-file": "5" },
  "live-restore": true
}
EOF
	restart_docker=1
fi
if [ "$IN_CONTAINER" = 0 ]; then
	systemctl enable --now docker >/dev/null
	# Only when its config is new: on a second run the stack keeps running untouched.
	if [ "$restart_docker" = 1 ]; then systemctl restart docker; fi
fi
usermod -aG docker "$DEPLOY_USER"

# GitHub's hosted runners have no fixed addresses, so port 22 is open to the internet for the
# deploy key: keys only, and no root logins. Ubuntu's cloud images already refuse passwords;
# this makes it explicit, and wins over later files in sshd_config.d (the first value counts).
if [ -d /etc/ssh/sshd_config.d ]; then
	say "SSH: keys only"
	cat >/etc/ssh/sshd_config.d/10-hopper.conf <<'EOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin no
EOF
	if ! sshd -t; then
		rm -f /etc/ssh/sshd_config.d/10-hopper.conf
		echo "warning: sshd rejected its config; left SSH settings as they were" >&2
	elif [ "$IN_CONTAINER" = 0 ]; then
		systemctl try-reload-or-restart ssh
	fi
fi

if [ "$IN_CONTAINER" = 0 ] && [ -z "$(swapon --show --noheadings)" ]; then
	say "2 GB swap"
	fallocate -l 2G /swapfile
	chmod 600 /swapfile
	mkswap /swapfile >/dev/null
	swapon /swapfile
	grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >>/etc/fstab
	echo 'vm.swappiness=10' >/etc/sysctl.d/90-hopper-swap.conf
	sysctl -q -p /etc/sysctl.d/90-hopper-swap.conf
fi

say "$ROOT"
install -d -o "$DEPLOY_USER" -g "$DEPLOY_USER" -m 750 "$ROOT" "$ROOT/releases"
if [ -f "$HERE/deploy.sh" ]; then
	install -o "$DEPLOY_USER" -g "$DEPLOY_USER" -m 755 "$HERE/deploy.sh" "$ROOT/deploy.sh"
else
	curl -fsSL "$RAW/deploy/deploy.sh" -o "$ROOT/deploy.sh"
	chown "$DEPLOY_USER:$DEPLOY_USER" "$ROOT/deploy.sh"
	chmod 755 "$ROOT/deploy.sh"
fi

if [ ! -f "$ROOT/.env" ]; then
	say "$ROOT/.env with new secrets (mode 600)"
	public_url="http://localhost"
	if [ "${SITE_ADDRESS:-:80}" != ":80" ]; then
		public_url="https://$SITE_ADDRESS"
	elif [ "$IN_CONTAINER" = 0 ]; then
		# The instance's public IPv4 address, from the EC2 metadata service (IMDSv2).
		token="$(curl -fsS -m 2 -X PUT http://169.254.169.254/latest/api/token \
			-H 'X-aws-ec2-metadata-token-ttl-seconds: 60' || true)"
		ip="$(curl -fsS -m 2 -H "X-aws-ec2-metadata-token: $token" \
			http://169.254.169.254/latest/meta-data/public-ipv4 || true)"
		[ -n "$ip" ] && public_url="http://$ip"
	fi
	umask 077
	cat >"$ROOT/.env" <<EOF
# Hopper production settings. Secrets were generated on this host; keep this file mode 600.
POSTGRES_PASSWORD=$(openssl rand -hex 24)
API_KEY_PEPPER=$(openssl rand -hex 32)
JWT_SECRET=$(openssl rand -hex 32)
GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 16)
SITE_ADDRESS=${SITE_ADDRESS:-:80}
PUBLIC_URL=$public_url
LOG_LEVEL=INFO
# Tenants pick queue names; these are the ones workers serve (and that get their own metric label).
WORKER_QUEUES=default
# deploy.sh adds SMOKE_API_KEY on the first deploy.
EOF
	chown "$DEPLOY_USER:$DEPLOY_USER" "$ROOT/.env"
	chmod 600 "$ROOT/.env"
elif [ -n "${SITE_ADDRESS:-}" ] && ! grep -qxF "SITE_ADDRESS=$SITE_ADDRESS" "$ROOT/.env"; then
	echo "note: $ROOT/.env keeps its own SITE_ADDRESS. To change it, edit SITE_ADDRESS and" \
		"PUBLIC_URL there, then redeploy the live release (docs/deploy.md, Day to day)." >&2
fi

if [ -n "${DEPLOY_PUBKEY:-}" ]; then
	say "CI deploy key: may run deploy.sh with one word, nothing else"
	install -d -o "$DEPLOY_USER" -g "$DEPLOY_USER" -m 700 "$home/.ssh"
	touch "$keys"
	# One CI key at a time: a new key replaces the old one's line (key rotation).
	{
		grep -vF "$forced" "$keys" || true
		printf 'restrict,%s %s\n' "$forced" "$DEPLOY_PUBKEY"
	} >"$keys.new"
	mv "$keys.new" "$keys"
	chown "$DEPLOY_USER:$DEPLOY_USER" "$keys"
	chmod 600 "$keys"
fi

say "done. Next, from docs/deploy.md: GitHub secrets and variables, then the first deploy."
docker --version
docker compose version
