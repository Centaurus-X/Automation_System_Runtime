#!/usr/bin/env bash
#
# Worker-Systemdienst verwalten
# =============================
#
# Empfohlene Installation:
#
#   nano worker_service_manager.sh
#   chmod 700 worker_service_manager.sh
#   ./worker_service_manager.sh --install
#   ./worker_service_manager.sh --enable-now
#
# Wenn der Worker bereits durch rollout_automation gestartet wurde, übernimmt
# --enable-now ihn kontrolliert: alter Prozess wird sauber beendet und danach
# unter systemd neu gestartet.
#
# Optionen:
#
#   --help          Diese Hilfe anzeigen
#   --install       systemd-Dienst erstellen/aktualisieren, nicht starten
#   --enable        Autostart aktivieren, Worker nicht starten
#   --enable-now    Dienst installieren, Autostart aktivieren und Worker übernehmen
#   --disable       Autostart deaktivieren, laufenden Worker nicht stoppen
#   --disable-now   Autostart deaktivieren und Worker stoppen
#   --start         Installierten Dienst starten
#   --stop          systemd- und rollout-gestarteten Worker sauber stoppen
#   --restart       Worker stoppen und unter systemd neu starten
#   --status        Dienststatus und tatsächlich erkannte Worker-PIDs anzeigen
#   --logs          Worker-Log fortlaufend anzeigen; Abbruch mit Strg+C
#   --delete        Dienst stoppen, deaktivieren und aus systemd löschen
#
# Konfiguration kann über Umgebungsvariablen geändert werden:
#
#   SERVICE_USER=philo
#   SERVICE_NAME=automation-worker-fn-01.service
#   WORKER_DIR=/home/philo/python-skripte_22/__NEW_FILES__/automation_system_v0101
#   AUTOMATION_PROFILE=worker_simulation
#
# Beispiel:
#
#   SERVICE_USER=philo ./worker_service_manager.sh --status
#
# WICHTIG:
# - Das Skript verwaltet nur den Worker site-main/fn-01.
# - Der Root-Prozess "[fake ethernet]" wird wegen Benutzer- und Pfadprüfung
#   nicht beendet.
# - --delete entfernt NICHT das Worker-Paket oder dessen Konfiguration.
#

set -Eeuo pipefail
umask 077

ACTION="${1:---help}"

SERVICE_USER="${SERVICE_USER:-philo}"
SERVICE_NAME="${SERVICE_NAME:-automation-worker-fn-01.service}"
SERVICE_HOME="${SERVICE_HOME:-/home/$SERVICE_USER}"
AUTOMATION_PROFILE="${AUTOMATION_PROFILE:-worker_simulation}"

WORKER_DIR="${WORKER_DIR:-$SERVICE_HOME/python-skripte_22/__NEW_FILES__/automation_system_v0101}"
STATE_DIR="${STATE_DIR:-$SERVICE_HOME/.local/state/automation-worker/site-main/fn-01}"
WORKER_LOG="${WORKER_LOG:-$STATE_DIR/worker.log}"
PID_FILE="${PID_FILE:-$STATE_DIR/worker.pid}"

UNIT_FILE="/etc/systemd/system/$SERVICE_NAME"
TEMP_UNIT=""

cleanup() {
    if [[ -n "$TEMP_UNIT" && -e "$TEMP_UNIT" ]]; then
        rm -f -- "$TEMP_UNIT"
    fi
}
trap cleanup EXIT

die() {
    echo "FEHLER: $*" >&2
    exit 1
}

usage() {
    sed -n '2,/^$/p' "$0"
    cat <<'EOF'

Kurzbefehle:

  ./worker_service_manager.sh --install
  ./worker_service_manager.sh --enable
  ./worker_service_manager.sh --enable-now
  ./worker_service_manager.sh --disable
  ./worker_service_manager.sh --disable-now
  ./worker_service_manager.sh --start
  ./worker_service_manager.sh --stop
  ./worker_service_manager.sh --restart
  ./worker_service_manager.sh --status
  ./worker_service_manager.sh --logs
  ./worker_service_manager.sh --delete
EOF
}

require_command() {
    command -v "$1" >/dev/null 2>&1 ||
        die "Benötigtes Kommando fehlt: $1"
}

require_base_tools() {
    require_command systemctl
    require_command sudo
    require_command pgrep
    require_command readlink
    require_command install
    require_command id
}

validate_configuration() {
    id "$SERVICE_USER" >/dev/null 2>&1 ||
        die "Benutzer existiert nicht: $SERVICE_USER"

    [[ "$SERVICE_NAME" =~ ^[A-Za-z0-9_.@-]+[.]service$ ]] ||
        die "Unsicherer SERVICE_NAME: $SERVICE_NAME"

    SERVICE_HOME="$(readlink -f -- "$SERVICE_HOME")"
    WORKER_DIR="$(readlink -m -- "$WORKER_DIR")"
    STATE_DIR="$(readlink -m -- "$STATE_DIR")"
    WORKER_LOG="$(readlink -m -- "$WORKER_LOG")"
    PID_FILE="$(readlink -m -- "$PID_FILE")"

    for path in "$WORKER_DIR" "$STATE_DIR" "$WORKER_LOG" "$PID_FILE"; do
        [[ "$path" == "$SERVICE_HOME"/* ]] ||
            die "Pfad liegt außerhalb von $SERVICE_HOME: $path"
        [[ "$path" != *$'\n'* && "$path" != *$'\r'* ]] ||
            die "Ungültiger Pfad"
        [[ "$path" != *[[:space:]]* ]] ||
            die "Pfade mit Leerzeichen werden aus Sicherheitsgründen abgewiesen: $path"
    done
}

require_worker_package() {
    [[ -d "$WORKER_DIR" ]] ||
        die "Worker-Verzeichnis fehlt: $WORKER_DIR"

    [[ -x "$WORKER_DIR/run_worker.sh" ]] ||
        die "run_worker.sh fehlt oder ist nicht ausführbar: $WORKER_DIR/run_worker.sh"
}

worker_pids() {
    local uid pid command_line process_cwd
    uid="$(id -u "$SERVICE_USER")"

    while IFS= read -r pid; do
        [[ "$pid" =~ ^[1-9][0-9]*$ && -d "/proc/$pid" ]] || continue

        command_line="$(
            tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true
        )"
        [[ "$command_line" == *"src.sync_xserver_main"* ]] || continue

        process_cwd="$(
            readlink -f "/proc/$pid/cwd" 2>/dev/null || true
        )"
        [[ "$process_cwd" == "$WORKER_DIR" ]] || continue

        printf '%s\n' "$pid"
    done < <(
        pgrep -u "$uid" -f -- 'src[.]sync_xserver_main' 2>/dev/null || true
    )
}

unit_installed() {
    [[ -f "$UNIT_FILE" ]]
}

install_unit() {
    local service_group
    require_worker_package

    service_group="$(id -gn "$SERVICE_USER")"
    install -d -m 0700 "$STATE_DIR"

    TEMP_UNIT="$(mktemp)"

    cat >"$TEMP_UNIT" <<EOF
[Unit]
Description=Automation Worker site-main/fn-01
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$service_group
WorkingDirectory=$WORKER_DIR
Environment=HOME=$SERVICE_HOME
Environment=AUTOMATION_PROFILE=$AUTOMATION_PROFILE
UMask=0077

ExecStartPre=/usr/bin/install -d -m 0700 $STATE_DIR
ExecStartPre=/usr/bin/env AUTOMATION_PROFILE=$AUTOMATION_PROFILE $WORKER_DIR/run_worker.sh --preflight
ExecStart=/usr/bin/env AUTOMATION_PROFILE=$AUTOMATION_PROFILE $WORKER_DIR/run_worker.sh

StandardOutput=append:$WORKER_LOG
StandardError=append:$WORKER_LOG

KillSignal=SIGTERM
KillMode=control-group
TimeoutStartSec=60
TimeoutStopSec=35
Restart=on-failure
RestartSec=5
SuccessExitStatus=0 143 SIGTERM

[Install]
WantedBy=multi-user.target
EOF

    sudo install -o root -g root -m 0644 "$TEMP_UNIT" "$UNIT_FILE"
    rm -f -- "$TEMP_UNIT"
    TEMP_UNIT=""

    sudo systemctl daemon-reload
    echo "Dienst installiert: $UNIT_FILE"
}

stop_all_worker_processes() {
    local pids=()
    local remaining=()
    local pid

    if unit_installed; then
        sudo systemctl stop "$SERVICE_NAME" || true
    fi

    mapfile -t pids < <(worker_pids)

    if ((${#pids[@]} == 0)); then
        rm -f -- "$PID_FILE"
        echo "Worker ist bereits gestoppt."
        return 0
    fi

    printf 'Stoppe Worker-PIDs: %s\n' "${pids[*]}"
    sudo kill -TERM -- "${pids[@]}"

    for _ in $(seq 1 30); do
        remaining=()

        for pid in "${pids[@]}"; do
            if [[ -d "/proc/$pid" ]]; then
                while IFS= read -r active_pid; do
                    [[ "$active_pid" == "$pid" ]] &&
                        remaining+=("$pid")
                done < <(worker_pids)
            fi
        done

        ((${#remaining[@]} == 0)) && break
        sleep 1
    done

    mapfile -t remaining < <(worker_pids)

    if ((${#remaining[@]} != 0)); then
        die "Worker reagierte nicht auf SIGTERM: ${remaining[*]}"
    fi

    rm -f -- "$PID_FILE"
    echo "Worker wurde sauber gestoppt."
}

start_service() {
    local pids=()

    unit_installed ||
        die "Dienst ist nicht installiert. Zuerst --install verwenden."

    require_worker_package

    if sudo systemctl is-active --quiet "$SERVICE_NAME"; then
        echo "Dienst läuft bereits."
        return 0
    fi

    mapfile -t pids < <(worker_pids)

    if ((${#pids[@]} != 0)); then
        echo "Worker läuft bereits außerhalb von systemd: ${pids[*]}" >&2
        echo "Zur kontrollierten Übernahme bitte --restart verwenden." >&2
        exit 2
    fi

    sudo systemctl start "$SERVICE_NAME"
    sleep 3

    sudo systemctl is-active --quiet "$SERVICE_NAME" ||
        die "Dienst konnte nicht gestartet werden."

    echo "Worker-Dienst wurde gestartet."
}

show_status() {
    local pids=()

    echo "=== systemd ==="

    if unit_installed; then
        sudo systemctl status "$SERVICE_NAME" --no-pager --full || true
        echo
        sudo systemctl is-enabled "$SERVICE_NAME" 2>/dev/null || true
    else
        echo "Dienst ist nicht installiert."
    fi

    echo
    echo "=== Tatsächlich erkannte Worker-Prozesse ==="

    mapfile -t pids < <(worker_pids)

    if ((${#pids[@]} == 0)); then
        echo "Worker: STOPPED"
    else
        printf 'Worker: RUNNING; PIDs=%s\n' "${pids[*]}"
        ps -o pid,ppid,user,stat,lstart,args -p "$(IFS=,; echo "${pids[*]}")"
    fi

    echo
    echo "Worker-Verzeichnis: $WORKER_DIR"
    echo "Worker-Log:         $WORKER_LOG"
    echo "systemd-Unit:       $UNIT_FILE"
}

delete_service() {
    stop_all_worker_processes

    if unit_installed; then
        sudo systemctl disable "$SERVICE_NAME" 2>/dev/null || true
        sudo rm -f -- "$UNIT_FILE"
        sudo systemctl daemon-reload
        sudo systemctl reset-failed "$SERVICE_NAME" 2>/dev/null || true
        echo "systemd-Dienst wurde gelöscht."
    else
        echo "systemd-Dienst war nicht installiert."
    fi

    echo "Worker-Paket, Konfiguration, Zertifikate und Logs wurden NICHT gelöscht."
}

require_base_tools
validate_configuration

case "$ACTION" in
    --help|-h)
        usage
        ;;

    --install)
        install_unit
        ;;

    --enable)
        install_unit
        sudo systemctl enable "$SERVICE_NAME"
        echo "Autostart wurde aktiviert; Worker wurde nicht gestartet."
        ;;

    --enable-now)
        install_unit
        sudo systemctl enable "$SERVICE_NAME"
        stop_all_worker_processes
        sudo systemctl start "$SERVICE_NAME"
        sleep 3
        sudo systemctl is-active --quiet "$SERVICE_NAME" ||
            die "Worker-Dienst konnte nicht gestartet werden."
        show_status
        ;;

    --disable)
        unit_installed ||
            die "Dienst ist nicht installiert."
        sudo systemctl disable "$SERVICE_NAME"
        echo "Autostart wurde deaktiviert; laufender Worker bleibt aktiv."
        ;;

    --disable-now)
        unit_installed ||
            die "Dienst ist nicht installiert."
        sudo systemctl disable "$SERVICE_NAME"
        stop_all_worker_processes
        ;;

    --start)
        start_service
        show_status
        ;;

    --stop)
        stop_all_worker_processes
        show_status
        ;;

    --restart)
        unit_installed ||
            die "Dienst ist nicht installiert. Zuerst --install verwenden."
        stop_all_worker_processes
        start_service
        show_status
        ;;

    --status)
        show_status
        ;;

    --logs)
        echo "Worker-Log: $WORKER_LOG"
        echo "Abbruch mit Strg+C"
        install -d -m 0700 "$STATE_DIR"
        touch "$WORKER_LOG"
        tail -n 200 -F "$WORKER_LOG"
        ;;

    --delete|--uninstall)
        delete_service
        ;;

    *)
        echo "Unbekannte Option: $ACTION" >&2
        usage >&2
        exit 64
        ;;
esac
