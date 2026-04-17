# IRNAS Smart Parks batch onboarding

Tools for configuring IRNAS Smart Parks GPS trackers (the "OpenCollar
SmartBuoy" hardware family, IRNAS company ID `0x0A61`) over BLE without
using the web app, and for verifying that they actually joined a
LoRaWAN network on the back end.

Self-contained and open-source-friendly: no hard-coded paths, no shared
secrets in the repo. Credentials live in a `.env` file that the wizard
will help you create on first run and that's gitignored.

## Quick start

```bash
./batch-onboard.sh
```

The wrapper creates `.venv`, installs `bleak` + `questionary`, then
launches the interactive wizard. The wizard asks:

1. **ChirpStack credentials** (only on first run) — URL, application
   UUID, and API key. The wizard offers to save these to `./.env` with
   mode 0600 so you only do this once.
2. **Region** — US915, EU868, AS923, AU915, "no change", or an
   "advanced" option that exposes all 13 Semtech modem regions.
3. **Quick profile** — None, Transit, Daily, Battery Saver, Hourly,
   Frequent, or Test (mirrors the Quick Profiles in
   [configure.buoy.fish](https://configure.buoy.fish)).
4. **Force re-visit?** — whether to re-onboard devices that are already
   activated on ChirpStack or already in the local ledger. Default no.
5. **GPS home (optional)** — decimal `lat,lon` of your deployment area.
   Written to `gps_init_lat`/`gps_init_lon` to speed up first GPS fix
   on the device. Press Enter to skip. Use your **deployment** location,
   not the configuration location, when the device is being shipped to
   another region.

Every device the script touches also gets its `init_time` set to the
host's current UTC time (free fix for the stale 2021 timestamps that
factory-fresh devices ship with). Disable with `--no-set-time`.

After answering 2–4, the wizard offers to save them to
`./.last-batch.json`. **On subsequent runs**, if both `.env` and
`.last-batch.json` exist, the wizard collapses everything into a
single confirmation:

```
Last batch saved 2026-04-17T00:00:00Z:
  ChirpStack:  https://console.example.com
               app=28359e48-8409-4900-9eac-e2d5692fa8c2
               key=eyJ0eX...n3dA
  Region:      US915 (3)
  Profile:     Hourly  -- GPS every hour, motion-triggered.
  Force:       no (skip already-onboarded)

Reuse these settings? [Y/n]
```

Press **Y** to skip directly to the device scan and final confirm.
Press **N** to answer the three questions fresh (and optionally re-save).

It then scans, shows you how many devices it found, asks one final
confirmation, and processes each device serially:

- reads the current region/profile settings,
- writes only the values that differ,
- always issues `cmd_reset` (0xA1) so the radio re-initializes from
  flash and a fresh, network-visible JoinRequest is transmitted,
- records the action in `onboarded.json` with a timestamp.

After the last device, it waits 10 minutes (configurable with
`--verify-wait-min N`), queries ChirpStack via the activation endpoint
once per device, and reports exactly which devices joined this run vs
which were already joined vs which never joined.

## Pre-flight skip

At the start of every run the script captures a snapshot of every
device's current activation state on ChirpStack. When BLE scanning
discovers a device, the script reads its DevEUI and decides what to do:

| ChirpStack state | Local ledger state | Action |
|---|---|---|
| Activated (has devAddr) | (any) | **Skip** — don't disturb a working session |
| Not activated | (any) | **Process** — write settings + reboot |
| LNS not queried (--no-verify) | "ok" | Skip (ledger is the only signal) |
| LNS not queried (--no-verify) | not in ledger | Process |

**Crucially: ChirpStack is the source of truth.** When LNS data is
available, the local ledger is ignored entirely. A "ledger says ok"
without a corresponding ChirpStack activation just means a previous run
wrote the wrong settings (e.g., wrong region) or the device's
JoinRequest never reached the network — and we want to fix that, not
skip it. The script will print a `Note: ledger marks this device
onboarded, but ChirpStack shows no activation. Re-onboarding (LNS is
authoritative).` line when this happens.

`--force` bypasses every skip and re-processes every device.

## Resilience

- **Retry on transient BLE disconnects**: each device gets up to 2 connect
  attempts. If the first attempt fails with a `BleakError` (typically
  `disconnected` during service discovery), the script waits 2 seconds and
  retries. Only after both attempts fail does the device get marked failed.
- **Failed-device list**: at the end of the run, every device that failed
  is listed by SP# and BLE address so you know exactly what to retry.
- **Idempotent re-runs**: failed devices are NOT added to the ledger, so
  simply re-running the same command will retry them automatically. Already-
  onboarded devices and ChirpStack-active devices are still skipped.

## Why does it always reboot the devices it does touch?

The IRNAS firmware tracks LoRaWAN session state in RAM. After a fresh
factory boot the device may report `lr_joined = true` even though the
network has never seen a JoinRequest from it. In that state, `cmd_join`
(0xA0) is silently dropped — only `cmd_reset` (0xA1) wipes the stale
RAM session and forces a clean OTAA handshake on the persisted
`lr_region`.

This was learned the hard way during the SP100207–SP100336 batch in
April 2026, where the original code path used `cmd_join` and ended up
with 5/120 devices actually joined. See
[`.cursor/rules/irnas-lorawan-regions.mdc`](../../.cursor/rules/irnas-lorawan-regions.mdc)
for the full rule.

The only authoritative source of truth for "joined" is the network
server (ChirpStack `deviceActivation.devAddr`), never the device's
self-report. ChirpStack's `lastSeenAt` field is intentionally NOT used
for verification because it only updates on data uplinks, not on join
events — so a freshly-joined device shows `lastSeenAt: null` for hours
until its first scheduled status uplink.

## Credentials

The script looks for ChirpStack credentials in this order:

1. CLI flags (`--lns-url`, `--lns-api-key`, `--lns-app-id`)
2. The `.env` file next to the script (`device/irnas/.env`)
3. A different env file pointed to by `--lns-env /path/to/.env`
4. Process environment variables (`CHIRPSTACK_BASE_URL`,
   `CHIRPSTACK_API_KEY`, `CHIRPSTACK_APPLICATION_ID`)
5. Interactive prompts via the wizard (and an offer to save to `.env`)

Either:

- Run the wizard once and answer the prompts; it will offer to save to
  `./.env`.
- Or copy [`.env.example`](./.env.example) to `.env` and fill in the
  three values.

`.env` is gitignored. The wizard writes it with mode 0600.

## Non-interactive use

All wizard answers can be supplied as flags. The wizard is skipped
when any non-trivial flag is present.

```bash
# Diagnostic: list all IRNAS adverts in range, no connects
./batch-onboard.sh --list

# Target specific SP numbers
./batch-onboard.sh --sp SP100219 --region US915 --profile transit

# Single device by direct address (when SP# isn't advertised)
./batch-onboard.sh --by-address 22EBCF88-3C86-72DA-DE0B-48BD2443E654 \
  --region US915 --profile transit --force

# Skip ChirpStack verification entirely
./batch-onboard.sh --sp SP100219 --region US915 --no-verify

# Use a different LNS or app
./batch-onboard.sh --sp SP100219 --region US915 \
  --lns-url https://console.example.com \
  --lns-api-key "..." \
  --lns-app-id 28359e48-...
```

### Useful flags

| Flag | Purpose |
|---|---|
| `--region N` | Semtech region enum (1–13) or name like `US915`. If omitted, the device's current region is preserved. |
| `--profile id` | One of `transit`, `daily`, `battery_saver`, `hourly`, `frequent`, `test`. |
| `--force` | Bypass BOTH the LNS-active skip and the ledger skip. Re-onboard devices that are already joined or already in the ledger. |
| `--dry-run` | Connect, read DevEUI + region, but do NOT write or reboot. |
| `--scan-seconds N` | Length of each BLE scan window (default 10s). Raise for dense deployments. |
| `--idle-windows N` | Exit after N consecutive empty scan windows (default 3). |
| `--reboot` / `--rejoin` / `--no-rejoin` | Override the post-action. Default is `--reboot`. `--rejoin` is diagnostic-only and almost never works (the firmware silently drops cmd_join). |
| `--per-device-timeout N` | Hard cap on per-device BLE work in seconds (default 45). Raise for slow devices or congested BLE environments. |
| `--no-set-time` | Don't write the device's `init_time`. Default is to set it to host UTC on every visit. |
| `--home-lat DEG`, `--home-lon DEG` | Set `gps_init_lat`/`gps_init_lon` (deployment area, not config location) for faster first GPS fix. Both must be supplied. |
| `--report` | Read-only audit mode. See "Report mode" below. |
| `--verify-wait-min N` | Minutes to wait before checking ChirpStack (default 10). |
| `--no-verify` | Skip the ChirpStack post-flight check (and the pre-flight snapshot). |
| `--lns-env PATH` | Use credentials from a different `.env` file. |

## Ledger

`onboarded.json` (next to the script, gitignored) is keyed by DevEUI:

```json
{
  "version": 1,
  "devices": {
    "0016C001F0000000": {
      "ble_address": "00000000-0000-0000-0000-000000000000",
      "ble_name": "SP100000",
      "region_before": 1,
      "region_after": 3,
      "profile_applied": "transit",
      "settings_written": ["lr_gps_interval", "ublox_send_interval", "..."],
      "post_action": "reboot",
      "reset_at": "2026-04-17T00:00:00.000Z",
      "timestamp": "2026-04-17T00:00:00.000Z",
      "status": "ok"
    }
  }
}
```

By default the script skips devices already in the ledger (in addition
to skipping LNS-active devices). `--force` bypasses both.

## Locate mode (force fresh GPS pins)

```bash
./batch-locate.sh
```

Walks every IRNAS device in BLE range and tells each one to acquire a
fresh u-blox GPS fix and uplink it over LoRaWAN
(`cmd_get_ublox_fix` / `0xB8`).

**Read-only on settings.** Locate mode does NOT modify any persisted
setting on the device — not region, profile, `init_time`, `gps_init_lat`,
or anything else. It only sends commands (events, not writes) and reads
the post-connect `last_position` notification. Safe to run against a
fully-configured fleet without disturbing sessions or changing config.

Useful when you want pins on a map _now_ instead of waiting for the
device's natural `status_send_interval` (which is hours on most profiles).

After the BLE loop, the script waits a few minutes (default 3, controlled
with `--locate-wait-min`) and queries ChirpStack to confirm new uplinks
landed by comparing each device's `fCntUp` before vs after. Output:

```
=== Locate verification (3 min after last cmd_get_ublox_fix) ===
  New uplinks observed:  3/4
  Silent (no new uplink): 1/4

  Uplinked since locate (likely have fresh pins on the map):
    SP100272   0016C001F09C7576  +1 uplink(s)
    ...
  No new uplinks (GPS may not have fixed; check coverage):
    SP100334   0016C001F09C7659
```

**Does NOT write region/profile/home/force-reboot anything** — safe to run
against a fully-configured fleet without disturbing sessions.

Useful flags:

| Flag | Purpose |
|---|---|
| `--sp SP#` | Locate only specific devices (filter by advertised SP#). |
| `--dev-eui HEX` | Locate only specific DevEUIs. |
| `--also-send-status` | Also send `cmd_send_status_lr` (0xAD) for an immediate status uplink — useful when GPS is questionable but you want a "device is alive" pin. |
| `--locate-wait-min N` | Minutes to wait before checking ChirpStack (default 3). Set to 0 to skip verification. |
| `--no-verify` | Skip the ChirpStack pre/post snapshot entirely. |

Behind the scenes this is just `./batch-onboard.sh --locate "$@"`.

## Report mode (read-only QA snapshot)

```bash
./batch-onboard.sh --report
```

Walks every IRNAS device in BLE range, reads its full settings dump,
queries ChirpStack for activation status, and writes:

- One Markdown file per device under `./reports/{SP#}_{DevEUI}_{ts}.md`
  — human-readable, includes LoRaWAN config, GPS profile, last-known
  position, and a complete table of every setting on the device.
- One combined JSONL file at `./reports/device-reports-{ts}.jsonl`
  — one line per device, machine-readable for grep/diff/import.

**Does NOT write or reboot anything.** Safe to run before deployment,
during QA acceptance, or any time you want a baseline snapshot to diff
against later.

The report uses an optional setting-name schema if it can find one
(searches a few candidate paths next to the script, including the
`prod/configure-buoy-fish/settings/settings_v7.0.0.json` from the
buoy-fish-tech monorepo). Without a schema, the report still works
but shows `unknown_0xXX` for unrecognized setting IDs. To get fully
decoded reports in a standalone install, copy
[settings_v7.0.0.json](https://github.com/SmartParksOrg/ble-settings-app)
next to the script.

Sensitive settings (`app_key`, `lp0_app_key`, etc.) are always
redacted in both the Markdown and JSONL output.

## Standalone status check

To verify the network state of every device in the ledger without
doing anything destructive:

```bash
source .venv/bin/activate
python3 check-onboarded-status.py                # full report
python3 check-onboarded-status.py --only-silent  # devices that never joined
python3 check-onboarded-status.py --only-joined  # devices that joined
```

Reads credentials from `./.env` and queries each device's activation
endpoint in parallel.

## Platform notes

- **macOS**: First run will prompt for Bluetooth permission. Grant it
  in System Settings → Privacy & Security → Bluetooth for whatever
  terminal you're running from (Terminal, iTerm, Cursor's integrated
  terminal, etc.), then re-run.
- **MAC address filtering is NOT possible on macOS.** CoreBluetooth
  hides real BT addresses and hands `bleak` an opaque per-host UUID
  instead. Use `--sp` (advertised name) for filtering on macOS.

## Files

| File | What it does |
|---|---|
| [`batch-onboard.sh`](./batch-onboard.sh) | Bash wrapper: env check, venv, deps, exec the Python script. |
| [`batch-locate.sh`](./batch-locate.sh) | Wrapper that runs the Python script in `--locate` mode. |
| [`batch-onboard-us915.py`](./batch-onboard-us915.py) | Wizard, BLE main loop, pre-flight skip, post-flight verification, locate, report. |
| [`check-onboarded-status.py`](./check-onboarded-status.py) | Standalone activation-based ChirpStack query. |
| [`requirements.txt`](./requirements.txt) | `bleak`, `questionary`. |
| [`.env.example`](./.env.example) | Template for credentials. Copy to `.env`. |
| `.env` | Your local credentials (gitignored, mode 0600). |
| `.last-batch.json` | Saved wizard answers (gitignored). Lets the next run skip Q&A with one Y/n. |
| `onboarded.json` | Ledger of processed devices (gitignored, created on first run). |

## License

MIT — see [LICENSE](./LICENSE).

## Source-of-truth references

- BLE protocol & setting IDs: [`prod/configure-buoy-fish/`](../../prod/configure-buoy-fish/)
- Quick profile definitions: [`prod/configure-buoy-fish/index.html`](../../prod/configure-buoy-fish/index.html) (`UPLINK_PROFILES`)
- Region enum & joining gotcha: [`.cursor/rules/irnas-lorawan-regions.mdc`](../../.cursor/rules/irnas-lorawan-regions.mdc)
