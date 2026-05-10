$ErrorActionPreference = 'Stop'

$LocalRoot = 'G:\ba\phoenix_work'
$SecureRoot = 'G:\ba\secure\phoenix'
$Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$AuditRoot = Join-Path $LocalRoot "audit_artifacts\environment_lockdown_$Timestamp"
New-Item -ItemType Directory -Force -Path $AuditRoot | Out-Null

$CoreFiles = @(
  [pscustomobject]@{rel='.env.example'; category='config_template'},
  [pscustomobject]@{rel='.gitignore'; category='repo_hygiene'},
  [pscustomobject]@{rel='README.md'; category='docs'},
  [pscustomobject]@{rel='requirements.txt'; category='dependency'},
  [pscustomobject]@{rel='run_testnet_live_runner.sh'; category='vps_runner_script'},
  [pscustomobject]@{rel='phoenix_config.py'; category='runtime_config'},
  [pscustomobject]@{rel='phoenix_runtime_modes.py'; category='runtime_safety'},
  [pscustomobject]@{rel='phoenix_testnet_safety.py'; category='runtime_safety'},
  [pscustomobject]@{rel='phoenix_signal_lab.py'; category='data_signal_input'},
  [pscustomobject]@{rel='phoenix_signal_bridge.py'; category='signal_bridge'},
  [pscustomobject]@{rel='phoenix_signal_bridge_modes.py'; category='signal_bridge'},
  [pscustomobject]@{rel='phoenix_testnet_round_runner.py'; category='testnet_runner'},
  [pscustomobject]@{rel='phoenix_dashboard_snapshot_api.py'; category='report_snapshot_api'},
  [pscustomobject]@{rel='phoenix_playbook_backtest.py'; category='backtest'},
  [pscustomobject]@{rel='phoenix_historical_research_replay.py'; category='backtest'},
  [pscustomobject]@{rel='phoenix_merge_backtest_reports.py'; category='backtest_report'},
  [pscustomobject]@{rel='phoenix_position_manager.py'; category='position_manager'},
  [pscustomobject]@{rel='phoenix_post_fill_worker.py'; category='execution'},
  [pscustomobject]@{rel='phoenix_executor.py'; category='execution'},
  [pscustomobject]@{rel='phoenix_live_execute.py'; category='execution'},
  [pscustomobject]@{rel='phoenix_execution_records.py'; category='execution_report'},
  [pscustomobject]@{rel='phoenix_testnet_execution_report.py'; category='execution_report'},
  [pscustomobject]@{rel='phoenix_learning_store.py'; category='learning'},
  [pscustomobject]@{rel='phoenix_learning_analyzer.py'; category='learning'},
  [pscustomobject]@{rel='phoenix_learning_gate.py'; category='learning_gate'},
  [pscustomobject]@{rel='phoenix_learning_diagnostics.py'; category='learning_report'},
  [pscustomobject]@{rel='phoenix_trade_attribution.py'; category='attribution'},
  [pscustomobject]@{rel='phoenix_loss_attribution_report.py'; category='attribution_report'},
  [pscustomobject]@{rel='phoenix_strategy_proposals.py'; category='learning_experiment'},
  [pscustomobject]@{rel='phoenix_strategy_experiments.py'; category='learning_experiment'},
  [pscustomobject]@{rel='phoenix_strategy_registry.py'; category='learning_experiment'},
  [pscustomobject]@{rel='phoenix_promotion_gate.py'; category='promotion_gate'},
  [pscustomobject]@{rel='phoenix_research_governance.py'; category='research_governance'},
  [pscustomobject]@{rel='phoenix_research_diagnostics.py'; category='research_report'},
  [pscustomobject]@{rel='phoenix_research_shadow_report.py'; category='research_report'},
  [pscustomobject]@{rel='phoenix_hmm_state_report.py'; category='complex_report'},
  [pscustomobject]@{rel='phoenix_markov_state_report.py'; category='complex_report'},
  [pscustomobject]@{rel='phoenix_monte_carlo_report.py'; category='complex_report'},
  [pscustomobject]@{rel='phoenix_feature_slice_report.py'; category='complex_report'},
  [pscustomobject]@{rel='phoenix_momentum_scalp_plus.py'; category='shadow_experiment'},
  [pscustomobject]@{rel='auto_tuner.py'; category='tuning_tool'},
  [pscustomobject]@{rel='phoenix/config.py'; category='runtime_config'},
  [pscustomobject]@{rel='phoenix/binance_futures.py'; category='exchange_client'},
  [pscustomobject]@{rel='phoenix/executor.py'; category='execution'},
  [pscustomobject]@{rel='phoenix/runtime_state.py'; category='runtime_state'},
  [pscustomobject]@{rel='phoenix/oms_state.py'; category='oms_state'},
  [pscustomobject]@{rel='phoenix/signal_lab_events.py'; category='signal_model'},
  [pscustomobject]@{rel='phoenix/judge.py'; category='candidate_scoring'},
  [pscustomobject]@{rel='phoenix/models.py'; category='domain_model'}
)

function Mask-Value([string]$Key, [string]$Value) {
  $v = if ($null -eq $Value) { '' } else { $Value.Trim().Trim('"').Trim("'") }
  if ($Key -match '(SECRET|KEY|TOKEN|PASS|PASSWORD|PRIVATE|API)' -or $v.Length -gt 18) {
    if ($v.Length -eq 0) { return '<empty> len=0' }
    if ($v.Length -ge 8) { return ($v.Substring(0,4) + '...' + $v.Substring($v.Length-4) + " len=$($v.Length)") }
    return "*** len=$($v.Length)"
  }
  return $v
}

function Get-FileManifest($Root, $Item, $Machine) {
  $path = Join-Path $Root ($Item.rel -replace '/', '\')
  if (Test-Path -LiteralPath $path -PathType Leaf) {
    $file = Get-Item -LiteralPath $path
    $sha = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant()
    return [pscustomobject]@{ machine=$Machine; root=$Root; rel=$Item.rel; category=$Item.category; exists=$true; size=$file.Length; mtime=$file.LastWriteTime.ToString('s'); sha256=$sha }
  }
  [pscustomobject]@{ machine=$Machine; root=$Root; rel=$Item.rel; category=$Item.category; exists=$false; size=$null; mtime=$null; sha256=$null }
}

function Parse-EnvFileMasked($Path, $Role, $Machine) {
  $rows = @()
  if (!(Test-Path -LiteralPath $Path -PathType Leaf)) {
    return [pscustomobject]@{ machine=$Machine; path=$Path; role=$Role; exists=$false; mode=$null; mtime=$null; size=$null; values=@() }
  }
  $file = Get-Item -LiteralPath $Path
  foreach ($raw in Get-Content -LiteralPath $Path) {
    $line = $raw.Trim()
    if (!$line -or $line.StartsWith('#') -or !$line.Contains('=')) { continue }
    if ($line.StartsWith('export ')) { $line = $line.Substring(7).TrimStart() }
    $parts = $line.Split('=',2)
    $key = $parts[0].Trim()
    $value = if ($parts.Count -gt 1) { $parts[1] } else { '' }
    if (!$key) { continue }
    $rows += [pscustomobject]@{ name=$key; value_masked=(Mask-Value $key $value); is_sensitive=($key -match '(SECRET|KEY|TOKEN|PASS|PASSWORD|PRIVATE|API)') }
  }
  [pscustomobject]@{ machine=$Machine; path=$Path; role=$Role; exists=$true; mode='local'; mtime=$file.LastWriteTime.ToString('s'); size=$file.Length; values=$rows }
}

function Get-ShallowPathInfo($Path, $Role, $Machine, $StatusTags, $EnvironmentType, $RuntimeMode, $SourceNote) {
  if (!(Test-Path -LiteralPath $Path)) { return $null }
  $item = Get-Item -LiteralPath $Path
  $files = @(Get-ChildItem -LiteralPath $Path -File -Recurse -Force -ErrorAction SilentlyContinue | Select-Object -First 5000)
  $latest = $item.LastWriteTime
  foreach ($file in $files) {
    if ($file.LastWriteTime -gt $latest) { $latest = $file.LastWriteTime }
  }
  [pscustomobject]@{
    machine=$Machine
    path=$Path
    kind=if($item.PSIsContainer){'directory'}else{'file'}
    role=$Role
    status_tags=$StatusTags
    environment_type=$EnvironmentType
    runtime_mode=$RuntimeMode
    generated_by_process=$null
    generating_command=$null
    input_data_source=$null
    current_running_version_generated=$false
    mtime=$item.LastWriteTime.ToString('s')
    latest_nested_mtime=$latest.ToString('s')
    size=$item.Length
    file_count=$files.Count
    source_note=$SourceNote
  }
}

$LocalFileRows = @($CoreFiles | ForEach-Object { Get-FileManifest $LocalRoot $_ 'local' })

$SshVars = @{}
Get-Content -LiteralPath (Join-Path $SecureRoot 'vps_secrets.local.env') | ForEach-Object {
  if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
  if ($_ -match '^\s*([^=]+)=(.*)$') { $SshVars[$matches[1].Trim()] = $matches[2].Trim().Trim('"').Trim("'") }
}

$CoreJson = ($CoreFiles | ConvertTo-Json -Compress)
$CoreB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($CoreJson))

$RemoteScript = @"
import base64, datetime, hashlib, json, os, pathlib, re, subprocess, sys

ROOT = pathlib.Path('/opt/phoenix-testnet')
CORE = json.loads(base64.b64decode('$CoreB64').decode('utf-8'))
SECRET_RE = re.compile(r'(SECRET|KEY|TOKEN|PASS|PASSWORD|PRIVATE|API)', re.I)
SELECTED_ENV_KEYS = [
    'PHOENIX_RUNTIME_MODE', 'PHOENIX_BINANCE_ENV', 'PHOENIX_BINANCE_ACCOUNT_API',
    'PHOENIX_MAINNET_LIVE_ENABLED', 'PHOENIX_LIVE_TRADING_ENABLED',
    'PHOENIX_ENABLE_MAINNET_LIVE', 'PHOENIX_PROMOTION_ALLOWED',
    'PHOENIX_RESEARCH_AGENT_ENABLED', 'PHOENIX_HMM_REPORT_ENABLED',
    'PHOENIX_HMM_TRADING_GATE_ENABLED', 'PHOENIX_HMM_POSITION_MANAGER_ENABLED',
    'PHOENIX_LEARNING_ENABLED', 'PHOENIX_TESTNET_ORDER_VALIDATION_ENABLED',
    'PHOENIX_MARGIN_TYPE', 'PHOENIX_MAX_OPEN_POSITIONS', 'PHOENIX_EXECUTION_MODE',
]

def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def stat_iso(st):
    return datetime.datetime.fromtimestamp(st.st_mtime, datetime.timezone.utc).isoformat()

def mask(key, value):
    v = (value or '').strip().strip('"').strip("'")
    if SECRET_RE.search(key) or len(v) > 18:
        if not v:
            return '<empty> len=0'
        if len(v) >= 8:
            return f'{v[:4]}...{v[-4:]} len={len(v)}'
        return f'*** len={len(v)}'
    return v

def parse_env_file(path):
    rows = []
    p = pathlib.Path(path)
    if not p.exists():
        return rows
    for raw in p.read_text(errors='replace').splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        if line.startswith('export '):
            line = line[7:].lstrip()
        key, value = line.split('=', 1)
        key = key.strip()
        if not key:
            continue
        rows.append({'name': key, 'value_masked': mask(key, value), 'is_sensitive': bool(SECRET_RE.search(key))})
    return rows

def file_manifest(rel, category):
    path = ROOT / rel
    if not path.exists() or not path.is_file():
        return {'machine': 'vps', 'root': str(ROOT), 'rel': rel, 'category': category, 'exists': False, 'size': None, 'mtime': None, 'sha256': None}
    h = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            h.update(chunk)
    st = path.stat()
    return {'machine': 'vps', 'root': str(ROOT), 'rel': rel, 'category': category, 'exists': True, 'size': st.st_size, 'mtime': stat_iso(st), 'sha256': h.hexdigest()}

def proc_argv(pid):
    try:
        return (pathlib.Path('/proc') / str(pid) / 'cmdline').read_bytes().split(b'\0')[:-1]
    except Exception:
        return []

def arg_value(argv, *names):
    wanted = set(names)
    for idx, item in enumerate(argv):
        if item in wanted and idx + 1 < len(argv):
            return argv[idx + 1]
        for name in wanted:
            if item.startswith(name + '='):
                return item.split('=', 1)[1]
    return None

def fd_target(pid, fd):
    try:
        return os.readlink(f'/proc/{pid}/fd/{fd}')
    except Exception:
        return None

def ps_start(pid):
    try:
        return subprocess.check_output(['ps', '-p', str(pid), '-o', 'lstart='], text=True).strip()
    except Exception:
        return None

def proc_env(pid):
    rows, envmap = [], {}
    try:
        items = (pathlib.Path('/proc') / str(pid) / 'environ').read_bytes().split(b'\0')
    except Exception:
        items = []
    for item in items:
        if not item or b'=' not in item:
            continue
        key_b, value_b = item.split(b'=', 1)
        key = key_b.decode(errors='replace')
        value = value_b.decode(errors='replace')
        envmap[key] = value
        if key.startswith(('PHOENIX', 'BINANCE', 'OPENAI')) or key in ('PATH', 'VIRTUAL_ENV', 'PYTHONUNBUFFERED'):
            rows.append({'name': key, 'value_masked': mask(key, value), 'is_sensitive': bool(SECRET_RE.search(key))})
    for key in SELECTED_ENV_KEYS:
        if key not in envmap:
            rows.append({'name': key, 'value_masked': '<unset>', 'is_sensitive': False})
    return sorted(rows, key=lambda row: row['name']), envmap

def service_show(name):
    try:
        raw = subprocess.check_output([
            'systemctl', 'show', name, '--no-pager',
            '-p', 'Id', '-p', 'FragmentPath', '-p', 'LoadState', '-p', 'ActiveState',
            '-p', 'SubState', '-p', 'MainPID', '-p', 'WorkingDirectory',
            '-p', 'ExecStart', '-p', 'EnvironmentFiles', '-p', 'Environment',
            '-p', 'StandardOutput', '-p', 'StandardError'
        ], text=True, stderr=subprocess.DEVNULL)
    except Exception:
        raw = ''
    props = {}
    for line in raw.splitlines():
        if '=' in line:
            key, value = line.split('=', 1)
            props[key] = re.sub(r'([A-Za-z0-9_]*(API_KEY|API_SECRET|SECRET|TOKEN|PASSWORD|PASS|PRIVATE_KEY)[A-Za-z0-9_]*=)[^ ]+', r'\1****', value)
    try:
        unit = subprocess.check_output(['systemctl', 'cat', name, '--no-pager'], text=True, stderr=subprocess.DEVNULL)
    except Exception:
        unit = ''
    unit = re.sub(r'([A-Za-z0-9_]*(API_KEY|API_SECRET|SECRET|TOKEN|PASSWORD|PASS|PRIVATE_KEY)[A-Za-z0-9_]*=)[^ \n]+', r'\1****', unit)
    return {'name': name, 'properties': props, 'unit_text_masked': unit}

def line_count(path):
    try:
        with open(path, 'rb') as handle:
            return sum(1 for _ in handle)
    except Exception:
        return None

def shallow_info(path):
    p = pathlib.Path(path)
    if not p.exists():
        return None
    st = p.stat()
    latest, count = st.st_mtime, 0
    if p.is_dir():
        for base, dirs, files in os.walk(p):
            depth = pathlib.Path(base).relative_to(p).parts
            if len(depth) >= 2:
                dirs[:] = []
            for name in files:
                count += 1
                try:
                    latest = max(latest, (pathlib.Path(base) / name).stat().st_mtime)
                except Exception:
                    pass
    return {
        'mtime': stat_iso(st),
        'latest_nested_mtime': datetime.datetime.fromtimestamp(latest, datetime.timezone.utc).isoformat(),
        'file_count': count if p.is_dir() else None,
        'size': st.st_size,
        'line_count': line_count(p) if p.is_file() else None,
    }

def parse_event(path, event_name):
    p = pathlib.Path(path)
    if not p.exists():
        return None
    found = None
    for line in p.read_text(errors='replace').splitlines():
        if event_name in line:
            try:
                found = json.loads(line)
            except Exception:
                pass
    return found

def build_processes():
    processes = []
    for proc_dir in pathlib.Path('/proc').iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        argv_b = proc_argv(pid)
        if not argv_b:
            continue
        argv = [item.decode(errors='replace') for item in argv_b]
        command = ' '.join(argv)
        if not re.search(r'phoenix|openclaw|agent-tool', command, re.I):
            continue
        try:
            cwd = os.readlink(f'/proc/{pid}/cwd')
        except Exception:
            cwd = None
        try:
            exe = os.readlink(f'/proc/{pid}/exe')
        except Exception:
            exe = None
        env_rows, envmap = proc_env(pid)
        runtime = arg_value(argv, '--runtime-mode') or arg_value(argv, '--execution-mode') or envmap.get('PHOENIX_RUNTIME_MODE')
        env_arg = arg_value(argv, '--env') or envmap.get('PHOENIX_BINANCE_ENV')
        output_dir = arg_value(argv, '--output-dir') or arg_value(argv, '--mainnet-shadow-dir') or arg_value(argv, '--project-root')
        script = next((item for item in argv if item.endswith('.py') or 'uvicorn' in item), None)
        kind = 'adjacent_service'
        if 'phoenix_testnet_round_runner.py' in command:
            kind = 'testnet_runner'
        elif 'phoenix_signal_bridge.py' in command:
            kind = 'mainnet_shadow_bridge' if runtime == 'MAINNET_SHADOW' else 'signal_bridge'
        elif 'phoenix_signal_lab.py' in command and 'collect' in argv:
            kind = 'research_collector'
        elif 'phoenix_dashboard_snapshot_api.py' in command:
            kind = 'dashboard_snapshot_api'
        processes.append({
            'pid': pid, 'kind': kind, 'script': script, 'cwd': cwd, 'exe': exe,
            'argv': argv, 'command': command, 'start_time': ps_start(pid),
            'runtime_mode': runtime, 'env_arg_or_runtime_env': env_arg,
            'credentials_environment': envmap.get('PHOENIX_BINANCE_ENV'),
            'output_dir': output_dir, 'snapshots_file': arg_value(argv, '--snapshots-file'),
            'stdout': fd_target(pid, 1), 'stderr': fd_target(pid, 2), 'env_masked': env_rows,
        })
    return sorted(processes, key=lambda row: row['pid'])

def result_entry(path, role, tags, env_type, runtime, process=None, input_source=None, current=False, note=None):
    p = pathlib.Path(path)
    if not p.is_absolute() and process and process.get('cwd'):
        p = pathlib.Path(process.get('cwd')) / p
        path = str(p)
    info = shallow_info(path)
    if not info:
        return None
    p = pathlib.Path(path)
    return {
        'machine': 'vps', 'path': str(path), 'kind': 'directory' if p.is_dir() else 'file',
        'role': role, 'status_tags': tags, 'environment_type': env_type,
        'runtime_mode': runtime, 'generated_by_process': process.get('pid') if process else None,
        'generating_command': process.get('command') if process else None,
        'input_data_source': input_source,
        'current_running_version_generated': bool(current),
        'source_note': note,
        **info,
    }

processes = build_processes()
entries = []
for proc in processes:
    if proc['kind'] == 'testnet_runner':
        if proc.get('output_dir'):
            entries.append(result_entry(proc['output_dir'], 'testnet_round_runner_output', ['active'], 'testnet', 'TESTNET_LIVE', proc, 'Binance futures testnet live API + runner market scan', True, 'active testnet runner output'))
        if proc.get('stdout'):
            entries.append(result_entry(proc['stdout'], 'testnet_runner_log', ['active'], 'testnet', 'TESTNET_LIVE', proc, proc.get('output_dir'), True, 'active stdout/stderr log'))
    elif proc['kind'] == 'mainnet_shadow_bridge':
        if proc.get('output_dir'):
            entries.append(result_entry(proc['output_dir'], 'mainnet_shadow_output', ['active', 'ambiguous'], 'mainnet_shadow', 'MAINNET_SHADOW', proc, proc.get('snapshots_file'), True, 'active but credentials_environment conflicts with --env prod'))
        if proc.get('stdout'):
            entries.append(result_entry(proc['stdout'], 'mainnet_shadow_log', ['active', 'ambiguous'], 'mainnet_shadow', 'MAINNET_SHADOW', proc, proc.get('snapshots_file'), True, 'active bridge log'))
    elif proc['kind'] == 'research_collector':
        if proc.get('output_dir'):
            entries.append(result_entry(proc['output_dir'], 'research_collector_output', ['active'], 'research_only', proc.get('runtime_mode') or 'collect', proc, 'Binance prod public market data streams/rest', True, 'active public-data collector output'))
        if proc.get('stdout'):
            entries.append(result_entry(proc['stdout'], 'research_collector_log', ['active'], 'research_only', proc.get('runtime_mode') or 'collect', proc, proc.get('output_dir'), True, 'active collector stdout'))
    elif proc['kind'] == 'dashboard_snapshot_api':
        if proc.get('output_dir'):
            entries.append(result_entry(proc['output_dir'], 'dashboard_project_root', ['active'], 'readonly_dashboard', None, proc, 'on-disk Phoenix reports + readonly testnet probe', True, 'dashboard project root'))
        if proc.get('stdout'):
            entries.append(result_entry(proc['stdout'], 'dashboard_snapshot_api_log', ['active'], 'readonly_dashboard', None, proc, proc.get('output_dir'), True, 'active dashboard API log'))

active_paths = {entry['path'] for entry in entries if entry}
important_paths = [
    '/opt/phoenix-testnet/signal_lab_runs/vps_forward_shadow_mainnet_active/mainnet_shadow/signal_bridge_shadow_signals.jsonl',
    '/opt/phoenix-testnet/signal_lab_runs/vps_forward_shadow_mainnet_active/mainnet_shadow/signal_bridge_shadow_outcomes.jsonl',
    '/opt/phoenix-testnet/signal_lab_runs/vps_forward_shadow_mainnet_active/mainnet_shadow/mainnet_shadow_readiness.json',
    '/opt/phoenix-testnet/signal_lab_runs/vps_forward_shadow_mainnet_active/mainnet_shadow/Total_Sample_V9_Final.json',
    '/opt/phoenix-testnet/signal_lab_runs/vps_forward_shadow_mainnet_active/mainnet_shadow/Live_Performance_Report.txt',
    '/opt/phoenix-testnet/signal_lab_replay/local_merged_public_zip_20220101_20260427_top200_plus_retries_20260429/backtest_report.json',
    '/opt/phoenix-testnet/round_runner_reports_longrun_20260501_165101/learning_store.jsonl',
]
for path in important_paths:
    if 'vps_forward_shadow_mainnet_active' in path:
        tags, env_type, runtime = ['active', 'ambiguous'], 'mainnet_shadow', 'MAINNET_SHADOW'
    elif 'signal_lab_replay' in path:
        tags, env_type, runtime = ['stale', 'ambiguous'], 'backtest', 'BACKTEST'
    else:
        tags, env_type, runtime = ['active'], 'testnet', 'TESTNET_LIVE'
    entry = result_entry(path, 'core_report_file', tags, env_type, runtime, None, None, False, 'important report/input file')
    if entry:
        entries.append(entry)

for child in ROOT.iterdir() if ROOT.exists() else []:
    name = child.name
    if child.is_dir() and (name.startswith('round_runner_reports') or name in {'logs', 'signal_lab_runs', 'signal_lab_replay', 'strategy_experiments'}):
        if str(child) in active_paths:
            continue
        env_type = 'testnet' if name.startswith('round_runner_reports') else ('mixed_results' if name in {'logs', 'signal_lab_runs', 'signal_lab_replay'} else 'experiment_config')
        entry = result_entry(str(child), 'top_level_result_or_config_dir', ['stale', 'ambiguous'], env_type, None, None, None, False, 'not tied to current process without deeper per-file lineage')
        if entry:
            entries.append(entry)

for parent in [ROOT / 'signal_lab_runs', ROOT / 'signal_lab_replay']:
    if parent.exists():
        for child in sorted([item for item in parent.iterdir() if item.is_dir()], key=lambda item: item.name):
            if str(child) in active_paths:
                continue
            env_type = 'research_only' if parent.name == 'signal_lab_runs' else 'backtest'
            tags = ['active', 'ambiguous'] if 'vps_forward_shadow_mainnet_active' in child.name else ['stale']
            entry = result_entry(str(child), f'{parent.name}_child_dir', tags, env_type, None, None, None, False, 'child result directory')
            if entry:
                entries.append(entry)

env_files = []
for path, role in [
    ('/etc/phoenix/phoenix.env', 'global_default_missing_or_legacy'),
    ('/etc/phoenix/phoenix-testnet.env', 'testnet_runner_env'),
    ('/etc/phoenix/phoenix-testnet.systemd.env', 'systemd_env_loaded_by_shadow_bridge'),
    ('/opt/phoenix-testnet/.env', 'project_local_prod_env'),
    ('/opt/phoenix-testnet/.env.example', 'template'),
    ('/opt/phoenix-testnet/.env.backup-mainnet-shadow-20260429-061330', 'backup'),
    ('/opt/phoenix-testnet/.env.backup-mainnet-shadow-20260429-061412', 'backup'),
]:
    p = pathlib.Path(path)
    env_files.append({
        'machine': 'vps', 'path': path, 'role': role, 'exists': p.exists(),
        'mode': oct(p.stat().st_mode & 0o777) if p.exists() else None,
        'mtime': stat_iso(p.stat()) if p.exists() else None,
        'size': p.stat().st_size if p.exists() else None,
        'values': parse_env_file(p) if p.exists() else [],
    })

services = [service_show(name) for name in [
    'phoenix-mainnet-shadow-bridge.service',
    'phoenix-dashboard-snapshot-api.service',
    'openclaw-trade-gateway.service',
]]

out = {
    'collected_at': now_iso(),
    'remote_root': str(ROOT),
    'file_manifest': [file_manifest(item['rel'], item.get('category')) for item in CORE],
    'processes': processes,
    'env_files': env_files,
    'services': services,
    'result_entries': [entry for entry in entries if entry],
    'startup_events': {
        'mainnet_shadow_started_latest': parse_event('/opt/phoenix-testnet/logs/mainnet_shadow_bridge_active.log', 'signal_bridge_started'),
        'dashboard_started_latest': parse_event('/opt/phoenix-testnet/logs/dashboard_snapshot_api_service.log', 'phoenix_dashboard_snapshot_api_started'),
    },
}
print(json.dumps(out, ensure_ascii=False))
"@

$SshBase = @(
  '-i', $SshVars['VPS_SSH_PRIVATE_KEY_PATH'],
  '-p', $SshVars['VPS_PORT'],
  '-o', 'BatchMode=yes',
  '-o', 'ConnectTimeout=12',
  '-o', 'StrictHostKeyChecking=no',
  '-o', 'UserKnownHostsFile=NUL',
  "$($SshVars['VPS_USER'])@$($SshVars['VPS_HOST'])"
)
$RemoteRaw = $RemoteScript | & ssh @SshBase 'python3 -'
$RemoteJson = ($RemoteRaw | Select-String -Pattern '^\{' | Select-Object -Last 1).Line
if (!$RemoteJson) { throw 'Remote JSON collection failed.' }
$Remote = $RemoteJson | ConvertFrom-Json

$LocalEnvFiles = @(
  Parse-EnvFileMasked (Join-Path $LocalRoot '.env.example') 'template' 'local',
  Parse-EnvFileMasked (Join-Path $SecureRoot '.env') 'local_testnet_secret_source' 'local',
  Parse-EnvFileMasked (Join-Path $SecureRoot '.env.backup-20260424-025725') 'local_prod_backup' 'local',
  Parse-EnvFileMasked (Join-Path $SecureRoot 'phoenix-dashboard.env') 'dashboard_local_env' 'local',
  Parse-EnvFileMasked (Join-Path $SecureRoot 'vps_secrets.local.env') 'vps_connection_metadata_masked' 'local'
)

$LocalProcesses = @(Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match 'phoenix|binance|round_runner|signal_bridge|dashboard_snapshot' } |
  Where-Object { $_.CommandLine -notmatch 'Get-CimInstance Win32_Process' } |
  ForEach-Object {
    [pscustomobject]@{
      pid=$_.ProcessId; kind='local_process_candidate'; script=$null; cwd=$null
      exe=$_.ExecutablePath; command=$_.CommandLine; start_time=$null
      runtime_mode=$null; env_arg_or_runtime_env=$null; credentials_environment=$null
      output_dir=$null; stdout=$null; stderr=$null; env_masked=@()
    }
  })

$RemoteFileRows = @($Remote.file_manifest)
$RemoteMap = @{}
foreach ($row in $RemoteFileRows) { $RemoteMap[$row.rel] = $row }
$LocalMap = @{}
foreach ($row in $LocalFileRows) { $LocalMap[$row.rel] = $row }

$FileCompare = @()
foreach ($core in $CoreFiles) {
  $local = $LocalMap[$core.rel]
  $vps = $RemoteMap[$core.rel]
  $status = if ((-not $local.exists) -and (-not $vps.exists)) {
    'missing_both'
  } elseif (-not $local.exists) {
    'only_vps'
  } elseif (-not $vps.exists) {
    'only_local'
  } elseif ($local.sha256 -eq $vps.sha256) {
    'same'
  } else {
    'changed'
  }
  $FileCompare += [pscustomobject]@{
    rel=$core.rel; category=$core.category; status=$status
    local_exists=$local.exists; local_size=$local.size; local_mtime=$local.mtime; local_sha256=$local.sha256
    vps_exists=$vps.exists; vps_size=$vps.size; vps_mtime=$vps.mtime; vps_sha256=$vps.sha256
  }
}

$LocalResultEntries = @()
foreach ($pattern in @('round_runner_reports*', 'logs', 'signal_lab_runs', 'signal_lab_replay', 'strategy_experiments', 'phoenix_reports')) {
  foreach ($item in Get-ChildItem -LiteralPath $LocalRoot -Force -ErrorAction SilentlyContinue | Where-Object { $_.Name -like $pattern }) {
    $role = if ($item.Name -like 'round_runner_reports*') {
      'local_testnet_or_backtest_result_dir'
    } elseif ($item.Name -eq 'signal_lab_replay') {
      'local_backtest_replay_root'
    } elseif ($item.Name -eq 'signal_lab_runs') {
      'local_research_runs_root'
    } elseif ($item.Name -eq 'logs') {
      'local_logs_root'
    } elseif ($item.Name -eq 'strategy_experiments') {
      'local_experiment_config_dir'
    } else {
      'local_report_or_code_dir'
    }
    $entry = Get-ShallowPathInfo $item.FullName $role 'local' @('stale','ambiguous') 'local_candidate_or_unknown' $null 'local mirror/candidate result; no active local Phoenix process tied to it'
    if ($entry) { $LocalResultEntries += $entry }
  }
}

$ResultEntries = @($Remote.result_entries + $LocalResultEntries)
$DirectAnalysis = @()
$NotDirectAnalysis = @()
foreach ($entry in $ResultEntries) {
  $tags = @($entry.status_tags)
  $reasons = @()
  if ($tags -contains 'ambiguous') { $reasons += 'source/environment/generating process ambiguity' }
  if ($tags -contains 'stale') { $reasons += 'not current active output' }
  if ($entry.environment_type -eq 'research_only') { $reasons += 'research-only; not direct PnL attribution' }
  if ($entry.environment_type -eq 'readonly_dashboard') { $reasons += 'dashboard aggregation; not primary PnL source' }
  if ($entry.environment_type -eq 'backtest') { $reasons += 'backtest; execution/cost assumptions require Auditor review' }
  if (($tags -contains 'active') -and -not ($tags -contains 'ambiguous') -and $entry.environment_type -eq 'testnet' -and $entry.current_running_version_generated) {
    $DirectAnalysis += [pscustomobject]@{ path=$entry.path; role=$entry.role; environment_type=$entry.environment_type; runtime_mode=$entry.runtime_mode; why='active VPS testnet runner output, current PID-linked command/env/log lineage clear' }
  } else {
    $reasonText = if ($reasons.Count) { $reasons -join '; ' } else { 'not a current primary PnL result' }
    $NotDirectAnalysis += [pscustomobject]@{ path=$entry.path; role=$entry.role; environment_type=$entry.environment_type; runtime_mode=$entry.runtime_mode; reason=$reasonText }
  }
}

$SourceOfTruth = [pscustomobject]@{
  judgement='No single unrestricted source of truth. VPS active process manifests are the current operational source of truth; local is a candidate/mirror only; result analysis must use per-result lineage tags.'
  current_operational_source='VPS /opt/phoenix-testnet process + env + output manifests'
  code_comparison_source='core file hash comparison between local G:\ba\phoenix_work and VPS /opt/phoenix-testnet'
  local_limit='Local has no active Phoenix process and is not a git checkout; cannot prove it is deployed source of truth.'
  vps_limit='VPS is actual runtime but has Deployment Drift and Environment Mix-up; cannot treat every result as directly analyzable without lineage tags.'
}

$MinimalRuntimeTable = @()
foreach ($proc in @($Remote.processes | Where-Object { $_.kind -match 'testnet_runner|mainnet_shadow_bridge|research_collector|dashboard_snapshot_api' })) {
  $entryScript = if ($proc.script) { $proc.script } else { '<unknown>' }
  $rel = if ($entryScript -like '/opt/phoenix-testnet/*') {
    $entryScript.Substring('/opt/phoenix-testnet/'.Length)
  } elseif ($entryScript -match 'phoenix_.*\.py$') {
    Split-Path $entryScript -Leaf
  } else {
    $null
  }
  $localFile = if ($rel) { Join-Path $LocalRoot ($rel -replace '/', '\') } else { $null }
  $envFile = if ($proc.kind -eq 'mainnet_shadow_bridge') {
    '/etc/phoenix/phoenix-testnet.systemd.env + /opt/phoenix-testnet/.env'
  } elseif ($proc.kind -eq 'testnet_runner') {
    '/etc/phoenix/phoenix-testnet.env'
  } elseif ($proc.kind -eq 'dashboard_snapshot_api') {
    'token file + optional /etc/phoenix/phoenix-testnet.env for readonly testnet probe'
  } else {
    'command args/public data; no PHOENIX env visible'
  }
  $match = $FileCompare | Where-Object { $_.rel -eq $rel } | Select-Object -First 1
  $consistent = if ($match -and $match.status -eq 'same') { 'yes' } elseif ($match) { 'no' } else { 'unknown/not tracked' }
  $MinimalRuntimeTable += [pscustomobject]@{
    pid=$proc.pid; kind=$proc.kind; entry_script=$entryScript; env_file=$envFile
    output_dir=$proc.output_dir; log_stdout=$proc.stdout; runtime_mode=$proc.runtime_mode
    credentials_environment=$proc.credentials_environment; local_corresponding_file=$localFile
    local_vps_file_consistent=$consistent
  }
}

$EnvManifest = [pscustomobject]@{
  collected_at=(Get-Date).ToString('s')
  local_env_files=$LocalEnvFiles
  vps_env_files=$Remote.env_files
  process_envs=@($Remote.processes | Select-Object pid,kind,script,runtime_mode,env_arg_or_runtime_env,credentials_environment,env_masked)
}
$ProcessManifest = [pscustomobject]@{
  collected_at=$Remote.collected_at
  local_process_candidates=$LocalProcesses
  vps_processes=$Remote.processes
  vps_services=$Remote.services
  startup_events=$Remote.startup_events
}
$LineageManifest = [pscustomobject]@{
  collected_at=(Get-Date).ToString('s')
  source_of_truth=$SourceOfTruth
  result_entries=$ResultEntries
  direct_analysis=$DirectAnalysis
  not_direct_analysis=$NotDirectAnalysis
}

$FileCompare | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $AuditRoot 'file_hash_manifest.json') -Encoding UTF8
$FileCompare | Export-Csv -NoTypeInformation -LiteralPath (Join-Path $AuditRoot 'file_hash_manifest.csv') -Encoding UTF8
$ProcessManifest | ConvertTo-Json -Depth 80 | Set-Content -LiteralPath (Join-Path $AuditRoot 'process_manifest.json') -Encoding UTF8
$EnvManifest | ConvertTo-Json -Depth 80 | Set-Content -LiteralPath (Join-Path $AuditRoot 'env_masked_manifest.json') -Encoding UTF8
$LineageManifest | ConvertTo-Json -Depth 80 | Set-Content -LiteralPath (Join-Path $AuditRoot 'result_lineage_map.json') -Encoding UTF8
$MinimalRuntimeTable | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $AuditRoot 'minimal_runtime_crosscheck.json') -Encoding UTF8

$HashSummary = ($FileCompare | Group-Object status | ForEach-Object { "- $($_.Name): $($_.Count)" }) -join "`n"
$DriftRows = ($FileCompare | Where-Object { $_.status -ne 'same' } | Sort-Object status,rel | ForEach-Object { "| $($_.status) | $($_.rel) | $($_.category) | $($_.local_mtime) | $($_.vps_mtime) |" }) -join "`n"
$RuntimeRows = ($MinimalRuntimeTable | ForEach-Object { "| $($_.pid) | $($_.kind) | $($_.entry_script) | $($_.env_file) | $($_.output_dir) | $($_.runtime_mode) | $($_.credentials_environment) | $($_.local_vps_file_consistent) |" }) -join "`n"
$ActiveRows = ($ResultEntries | Where-Object { @($_.status_tags) -contains 'active' } | Sort-Object environment_type,path | ForEach-Object {
  $analysis = if ($DirectAnalysis.path -contains $_.path) { 'direct_candidate' } else { 'not_direct' }
  "| $($_.environment_type) / $($_.role) | $($_.path) | $((@($_.status_tags)) -join ',') | $($_.runtime_mode) | $analysis |"
}) -join "`n"
$DirectRows = ($DirectAnalysis | ForEach-Object { "| $($_.path) | $($_.role) | $($_.environment_type) | $($_.why) |" }) -join "`n"
$NotRows = ($NotDirectAnalysis | Select-Object -First 160 | ForEach-Object { "| $($_.path) | $($_.role) | $($_.environment_type) | $($_.reason) |" }) -join "`n"

$Report = @"
# Environment Lockdown + Result Lineage Audit

Generated at: $(Get-Date -Format s)
Local root: $LocalRoot
VPS root: /opt/phoenix-testnet

## Source of Truth Judgement

$($SourceOfTruth.judgement)

- Current operational source: $($SourceOfTruth.current_operational_source)
- Code comparison source: $($SourceOfTruth.code_comparison_source)
- Local limit: $($SourceOfTruth.local_limit)
- VPS limit: $($SourceOfTruth.vps_limit)

## High Risk Tags

- High Risk: Deployment Drift
- High Risk: Environment Mix-up

## File Hash Summary

$HashSummary

### Non-same Core Files

| status | rel | category | local_mtime | vps_mtime |
|---|---|---|---|---|
$DriftRows

## Minimal Runtime Reality Crosscheck

| PID | kind | entry script | env file | output dir | runtime mode | credentials environment | local/VPS file consistent |
|---|---|---|---|---|---|---|---|
$RuntimeRows

## Active Output Crosswalk

| category | active VPS path | status tags | runtime/env | analysis status |
|---|---|---|---|---|
$ActiveRows

## Directly Analyzable Result Candidates

| path | role | environment | why |
|---|---|---|---|
$DirectRows

## Not Directly Analyzable / Must Isolate

| path | role | environment | reason |
|---|---|---|---|
$NotRows

## Guardrails Observed

- No Phoenix process stopped or restarted.
- No orders placed.
- No strategy, factor, TP/SL, risk, learning, HMM, Markov, HAA, or Monte Carlo behavior changed.
- Secrets are masked as prefix/suffix/length only.
"@
$Report | Set-Content -LiteralPath (Join-Path $AuditRoot 'ENVIRONMENT_LOCKDOWN_LINEAGE_AUDIT.md') -Encoding UTF8

$CrosswalkRows = ($ResultEntries | Sort-Object environment_type,path | ForEach-Object {
  "| $($_.role) | $($_.path) | $((@($_.status_tags)) -join ',') | $($_.runtime_mode) | $($_.environment_type) | $($_.source_note) |"
}) -join "`n"
$Crosswalk = @"
# Testnet / Mainnet Shadow / Backtest Output Path Crosswalk

| category | path | status tags | runtime mode | environment type | source note |
|---|---|---|---|---|---|
$CrosswalkRows
"@
$Crosswalk | Set-Content -LiteralPath (Join-Path $AuditRoot 'output_path_crosswalk.md') -Encoding UTF8

$Readiness = @"
# Analysis Readiness

## Directly Analyzable Candidates

| path | role | environment | why |
|---|---|---|---|
$DirectRows

## Not Directly Analyzable / Isolate First

| path | role | environment | reason |
|---|---|---|---|
$NotRows

## Builder Warnings

### High Risk: Environment Mix-up

`phoenix-mainnet-shadow-bridge.service` runs `--env prod --execution-mode MAINNET_SHADOW`, but the process env contains testnet credentials/environment from `/etc/phoenix/phoenix-testnet.systemd.env`. Treat mainnet shadow reports as ambiguous until Auditor signs off.

### High Risk: Deployment Drift

Local and VPS are not identical. Runtime-focused files are mostly equal, but at least `phoenix_playbook_backtest.py` differs; VPS also has `run_testnet_live_runner.sh` and local has `phoenix_executor.py` only.
"@
$Readiness | Set-Content -LiteralPath (Join-Path $AuditRoot 'analysis_readiness.md') -Encoding UTF8

[pscustomobject]@{
  audit_root=$AuditRoot
  file_manifest_count=$FileCompare.Count
  file_status_counts=@($FileCompare | Group-Object status | ForEach-Object { [pscustomobject]@{status=$_.Name; count=$_.Count} })
  vps_process_count=@($Remote.processes).Count
  result_entry_count=@($ResultEntries).Count
  direct_analysis_count=@($DirectAnalysis).Count
  not_direct_analysis_count=@($NotDirectAnalysis).Count
} | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $AuditRoot 'audit_summary.json') -Encoding UTF8

"AUDIT_ROOT=$AuditRoot"
"COUNTS files=$($FileCompare.Count) vps_processes=$(@($Remote.processes).Count) result_entries=$($ResultEntries.Count) direct=$($DirectAnalysis.Count) not_direct=$($NotDirectAnalysis.Count)"
Get-ChildItem -LiteralPath $AuditRoot | Select-Object Name,Length,LastWriteTime | Format-Table -AutoSize
