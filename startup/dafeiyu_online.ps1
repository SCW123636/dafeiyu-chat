param([switch]$Watch, [int]$Every = 10)
$ErrorActionPreference = 'SilentlyContinue'
$Root = Split-Path -Parent $PSScriptRoot
$Log = Join-Path $PSScriptRoot 'online.log'
$Hold = Join-Path $PSScriptRoot '暂停上线'
$script:actions = @()

function Log([string]$m) {
    "$(Get-Date -Format 'MM-dd HH:mm:ss') $m" | Out-File -FilePath $Log -Append -Encoding utf8
}

function Test-Port([int]$p) {
    $c = New-Object Net.Sockets.TcpClient
    try { $c.Connect('127.0.0.1', $p); $true } catch { $false } finally { $c.Close() }
}

function Wait-Port([int]$p, [int]$sec) {
    $end = (Get-Date).AddSeconds($sec)
    while ((Get-Date) -lt $end) { if (Test-Port $p) { return $true }; Start-Sleep 1 }
    return $false
}

function Invoke-Online {
    $script:actions = @()
    if (Test-Path $Hold) { if (-not $Watch) { Log '有「暂停上线」标记，本次跳过' }; return }

    if (-not (Test-Port 3000)) {
        $script:actions += '拉起 NapCat'
        $bat = Join-Path $Root 'vendor\napcat-shell\napcat\launcher-user.bat'
        Start-Process cmd.exe -ArgumentList '/c', "`"$bat`"" -WorkingDirectory (Split-Path $bat) -WindowStyle Hidden
        if (-not (Wait-Port 3000 150)) { Log 'NapCat 150 秒内没起来：多半是 QQ 需要重新扫码登录'; return }
    }

    if (-not (Test-Port 3002)) {
        $script:actions += '拉起中控台'
        $py = 'C:\Users\lenovo\AppData\Local\Programs\Python\Python310\python.exe'
        if (-not (Test-Path $py)) { $py = (Get-Command python.exe).Source }
        Start-Process $py -ArgumentList '-u', 'panel\server.py', '--no-open' -WorkingDirectory $Root -WindowStyle Hidden
        if (-not (Wait-Port 3002 40)) { Log '中控台 40 秒内没起来'; return }
    }

    $qq = (Invoke-RestMethod 'http://127.0.0.1:3002/api/state').platforms.qq.runner
    if (-not $qq.running) {
        if (Test-Port 3001) {
            $script:actions += '接管旧 QQ agent'
            Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:3002/api/qq/takeover' `
                -ContentType 'application/json' -Body '{"confirm":true}' | Out-Null
        } else {
            $script:actions += '启动 QQ agent（真发）'
            Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:3002/api/run/qq' `
                -ContentType 'application/json' -Body '{"dry_run":false,"confirm":true}' | Out-Null
        }
        if (-not (Wait-Port 3001 30)) { Log 'QQ agent 端口起不来，看 panel\logs\qq.log'; return }
    }

    if ($script:actions.Count -gt 0 -or -not $Watch) {
        $msg = if ($script:actions.Count) { $script:actions -join '、' } else { '全部已在线' }
        Log ("{0} ｜ 在线：NapCat 3000 / 面板 3002 / QQ 3001" -f $msg)
    }
}

if ($Watch) {
    while ($true) { Invoke-Online; Start-Sleep -Seconds ($Every * 60) }
} else {
    Invoke-Online
}
