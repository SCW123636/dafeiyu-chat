# 注册登录自启（写 HKCU Run，不需要管理员）并立刻拉起守护循环
$ErrorActionPreference = 'Stop'
$ps = Join-Path $PSScriptRoot 'dafeiyu_online.ps1'

$cmd = '"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass ' +
       '-WindowStyle Hidden -File "' + $ps + '" -Watch'
Set-ItemProperty -Path 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run' -Name 'DaFeiYuOnline' -Value $cmd

$alive = Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" |
    Where-Object { $_.CommandLine -match 'dafeiyu_online\.ps1' -and $_.CommandLine -match '-Watch' }
if (-not $alive) {
    Start-Process 'powershell.exe' -ArgumentList '-NoProfile', '-ExecutionPolicy', 'Bypass',
        '-WindowStyle', 'Hidden', '-File', ('"' + $ps + '"'), '-Watch' -WindowStyle Hidden
    Start-Sleep 2
}

'已写入登录自启 HKCU\...\Run\DaFeiYuOnline，守护循环每 10 分钟检查一次'
Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" |
    Where-Object { $_.CommandLine -match 'dafeiyu_online\.ps1' } |
    ForEach-Object { "watch pid=$($_.ProcessId)" }
'取消自启：Remove-ItemProperty -Path HKCU:\Software\Microsoft\Windows\CurrentVersion\Run -Name DaFeiYuOnline'
'临时下线：在 startup 目录建一个名为「暂停上线」的空文件'
