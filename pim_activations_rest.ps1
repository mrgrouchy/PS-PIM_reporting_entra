[CmdletBinding()]
param(
  [int]$Days = 30,
  [datetime]$StartDate = (Get-Date).AddDays(-$Days),
  [datetime]$EndDate   = (Get-Date),
  [switch]$CompletedOnly = $true,
  [string]$OutputPath = (Get-Location),
  [switch]$DebugTickets
)

# --- minimal dependency: Microsoft.Graph.Authentication only ---
if (-not (Get-Module -ListAvailable Microsoft.Graph.Authentication)) {
  Write-Verbose "Installing Microsoft.Graph.Authentication (CurrentUser)…"
  Install-Module Microsoft.Graph.Authentication -Scope CurrentUser -ErrorAction SilentlyContinue -Verbose:$false
}

# Connect once (needs AuditLog.Read.All)
if (-not (Get-MgContext)) {
  Write-Verbose "Connecting to Graph (AuditLog.Read.All)…"
  Connect-MgGraph -Scopes "AuditLog.Read.All","Directory.Read.All" -ErrorAction Stop
} else {
  Write-Verbose "Using existing Graph context."
}

# Helpers
function ODataUtc([datetime]$d) { $d.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss'Z'") }

function Get-ADetail {
  [CmdletBinding()]
  param([object]$Details,[string[]]$Keys)
  if (-not $Details -or -not $Keys) { return $null }
  $lk = @($Keys | ForEach-Object { $_.ToString().ToLower() })
  foreach ($item in @($Details)) {
    if ($null -eq $item) { continue }
    $name = $null
    foreach ($f in 'key','Key','name','Name','displayName') {
      if ($item.PSObject.Properties.Match($f).Count -gt 0 -and $item.$f) { $name = $item.$f; break }
    }
    if (-not $name) { continue }
    if ($lk -notcontains $name.ToString().ToLower()) { continue }
    foreach ($vf in 'value','Value','AdditionalDetailsValue','newValue','NewValue') {
      if ($item.PSObject.Properties.Match($vf).Count -gt 0 -and $item.$vf) {
        $v = $item.$vf
        $text = $v.ToString()
        if ($text -match '^\s*"(.*)"\s*$') { $text = $matches[1] }
        if ($text.TrimStart().StartsWith("{") -or $text.TrimStart().StartsWith("[")) {
          try {
            $obj = $text | ConvertFrom-Json -ErrorAction Stop
            if ($obj -is [System.Collections.IEnumerable]) {
              foreach ($elem in @($obj)) {
                foreach ($nf in 'name','Name','key','Key','displayName') {
                  if ($elem.PSObject.Properties.Match($nf).Count -gt 0) {
                    $nm = $elem.$nf
                    if ($nm -and ($lk -contains $nm.ToString().ToLower())) {
                      foreach ($valf in 'value','Value','newValue','NewValue') {
                        if ($elem.PSObject.Properties.Match($valf).Count -gt 0 -and $elem.$valf) { return $elem.$valf }
                      }
                    }
                  }
                }
              }
            }
            if ($obj -is [System.Collections.IDictionary]) {
              foreach ($k in $Keys) {
                if ($obj.Contains($k)) { return $obj[$k] }
                $mk = $obj.Keys | Where-Object { $_.ToString().ToLower() -eq $k.ToLower() } | Select-Object -First 1
                if ($mk) { return $obj[$mk] }
              }
            }
          } catch {}
        }
        if ($text) { return $text }
      }
    }
  }
  return $null
}

function Get-Ticket { param([object]$Event)
  if (-not $Event) { return $null }
  $keys = @('TicketNumber','Ticket','Ticket Number','TicketId','Ticket ID','Incident','IncidentId','ChangeNumber','ChangeId','WorkItemId','ChangeTicket','Change Request')
  $t = Get-ADetail $Event.additionalDetails $keys
  if ($t) { return $t }
  if ($Event.targetResources -and $Event.targetResources[0].modifiedProperties) {
    $t = Get-ADetail $Event.targetResources[0].modifiedProperties $keys
    if ($t) { return $t }
  }
  if ($Event.resultReason) {
    try {
      $rr = $Event.resultReason.ToString()
      if ($rr.TrimStart().StartsWith("{") -or $rr.TrimStart().StartsWith("[")) {
        $obj = $rr | ConvertFrom-Json -ErrorAction Stop
        $t = Get-ADetail $obj $keys
        if ($t) { return $t }
        foreach ($k in $keys) {
          if ($obj.PSObject.Properties.Name -contains $k) { return $obj.$k }
          $mk = $obj.PSObject.Properties.Name | Where-Object { $_.ToLower() -eq $k.ToLower() } | Select-Object -First 1
          if ($mk) { return $obj.$mk }
        }
      }
    } catch {}
  }
  return $null
}

$start = ODataUtc $StartDate
$end   = ODataUtc $EndDate
Write-Verbose "Window: $start .. $end"

# PIM activation phrases
$activityFilter = if ($CompletedOnly) {
  "(activityDisplayName eq 'Add member to role completed (PIM activation)')"
} else {
  "(" +
    "activityDisplayName eq 'Add member to role requested (PIM activation)' or " +
    "activityDisplayName eq 'Add member to role completed (PIM activation)'" +
  ")"
}

$filter = "$activityFilter and (category eq 'RoleManagement') and activityDateTime ge $start and activityDateTime le $end"
$qs   = '$filter=' + [uri]::EscapeDataString($filter) + '&$top=999'
$uri  = "/v1.0/auditLogs/directoryAudits?$qs"
Write-Verbose "GET $uri"

# Fetch with paging
$events = @()
try {
  $resp = Invoke-MgGraphRequest -Method GET -Uri $uri -OutputType PSObject -ErrorAction Stop
  $events += @($resp.value)
  while ($resp.'@odata.nextLink') {
    $resp = Invoke-MgGraphRequest -Method GET -Uri $resp.'@odata.nextLink' -OutputType PSObject -ErrorAction Stop
    $events += @($resp.value)
  }
} catch {
  Write-Error "Failed to fetch audit logs: $($_.Exception.Message)"
  return
}

# Shape rows
$rows = foreach ($e in $events) {
  $upn = $e.initiatedBy.user.userPrincipalName
  if (-not $upn -and $e.targetResources -and $e.targetResources[0].userPrincipalName) { $upn = $e.targetResources[0].userPrincipalName }
  $member = $e.initiatedBy.user.displayName
  if (-not $member -and $e.targetResources -and $e.targetResources[0].displayName) { $member = $e.targetResources[0].displayName }
  $role = $null
  if ($e.targetResources -and $e.targetResources[0].displayName) { $role = $e.targetResources[0].displayName }
  if (-not $role -and $e.targetResources -and $e.targetResources[0].modifiedProperties) {
    $role = Get-ADetail $e.targetResources[0].modifiedProperties @('Role','RoleName','Role name','Role Display Name','displayName')
  }
  $just = Get-ADetail $e.additionalDetails @('Justification','Reason','JustificationText','Justification Text')
  if (-not $just -and $e.resultReason) { $just = $e.resultReason }
  $ticket = Get-Ticket $e

  if ($DebugTickets -and -not $ticket) {
    if (-not $script:_ticketProbes) { $script:_ticketProbes = @() }
    $probe = [ordered]@{
      Id = $e.id; MemberUPN = $upn; Activity = $e.activityDisplayName
      additionalDetails_keys = @($e.additionalDetails | ForEach-Object {
        if ($_ -ne $null) {
          if ($_.PSObject.Properties.Name -contains 'key') { $_.key }
          elseif ($_.PSObject.Properties.Name -contains 'name') { $_.name }
          elseif ($_.PSObject.Properties.Name -contains 'displayName') { $_.displayName }
        }
      }) | Where-Object { $_ }
      modifiedProperties_keys = @()
      resultReason = $e.resultReason
    }
    if ($e.targetResources -and $e.targetResources[0].modifiedProperties) {
      $probe.modifiedProperties_keys = @($e.targetResources[0].modifiedProperties | ForEach-Object {
        if ($_ -ne $null) {
          if ($_.PSObject.Properties.Name -contains 'key') { $_.key }
          elseif ($_.PSObject.Properties.Name -contains 'name') { $_.name }
          elseif ($_.PSObject.Properties.Name -contains 'displayName') { $_.displayName }
        }
      }) | Where-Object { $_ }
    }
    $script:_ticketProbes += [pscustomobject]$probe
  }

  [pscustomobject]@{
    Id            = ("PIM_{0}_{1}_{2}" -f ($e.correlationId ?? "n/a"), ($e.id ?? "n/a"), ([DateTimeOffset]$e.activityDateTime).Ticks)
    Key           = ("{0}|{1}|{2}|{3}|{4}" -f ([DateTime]$e.activityDateTime).ToString("dd/MM/yyyy HH:mm:ss"), $upn, $role, $e.activityDisplayName, $e.result)
    Timestamp     = $e.activityDateTime
    MemberUPN     = $upn
    Member        = $member
    Role          = $role
    Justification = $just
    Ticket        = $ticket
    Activity      = $e.activityDisplayName
    Result        = $e.result
    ResultReason  = $e.resultReason
  }
}

# -------- Merge with existing store and print emoji summary --------
if (-not (Test-Path $OutputPath)) { New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null }
$storePath = Join-Path $OutputPath "PIM_Activations.json"
$jsPath    = Join-Path $OutputPath "PIM_Activations.data.js"

$existing = @()
if (Test-Path $storePath) {
  try { $existing = Get-Content -Raw -Path $storePath | ConvertFrom-Json -ErrorAction Stop } catch { $existing = @() }
}

$ids = @{}
foreach ($r in @($existing)) { if ($r.Id) { $ids[$r.Id] = $true } elseif ($r.Key) { $ids[$r.Key] = $true } }

$newRows = @()
foreach ($r in @($rows)) {
  $k = if ($r.Id) { $r.Id } else { $r.Key }
  if (-not $ids.ContainsKey($k)) { $newRows += $r; $ids[$k] = $true }
}

$merged = @($existing) + @($newRows) | Sort-Object {[datetime]$_.Timestamp}

$existingCount = @($existing).Count
$newCount      = @($newRows).Count
$totalCount    = @($merged).Count

Write-Host ("🧮 Existing: {0}, New this run: {1}, Total now: {2}" -f $existingCount, $newCount, $totalCount)
if ($newCount -eq 0) {
  Write-Host "✅ No new rows to export this run."
} else {
  Write-Host ("✅ Added {0} new row{1}." -f $newCount, $(if($newCount -eq 1) {''} else {'s'}))
}

$merged | ConvertTo-Json -Depth 6 | Out-File -FilePath $storePath -Encoding utf8
Write-Host "✅ Store updated: $storePath"

@("window.PIM_ACTIVATIONS = ", ($merged | ConvertTo-Json -Depth 6)) -join "" | Out-File -FilePath $jsPath -Encoding utf8
Write-Host "✅ Web data updated: $jsPath"

if ($DebugTickets -and $script:_ticketProbes -and $script:_ticketProbes.Count -gt 0) {
  $probePath = Join-Path $OutputPath "ticket_probes.json"
  $script:_ticketProbes | ConvertTo-Json -Depth 6 | Out-File -FilePath $probePath -Encoding utf8
  Write-Host "🔎 Ticket diagnostics written to $probePath"
}