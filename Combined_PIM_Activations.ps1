<# 
  Inline Combined PIM/PAG + PIM Activations (no temp files)
  ---------------------------------------------------------
  Defaults:
    -Run Both
    -CompletedOnly:$true  (disable with -CompletedOnly:$false)
#>

[CmdletBinding()]
param(
  [ValidateSet('PIM','PAG','Both','Activations','All')]
  [string]$Run = 'All',

  # Friendly convenience params (optional)
  [ValidateSet('PIM','PAG','Both')]
  [string]$LookupMode,

  [string]$OutputFolder,
  [int]$Days,
  [datetime]$StartDate,
  [datetime]$EndDate,
  [switch]$CompletedOnly = $true,
  [switch]$DebugTickets,

  # Power-user: pass raw parameter hashtables that match the originals exactly
  [hashtable]$PIMPAGArgs = @{},
  [hashtable]$ActivationsArgs = @{}
)

function Write-Step($msg) { Write-Host ("==> {0}" -f $msg) }

# Build the embedded originals as scriptblocks
$code_PIMPAG = @'
#Requires -Modules Microsoft.Graph

[CmdletBinding()]
param(
    [Parameter(Mandatory=$false)]
    [ValidateSet('PIM','PAG','Both')]
    [string]$LookupMode = 'Both',

    [Parameter(Mandatory=$false)]
    [string]$OutputFolder = (Get-Location).Path
)

# -------------------- Mode Booleans --------------------
$DoPIM = $LookupMode -in @('PIM','Both')
$DoPAG = $LookupMode -in @('PAG','Both')

# -------------------- Verbose helpers --------------------
function _v([string]$Msg){ Write-Verbose ("[{0:HH:mm:ss}] {1}" -f (Get-Date), $Msg) }
function _step([string]$Msg){ Write-Host ("[{0:HH:mm:ss}] {1}" -f (Get-Date), $Msg) -ForegroundColor Yellow }
function _elapsed([Diagnostics.Stopwatch]$sw){ '{0:mm\:ss\.fff}' -f $sw.Elapsed }

# -------------------- Helpers --------------------
function Join-List([object[]]$arr) { @($arr | Where-Object { $_ -and $_.ToString().Trim() -ne '' }) -join ';' }
function Split-List([string]$s) {
    if (-not $s) { return @() }
    return @($s -split ';' | ForEach-Object { $_.Trim() } | Where-Object { $_ }) | Sort-Object -Unique
}
function As-Array { param($x) if ($null -eq $x) { @() } else { @($x) } }
function CountOf { param($x) @($x).Count }
function Coalesce { param($a, $b) if ($null -ne $a -and $a -ne '') { $a } else { $b } }
function Get-ReportKey($o) {
    # Prefer object IDs; fall back to display names if needed
    $pKey  = $null
    $rKey  = $null
    $scope = $null

    if ($o.PSObject.Properties.Name -contains 'PrincipalObjectId') { $pKey = $o.PrincipalObjectId }
    if (-not $pKey -and $o.PSObject.Properties.Name -contains 'Principal') { $pKey = $o.Principal }

    if ($o.PSObject.Properties.Name -contains 'RoleDefinitionId' -and $o.RoleDefinitionId) {
        $rKey = $o.RoleDefinitionId
    } elseif ($o.PSObject.Properties.Name -contains 'AssignedRole' -and $o.AssignedRole) {
        # Fallback so eligibilities (which can miss RoleDefinitionId) still match
        $rKey = "NAME:"+$o.AssignedRole
    }

    # Normalize scope: treat null/empty as '/'
    if ($o.PSObject.Properties.Name -contains 'AssignedRoleScope') {
        $scope = $o.AssignedRoleScope
    }
    if ([string]::IsNullOrWhiteSpace($scope)) { $scope = '/' }

    if (-not $pKey -or -not $rKey -or -not $scope) { return $null }
    return ("{0}|{1}|{2}" -f $pKey, $rKey, $scope)
}
function Load-ExistingJs([string]$path, [string]$globalName) {
    if (-not (Test-Path $path)) { _v "No existing JS at '$path'"; return @() }
    try {
        $raw = Get-Content -Path $path -Raw

        # Strip any stray BOM/control marks
        $raw = $raw -replace ([char]0xFEFF), '' -replace ([char]0x200B), '' -replace ([char]0x200E), '' -replace ([char]0x200F), ''

        # Primary pattern: lazy JSON array capture (handles newlines)
        $pattern = "window\.$([regex]::Escape($globalName))\s*=\s*(\[[\s\S]*?\])\s*;"
        $m = [regex]::Match($raw, $pattern)
        if ($m.Success) {
            _v "Parsed existing '$globalName' from $path"
            return @($m.Groups[1].Value | ConvertFrom-Json)
        }

        # Fallback: find the first '[' after the assignment and the last ']' before ';'
        $idx = $raw.IndexOf("window.$globalName")
        if ($idx -ge 0) {
            $after = $raw.Substring($idx)
            $eq = $after.IndexOf('=')
            if ($eq -ge 0) {
                $afterEq = $after.Substring($eq + 1)
                $l = $afterEq.IndexOf('[')
                $r = $afterEq.LastIndexOf(']')
                if ($l -ge 0 -and $r -ge $l) {
                    $json = $afterEq.Substring($l, $r - $l + 1)
                    try {
                        _v "Parsed existing '$globalName' via fallback from $path"
                        return @($json | ConvertFrom-Json)
                    } catch {
                        Write-Warning "Fallback JSON parse failed for '$globalName' in $path : $($_.Exception.Message)"
                    }
                }
            }
        }

        _v "No window.$globalName payload found in $path"
        return @()
    } catch {
        Write-Warning "Existing JS at '$path' could not be parsed. Treating as first run. $($_.Exception.Message)"
        return @()
    }
}
function Save-Js([object[]]$data, [string]$path, [string]$globalName) {
    $null = New-Item -Path (Split-Path $path -Parent) -ItemType Directory -Force -ErrorAction SilentlyContinue
    $json   = $data | Sort-Object PrincipalDisplayName | ConvertTo-Json -Depth 12
    $banner = "// Generated on $(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') — do not edit by hand`n"
    $body   = "window.$globalName = $json;"
    Set-Content -Path $path -Encoding utf8 -Value ($banner + $body)
    _v ("Wrote {0} items to {1}" -f (CountOf(As-Array $data)), $path)
}

# -------------------- CONNECT --------------------
$scopes = @("Directory.Read.All","Group.Read.All")
if ($DoPIM) { $scopes += "RoleManagement.Read.Directory" }
if ($DoPAG) { $scopes += @("RoleManagement.Read.Directory","PrivilegedAccess.Read.AzureADGroup") }
$scopes = $scopes | Select-Object -Unique
_step ("Connecting to Microsoft Graph | Scopes: {0}" -f ($scopes -join ', '))

try {
    Connect-MgGraph -Scopes $scopes | Out-Null
    _v "Connected to Graph"
} catch {
    Write-Error "Connect-MgGraph failed: $($_.Exception.Message)"
    throw
}

# -------------------- Containers --------------------
$roles           = @()
$roleactivations = @()
$Proles          = @()

# -------------------- PIM (Directory Roles) --------------------
if ($DoPIM) {
    _step "PIM: Fetching role assignments (+principal)"
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $rolesAssignments  = Get-MgRoleManagementDirectoryRoleAssignment -All -ExpandProperty Principal
    _v ("PIM: role assignments fetched = {0} in {1}" -f (CountOf(As-Array $rolesAssignments)), (_elapsed $sw))

    _step "PIM: Fetching role assignments (+roleDefinition)"
    $sw.Restart()
    $rolesWithDefsOnly = Get-MgRoleManagementDirectoryRoleAssignment -All -ExpandProperty roleDefinition
    _v ("PIM: role assignments (defs) fetched = {0} in {1}" -f (CountOf(As-Array $rolesWithDefsOnly)), (_elapsed $sw))

    # Map roleDefinition onto each role (key = RoleDefinitionId)
    _v "PIM: Mapping roleDefinition objects by Id"
    $roleDefById = @{}
    foreach ($r in (As-Array $rolesWithDefsOnly)) { if ($r.roleDefinition) { $roleDefById[$r.roleDefinition.Id] = $r.roleDefinition } }
    foreach ($role in (As-Array $rolesAssignments)) {
        $rd = $null
        if ($role.roleDefinitionId -and $roleDefById.ContainsKey($role.roleDefinitionId)) { $rd = $roleDefById[$role.roleDefinitionId] }
        Add-Member -InputObject $role -MemberType NoteProperty -Name roleDefinition1 -Value $rd -Force
    }

    _step "PIM: Fetching eligibilities (+defs,+principal)"
    $sw.Restart()
    $eligibility = Get-MgRoleManagementDirectoryRoleEligibilitySchedule -All -ExpandProperty roleDefinition,principal -Verbose:$false -ErrorAction Stop |
        Select-Object id,principalId,directoryScopeId,roleDefinitionId,status,principal,
                      @{n="roleDefinition1";e={$_.roleDefinition}}
    _v ("PIM: eligibilities fetched = {0} in {1}" -f (CountOf(As-Array $eligibility)), (_elapsed $sw))

    _v "PIM: Merging roles + eligibilities"
    $roles += $rolesAssignments
    $roles += $eligibility

   _step "PIM: Fetching activations (AssignmentType = 'Activated')"
$sw.Restart()
try {
    $roleactivations = Get-MgRoleManagementDirectoryRoleAssignmentScheduleInstance `
        -All -Filter "AssignmentType eq 'Activated'" -Verbose:$false -ErrorAction Stop
} catch {
    Write-Error "Failed to fetch activations: $($_.Exception.Message)"
    $roleactivations = @()
}
_v ("PIM: activations fetched = {0} in {1}" -f (CountOf $roleactivations), (_elapsed $sw))

_v "PIM: Marking duplicates (assignment.Id == activation.RoleAssignmentOriginId)"
$dup = 0
foreach ($act in @($roleactivations)) {
    $roles | Where-Object { $_.Id -eq $act.RoleAssignmentOriginId } |
        ForEach-Object { Add-Member -InputObject $_ -MemberType NoteProperty -Name "Duplicate" -Value $true -Force; $dup++ }
}
_v ("PIM: duplicates marked = {0}" -f $dup)

    if (CountOf(As-Array $roles) -eq 0) { Write-Warning "No valid PIM role assignments found." }
    else { _step ("PIM: total role rows after merge = {0}" -f (CountOf(As-Array $roles))) }
}

# -------------------- PAG (Privileged Access Groups) --------------------
if ($DoPAG) {
    if (-not $DoPIM) {
        _step "PAG: Fetching role assignments (+principal) for PAG-only run"
        $sw = [Diagnostics.Stopwatch]::StartNew()
        $roles = Get-MgRoleManagementDirectoryRoleAssignment -All -ExpandProperty Principal
        _v ("PAG: role assignments fetched = {0} in {1}" -f (CountOf(As-Array $roles)), (_elapsed $sw))
        try {
            _v "PAG: Expanding roleDefinition onto roles"
            $rolesWithDefsOnly = Get-MgRoleManagementDirectoryRoleAssignment -All -ExpandProperty roleDefinition
            $roleDefById = @{}
            foreach ($r in (As-Array $rolesWithDefsOnly)) { if ($r.roleDefinition) { $roleDefById[$r.roleDefinition.Id] = $r.roleDefinition } }
            foreach ($role in (As-Array $roles)) {
                $rd = $null
                if ($role.roleDefinitionId -and $roleDefById.ContainsKey($role.roleDefinitionId)) { $rd = $roleDefById[$role.roleDefinitionId] }
                Add-Member -InputObject $role -MemberType NoteProperty -Name roleDefinition1 -Value $rd -Force
            }
        } catch {
            Write-Warning "Couldn't expand roleDefinition for PAG-only run: $($_.Exception.Message)"
        }
    }

    _step "PAG: Selecting roles with Group principals"
    $Proles = @($roles | Where-Object { $_.Principal.AdditionalProperties.'@odata.type' -eq '#microsoft.graph.group' })
    $n = (CountOf $Proles)
    _v ("PAG: group-principal roles = {0}" -f $n)

    if ($n -gt 0) {
        $i = 0
        foreach ($role in $Proles) {
            $i++
            if (($i % 25) -eq 1 -or $i -eq $n) {
                Write-Progress -Activity "PAG enrichment" -Status ("Group roles processed: {0}/{1}" -f $i, $n) -PercentComplete ([int](($i / $n) * 100))
            }
            $role | Add-Member -MemberType NoteProperty -Name "PAGEligibleReadSucceeded" -Value $false -Force

            # Active (transitive) members
            $dMembers=@{}; $dMembersId=@()
            try {
                _v ("PAG[{0}/{1}] Transitive members for group {2}" -f $i,$n,$role.PrincipalId)
                $transitive = Get-MgGroupTransitiveMember -GroupId $role.PrincipalId `
                    -Property id,displayName,userPrincipalName -Verbose:$false -ErrorAction Stop
                foreach ($member in (As-Array $transitive)) {
                    $upn = $member.AdditionalProperties.userPrincipalName
                    $dMembers[$member.Id] = $upn
                    if ($upn) { $dMembersId += $upn }
                    elseif ($member.AdditionalProperties.displayName) { $dMembersId += $member.AdditionalProperties.displayName }
                }
                $dMembersId = $dMembersId | ForEach-Object { $_.Trim() } | Sort-Object -Unique
                _v ("PAG[{0}/{1}] Eligible members indexed = {2}" -f $i, $n, (CountOf $eMembersId))
            } catch {
                Write-Warning ("Transitive member read failed for group {0}: {1}" -f $role.PrincipalId, $_.Exception.Message)
            }
            $role | Add-Member -MemberType NoteProperty -Name "Active group members"      -Value $dMembers -Force
            $role | Add-Member -MemberType NoteProperty -Name "Active group members IDs"  -Value (@($dMembersId) -join ";") -Force

            # Eligible members (PAG)
            $eMembers=@{}; $eMembersId=@()
            try {
                _v ("PAG[{0}/{1}] Eligible schedules for group {2}" -f $i,$n,$role.PrincipalId)
                $eligible = Get-MgIdentityGovernancePrivilegedAccessGroupEligibilitySchedule `
                    -Filter "groupId eq '$($role.principalId)'" -ExpandProperty principal -Verbose:$false -ErrorAction Stop
                foreach ($member in (As-Array $eligible)) {
                    $memberId = $member.principal.Id
                    $upn      = $member.principal.AdditionalProperties.userPrincipalName
                    $eMembers[$memberId] = $upn
                    if ($upn) { $eMembersId += $upn }
                    elseif ($member.principal.AdditionalProperties.displayName) { $eMembersId += $member.principal.AdditionalProperties.displayName }
                }
                $eMembersId = $eMembersId | ForEach-Object { $_.Trim() } | Sort-Object -Unique
                $role.PAGEligibleReadSucceeded = $true
                _v ("PAG[{0}/{1}] Eligible members indexed = {2}" -f $i, $n, (CountOf $eMembersId))
            } catch {
                Write-Warning ("PAG eligibility read failed for group {0}: {1}" -f $role.PrincipalId, $_.Exception.Message)
            }
            $role | Add-Member -MemberType NoteProperty -Name "Eligible group members"     -Value $eMembers -Force
            $role | Add-Member -MemberType NoteProperty -Name "Eligible group members IDs" -Value (@($eMembersId) -join ";") -Force
        }
        Write-Progress -Activity "PAG enrichment" -Completed
    }

    if (-not $DoPIM) {
        $roles = @($Proles)
        _v ("PAG-only: limiting roles to group-principal set = {0}" -f (CountOf(As-Array $roles)))
    }
}

# -------------------- BUILD REPORTS (separate PIM/PAG) --------------------
_step ("Building reports (LookupMode={0})" -f $LookupMode)
$nowUtc = (Get-Date).ToUniversalTime().ToString("s") + "Z"
$roles           = As-Array $roles
$roleactivations = As-Array $roleactivations
$reportPIM = @(); $reportPAG = @()

$k = 0
$kmax = (CountOf $roles)   # uses the helper: function CountOf { param($x) @CountOf($x) }
foreach ($role in $roles) {
    $k++
    if (($k % 200) -eq 1 -or $k -eq $kmax) {
        Write-Progress -Activity "Composing report rows" -Status ("Processed: {0}/{1}" -f $k,$kmax) -PercentComplete ([int](($k/$kmax)*100))
    }
    if (-not $role) { continue }
    if ($role.PSObject.Properties.Name -contains 'Duplicate' -and $role.Duplicate) { continue }

    # Determine if group principal
    $ptypeFull = $null
    $isGroup = $false

    $hasPrincipalProp = (CountOf ($role.PSObject.Properties.Match('principal'))) -gt 0
    if ($hasPrincipalProp -and $role.principal -and $role.principal.AdditionalProperties) {
        $ptypeFull = $role.principal.AdditionalProperties.'@odata.type'
        $isGroup = ($ptypeFull -eq '#microsoft.graph.group')
    }

    # Normalize assignment info (safe status access)
    $statusValue = $role | Select-Object -ExpandProperty status -ErrorAction SilentlyContinue
    if (-not $statusValue) {
        $role | Add-Member -MemberType NoteProperty -Name "Start time"     -Value "Permanent" -Force
        $role | Add-Member -MemberType NoteProperty -Name "End time"       -Value "Permanent" -Force
        $role | Add-Member -MemberType NoteProperty -Name "AssignmentType" -Value "Permanent" -Force
        $role | Add-Member -MemberType NoteProperty -Name "Activated for"  -Value $null -Force
        $activeRole = @()
    } else {
        if ($isGroup) {
            if (-not ($role.PSObject.Properties.Name -contains 'Active group members')) {
                $role | Add-Member -MemberType NoteProperty -Name "Active group members" -Value @{} -Force
            }
            $activeMembersMap = $role.'Active group members'; if (-not $activeMembersMap) { $activeMembersMap = @{} }
            $activeRole = @()
            if ($role.roleDefinitionId) {
                $activeRole = $roleactivations | Where-Object {
                    ($_.roleDefinitionId -eq $role.roleDefinitionId) -and
                    ($_.MemberType -eq "Group") -and
                    ($activeMembersMap.ContainsKey($_.principalId))
                }
            }
            $activatedFor = @($activeRole | ForEach-Object { $activeMembersMap[$_.principalId] }) -join ";"
            $role | Add-Member -MemberType NoteProperty -Name "Activated for" -Value $activatedFor -Force
        } else {
            $activeRole = @()
            if ($role.roleDefinitionId -and $role.PrincipalId) {
                $activeRole = $roleactivations | Where-Object {
                    ($_.roleDefinitionId -eq $role.roleDefinitionId) -and
                    ($_.PrincipalId -eq $role.PrincipalId)
                }
            }
            $role | Add-Member -MemberType NoteProperty -Name "Activated for" -Value $null -Force
        }

        $start = $activeRole | Select-Object -ExpandProperty startDateTime -ErrorAction Ignore | Sort-Object | Select-Object -First 1
        $end   = $activeRole | Select-Object -ExpandProperty endDateTime   -ErrorAction Ignore | Sort-Object -Descending | Select-Object -First 1
        $startVal = $null; if ($start) { $startVal = Get-Date $start -Format g }
        $endVal   = $null; if ($end)   { $endVal   = Get-Date $end   -Format g }

        $role | Add-Member -MemberType NoteProperty -Name "Start time"     -Value $startVal -Force
        $role | Add-Member -MemberType NoteProperty -Name "End time"       -Value $endVal   -Force
        $role | Add-Member -MemberType NoteProperty -Name "AssignmentType" -Value ($(if ($start) { "Eligible (Active)" } else { "Eligible" })) -Force
    }

    # Principal details (display)
    $principalVal = $role.PrincipalId; $principalDisplay = $null
    if ($ptypeFull) {
        $aprops = $role.principal.AdditionalProperties
        $principalDisplay = $aprops.displayName
        switch ($ptypeFull) {
            '#microsoft.graph.user'             { if ($aprops.userPrincipalName) { $principalVal = $aprops.userPrincipalName } }
            '#microsoft.graph.servicePrincipal' { if ($aprops.appId)             { $principalVal = $aprops.appId } }
            '#microsoft.graph.group'            { $principalVal = $role.PrincipalId }
            default                             { $principalVal = $role.PrincipalId }
        }
    }
    $principalTypeShort = if ($ptypeFull) { $ptypeFull.Split(".")[-1] } else { $null }

    # Role definition
    $rd           = if ($role.PSObject.Properties.Name -contains 'roleDefinition1') { $role.roleDefinition1 } else { $null }
    $assignedRole = if ($rd) { $rd.displayName } else { $null }
    $isBuiltIn    = if ($rd) { $rd.isBuiltIn }   else { $null }
    $templateId   = if ($rd) { $rd.templateId }  else { $null }

    # Optional group member IDs
    $activeIds        = if ($role.PSObject.Properties.Name -contains 'Active group members IDs')   { $role.'Active group members IDs' }   else { $null }
    $eligibleIds      = if ($role.PSObject.Properties.Name -contains 'Eligible group members IDs') { $role.'Eligible group members IDs' } else { $null }
    $activatedForDisp = if ($role.PSObject.Properties.Name -contains 'Activated for')              { $role.'Activated for' }              else { $null }

    # Internal keys/flags
    $principalObjectId = $role.PrincipalId
    $roleDefId         = $role.roleDefinitionId
    $objId             = $role.Id
    $pagEligRead       = if ($role.PSObject.Properties.Name -contains 'PAGEligibleReadSucceeded') { $role.PAGEligibleReadSucceeded } else { $null }
    
    # Normalize scope (null/empty => '/')
    $assignedScope = if ([string]::IsNullOrWhiteSpace($role.directoryScopeId)) { '/' } else { $role.directoryScopeId }

    $line = [ordered]@{
        "Principal"                           = $principalVal
        "PrincipalDisplayName"                = $principalDisplay
        "PrincipalType"                       = $principalTypeShort
        "AssignedRole"                        = $assignedRole
        "AssignedRoleScope"                   = $assignedScope
        "AssignmentType"                      = $role.AssignmentType
        "AssignmentStartDate"                 = $role.'Start time'
        "AssignmentEndDate"                   = $role.'End time'
        "ActiveGroupMembers"                  = $activeIds
        "EligibleGroupMembers"                = $eligibleIds
        "GroupEligibleAssignmentActivatedFor" = $activatedForDisp
        "IsBuiltIn"                           = $isBuiltIn
        "RoleTemplate"                        = $templateId
        "PrincipalObjectId"                   = $principalObjectId
        "RoleDefinitionId"                    = $roleDefId
        "ObjectId"                            = $objId
        "PAGEligibleReadSucceeded"            = $pagEligRead
        "EligibleGroupMembersAdded"           = $null
        "EligibleGroupMembersRemoved"         = $null
        "LastUpdatedUtc"                      = $nowUtc
    }

    $reportPIM += [pscustomobject]$line
    if ($isGroup) { $reportPAG += [pscustomobject]$line }
}

Write-Progress -Activity "Composing report rows" -Completed
_step ("Report rows composed | PIM={0}, PAG={1}" -f (CountOf(As-Array $reportPIM)), (CountOf(As-Array $reportPAG)))

# -------------------- MERGE (PAG only, from prior .data.js) --------------------
$pagPath   = Join-Path -Path $OutputFolder -ChildPath "AdministratorsReport-PAG.data.js"
$pagGlobal = 'PAG_REPORT'

if ($DoPAG -and (CountOf $reportPAG) -gt 0) {
    _step "PAG: Merging with previous snapshot (if any)"
    $existingPAG    = Load-ExistingJs -path $pagPath -globalName $pagGlobal
    $existingPAGIdx = @{}
    if ($existingPAG) {
        foreach ($e in $existingPAG) { $k = Get-ReportKey $e; if ($k) { $existingPAGIdx[$k] = $e } }
        _v ("PAG: prior items indexed = {0}" -f (CountOf $existingPAGIdx.Keys))
    }

    $m = 0
    foreach ($cur in $reportPAG) {
        $key = Get-ReportKey $cur
        if (-not $key) { continue }
         $prev = $null
        if ($existingPAGIdx.ContainsKey($key)) {
        $prev = $existingPAGIdx[$key]
        } else {
            _v ("PAG: no prior snapshot for key {0}" -f $key)   # <-- DEBUG (4) goes here
        }

        $curEligible = Split-List $cur.EligibleGroupMembers
        if ($null -ne $prev) {
            $prevEligible = Split-List $prev.EligibleGroupMembers
        } else {
             $prevEligible = @()
        }

        # Did this run successfully read eligibles?
        $readProp = $cur.PSObject.Properties['PAGEligibleReadSucceeded']
        $readSucceeded = $false; if ($readProp) { $readSucceeded = [bool]$readProp.Value }

        if ($readSucceeded) {
            $added   = @($curEligible  | Where-Object { $_ -and ($_ -notin $prevEligible) })
            $removed = @($prevEligible | Where-Object { $_ -and ($_ -notin $curEligible) })

            $cur.EligibleGroupMembers        = if (CountOf(As-Array $curEligible)) { Join-List $curEligible } else { $null }
            $cur.EligibleGroupMembersAdded   = if (CountOf(As-Array $added))       { Join-List $added }       else { $null }
            $cur.EligibleGroupMembersRemoved = if (CountOf(As-Array $removed))     { Join-List $removed }     else { $null }
        } else {
            if ($prev -and $prev.EligibleGroupMembers) {
                $cur.EligibleGroupMembers        = $prev.EligibleGroupMembers
                $cur.EligibleGroupMembersAdded   = $null
                $cur.EligibleGroupMembersRemoved = $null
            }
        }
        $m++
        if (($m % 200) -eq 0) { _v ("PAG: merged {0} items…" -f $m) }
    }
    _v ("PAG: merge complete for {0} items" -f $m)
}

# -------------------- EXPORT JS FILES --------------------
$pimPath   = Join-Path -Path $OutputFolder -ChildPath "AdministratorsReport-PIM.data.js"
$pimGlobal = 'PIM_REPORT'
if ($DoPIM) { _step "Writing PIM JS"; Save-Js -data $reportPIM -path $pimPath -globalName $pimGlobal }
if ($DoPAG) { _step "Writing PAG JS"; Save-Js -data $reportPAG -path $pagPath -globalName $pagGlobal }

# -------------------- END-OF-RUN SUMMARY (Verbose only) --------------------
$allPIM = if ($DoPIM) { As-Array $reportPIM } else { @() }
$allPAG = if ($DoPAG) { As-Array $reportPAG } else { @() }

$pimPerm = (As-Array ($allPIM | Where-Object { $_.AssignmentType -eq 'Permanent' })).Count
$pimElig = (As-Array ($allPIM | Where-Object { $_.AssignmentType -eq 'Eligible' })).Count
$pimActv = (As-Array ($allPIM | Where-Object { $_.AssignmentType -eq 'Eligible (Active)' })).Count

$pagReadSucceeded       = (As-Array ($allPAG | Where-Object { $_.PAGEligibleReadSucceeded -eq $true })).Count
$pagReadFailedPreserved = (As-Array ($allPAG | Where-Object { $_.PAGEligibleReadSucceeded -ne $true -and $_.EligibleGroupMembers })).Count
$changed = As-Array ($allPAG | Where-Object { $_.PAGEligibleReadSucceeded -eq $true -and ( $_.EligibleGroupMembersAdded -or $_.EligibleGroupMembersRemoved ) })

$addedTotal = 0; $removedTotal = 0
foreach ($c in $changed) {
    if ($c.'EligibleGroupMembersAdded')   { $addedTotal   += (As-Array ($c.'EligibleGroupMembersAdded'   -split ';')).Count }
    if ($c.'EligibleGroupMembersRemoved') { $removedTotal += (As-Array ($c.'EligibleGroupMembersRemoved' -split ';')).Count }
}

_v ("==== Summary ({0}) ====" -f $LookupMode)

if ($DoPIM) {
    $pimRecs = (CountOf (As-Array $allPIM))
    _v ("PIM JS : {0} | Records: {1} | Permanent: {2}, Eligible: {3}, Eligible (Active): {4}" -f `
            $pimPath, $pimRecs, $pimPerm, $pimElig, $pimActv)
}

if ($DoPAG) {
    $pagRecs = (CountOf (As-Array $allPAG))
    $chgRecs = (CountOf (As-Array $changed))
    _v ("PAG JS : {0} | Records: {1} | Eligible read OK: {2}, Preserved from prior: {3}, Changed this run: {4} (+{5}/-{6})" -f `
            $pagPath, $pagRecs, $pagReadSucceeded, $pagReadFailedPreserved, $chgRecs, $addedTotal, $removedTotal)

    $preview = $changed | Select-Object -First 5 `
        PrincipalDisplayName, AssignedRole, AssignedRoleScope, `
        EligibleGroupMembersAdded, EligibleGroupMembersRemoved

    if ($preview) {
        _v "Top changes (up to 5):"
        foreach ($p in $preview) {
            _v ("- {0} | {1} | scope: {2} | +[{3}] -[{4}]" -f `
                (Coalesce $p.PrincipalDisplayName $p.Principal),
                (Coalesce $p.AssignedRole $p.RoleDefinitionId),
                $p.AssignedRoleScope,
                (Coalesce $p.EligibleGroupMembersAdded ''),
                (Coalesce $p.EligibleGroupMembersRemoved ''))
        }
    }
}

_v ("Last updated (UTC): {0:yyyy-MM-ddTHH:mm:ssZ}" -f (Get-Date).ToUniversalTime())



'@

$code_ACT = @'
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
'@

$PIM_PAG_Script   = [ScriptBlock]::Create($code_PIMPAG)
$Activations_Script = [ScriptBlock]::Create($code_ACT)

# Prepare output folder if supplied
if ($PSBoundParameters.ContainsKey('OutputFolder')) {
  if (-not (Test-Path -LiteralPath $OutputFolder)) {
    New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null
  }
}

# Compose splats from convenience params + explicit *Args hashtables
$pp = @{} + $PIMPAGArgs
$aa = @{} + $ActivationsArgs

if ($PSBoundParameters.ContainsKey('LookupMode')) { $pp['LookupMode'] = $LookupMode }
if ($PSBoundParameters.ContainsKey('OutputFolder')) { 
  $pp['OutputFolder'] = $OutputFolder 
  # common alternative name used by activations script
  $aa['OutputPath']   = $OutputFolder 
}
if ($PSBoundParameters.ContainsKey('Days'))        { $aa['Days']        = $Days }
if ($PSBoundParameters.ContainsKey('StartDate'))   { $aa['StartDate']   = $StartDate }
if ($PSBoundParameters.ContainsKey('EndDate'))     { $aa['EndDate']     = $EndDate }
# Switch defaults to $true; allow explicit false via -CompletedOnly:$false
$aa['CompletedOnly'] = [bool]$CompletedOnly
if ($PSBoundParameters.ContainsKey('DebugTickets'))  { $aa['DebugTickets']  = [bool]$DebugTickets }

switch ($Run) {
  'PIM' {
    Write-Step "Running PIM via PIM_PAG.ps1 code"
    $pp['LookupMode'] = 'PIM'
    & $PIM_PAG_Script @pp
  }
  'PAG' {
    Write-Step "Running PAG via PIM_PAG.ps1 code"
    $pp['LookupMode'] = 'PAG'
    & $PIM_PAG_Script @pp
  }
  'Both' {
    Write-Step "Running PIM+PAG via PIM_PAG.ps1 code"
    $pp['LookupMode'] = 'Both'
    & $PIM_PAG_Script @pp
  }
  'Activations' {
    Write-Step "Running Activations via pim_activations_rest.ps1 code"
    & $Activations_Script @aa
  }
  'All' {
    Write-Step "Running PIM+PAG via PIM_PAG.ps1 code"
    $pp['LookupMode'] = 'Both'
    & $PIM_PAG_Script @pp

    Write-Step "Running Activations via pim_activations_rest.ps1 code"
    & $Activations_Script @aa
  }
}
