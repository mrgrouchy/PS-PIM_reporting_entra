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


