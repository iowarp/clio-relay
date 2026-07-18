# 1.0 live acceptance runbook

This is the operator procedure for collecting the non-local evidence required
by [`release-gate-1.0.yaml`](release-gate-1.0.yaml). Run the complete procedure
twice: once against the exact draft wheel and once against the released public
PyPI artifact. A candidate report cannot be renamed or reused as released
evidence.

`ares` and `homelab` below are the concrete physical evidence instances selected
by the 1.0 release policy. They are not product allowlists. Any operator can
register other cluster names, and a later policy can select other evidence
instances without a core-code change.

## Required report matrix

Each stage produces exactly these 17 policy reports. The tracked
[`report-matrix-1.0.json`](../examples/release-gate/report-matrix-1.0.json) is the
machine-readable inventory.

| # | report id | producer | evidence relationship |
|---:|---|---|---|
| 1 | `ares-bootstrap` | `cluster bootstrap` | exact artifact deployment |
| 2 | `ares-cluster-bootstrap-live-test` | `live-test` | separate worker execution proof |
| 3 | `ares-queue-management` | `queue validate` | must precede lifecycle fixtures |
| 4 | `ares-jarvis-gray-scott` | `jarvis-mcp-validate` | bounded package search, Gray-Scott run, progress, query, and artifacts |
| 5 | `ares-jarvis-lammps` | `jarvis-mcp-validate` | bounded package search and separate LAMMPS progress run |
| 6 | `ares-spack-find` | `remote-mcp validate` | `spack_find` |
| 7 | `ares-spack-locate` | `remote-mcp validate` | `spack_locate` |
| 8 | `ares-spack-install` | `remote-mcp validate` | absent -> fresh install -> exact locate transition |
| 9 | `ares-slurm-lifecycle` | `scheduler validate-lifecycle` | explicit SLURM provider |
| 10 | `ares-cleanup-detach` | `session detach` | grouped with report 11 by relay session |
| 11 | `ares-cleanup-teardown` | `session teardown` | explicit keep-jobs default proof |
| 12 | `ares-explicit-cancel-teardown` | `session teardown` | separate owned job plus unowned sentinel |
| 13 | `homelab-cleanup-detach` | `session detach` | grouped with report 14 by relay session |
| 14 | `homelab-cleanup-teardown` | `session teardown` | explicit keep-jobs default proof |
| 15 | `homelab-transport` | `live-test` | relay and owned SSH transport |
| 16 | `ares-gateway-start` | `gateway start-runtime` | grouped with report 17 by gateway session |
| 17 | `ares-gateway-stop` | `gateway stop-runtime` | keeps scheduler job by default |

Bootstrap of the homelab target and creation of cleanup-owned gateway fixtures
also write diagnostic reports. Store those outside the policy report directory
and do not upload them as part of the 17-report set.

## Start one evidence stage

Use Windows PowerShell 5.1 or newer from the reviewed tag checkout. The persistent `uv tool`
environment is the execution boundary. Do not switch launchers between report
commands.

Before starting either evidence stage, confirm that maintainers have frozen
`main` at the candidate tag commit through finalization. The protected workflows
recheck that exact equality before every persistent mutation; a later merge is
not a harmless documentation change during this window and must not be worked
around by moving the protected tag.

```powershell
$ErrorActionPreference = "Stop"
$Version = "1.3.25"
$Tag = "v$Version"
$Stage = "candidate" # Use "released" for the second complete pass.
if ($Stage -notin @("candidate", "released")) { throw "invalid stage" }

$TagCommit = (git rev-list -n 1 $Tag | Out-String).Trim()
if ($LASTEXITCODE -ne 0 -or -not $TagCommit) { throw "release tag is unavailable: $Tag" }
$CheckoutCommit = (git rev-parse HEAD | Out-String).Trim()
if ($LASTEXITCODE -ne 0 -or $CheckoutCommit -ne $TagCommit) {
  throw "acceptance must run from the exact $Tag commit"
}
$CheckoutChanges = @(git status --porcelain --untracked-files=all)
if ($LASTEXITCODE -ne 0 -or $CheckoutChanges.Count -ne 0) {
  throw "acceptance tag checkout is not clean"
}

$GitHubRepo = "github.com/iowarp/clio-relay"
$Matrix = Get-Content -Raw examples/release-gate/report-matrix-1.0.json | ConvertFrom-Json
$OrderedMatrix = @($Matrix.reports | Sort-Object ordinal)
$PolicyReportPrefix = if ($Stage -eq "candidate") { "validation" } else { "released-validation" }
$ExpectedReleaseAssetNames = @(
  $OrderedMatrix | ForEach-Object { "$PolicyReportPrefix-$($_.id).json" }
)
$StageSealNames = if ($Stage -eq "candidate") {
  @(
    "LIVE-VALIDATION-BINDING.json",
    "candidate-release-gate-1.0.json",
    "PYPI-PROMOTION.json",
    "RELEASED-VALIDATION-BINDING.json",
    "RELEASE-CLAIMS.json"
  )
} else {
  @("RELEASED-VALIDATION-BINDING.json", "RELEASE-CLAIMS.json")
}
$ExistingReleaseAssets = @(
  gh release view $Tag --repo $GitHubRepo --json assets --jq '.assets[].name'
)
if ($LASTEXITCODE -ne 0) { throw "release asset inventory failed" }
$ReleaseAssetSet = [System.Collections.Generic.HashSet[string]]::new(
  [System.StringComparer]::Ordinal
)
foreach ($AssetName in $ExistingReleaseAssets) {
  [void]$ReleaseAssetSet.Add([string]$AssetName)
}
$ObservedStageSeals = @(
  $StageSealNames | Where-Object { $ReleaseAssetSet.Contains([string]$_) }
)
if ($ObservedStageSeals.Count -ne 0) {
  throw "the selected evidence stage is already sealed and must never be replaced: $($ObservedStageSeals -join ', ')"
}
$ConflictingStageAssets = @(
  $ExpectedReleaseAssetNames | Where-Object { $ReleaseAssetSet.Contains([string]$_) }
)
if ($ConflictingStageAssets.Count -ne 0) {
  throw "the selected stage has unsealed policy assets; discard the entire incomplete stage with the documented exact-name recovery procedure before rerunning: $($ConflictingStageAssets -join ', ')"
}

$Uuid = [guid]::NewGuid().ToString("N")
$RunId = "$Stage-$Version-$((Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))-$($Uuid.Substring(0,8))"
$StageRoot = Join-Path ".clio-relay/release-acceptance" $RunId
$ReportRoot = Join-Path $StageRoot "policy-reports"
$DiagnosticRoot = Join-Path $StageRoot "diagnostic-reports"
$RenderedRoot = Join-Path $StageRoot "rendered"
New-Item -ItemType Directory -Force $ReportRoot, $DiagnosticRoot, $RenderedRoot | Out-Null

$OpenSsh = "C:\Windows\System32\OpenSSH\ssh.exe"
$OpenScp = "C:\Windows\System32\OpenSSH\scp.exe"
foreach ($Executable in @($OpenSsh, $OpenScp)) {
  if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
    throw "required Git-for-Windows-independent OpenSSH executable is absent: $Executable"
  }
}
$env:PATH = "$(Split-Path -Parent $OpenSsh);$env:PATH"
$ResolvedSsh = (Get-Command ssh -ErrorAction Stop).Source
if (-not [string]::Equals($ResolvedSsh, $OpenSsh, [StringComparison]::OrdinalIgnoreCase)) {
  throw "clio-relay subprocesses must resolve the audited Windows OpenSSH executable"
}
$AresCluster = "ares"
$HomelabCluster = "homelab"
$RegistryPath = if ($env:CLIO_RELAY_CLUSTER_REGISTRY) {
  $env:CLIO_RELAY_CLUSTER_REGISTRY
} else {
  ".clio-relay/clusters.json"
}
$Registry = Get-Content -Raw $RegistryPath | ConvertFrom-Json
$AresDefinition = $Registry.clusters.$AresCluster
$HomelabDefinition = $Registry.clusters.$HomelabCluster
$AresSshHost = [string]$AresDefinition.ssh_host
$HomelabSshHost = [string]$HomelabDefinition.ssh_host
if ([string]::IsNullOrWhiteSpace($AresSshHost) -or
    [string]::IsNullOrWhiteSpace($HomelabSshHost)) {
  throw "both evidence targets require registry-owned ssh_host values"
}
function Get-RemoteHome {
  param([Parameter(Mandatory)] [string] $SshHost)
  $HomePath = (& $OpenSsh $SshHost 'printf "%s" "$HOME"' | Out-String).Trim()
  if ($LASTEXITCODE -ne 0 -or $HomePath -notmatch '^/[A-Za-z0-9._/-]+$') {
    throw "remote HOME is unavailable or unsafe for $SshHost"
  }
  $Segments = @($HomePath.Split('/', [StringSplitOptions]::RemoveEmptyEntries))
  if ($Segments.Count -eq 0 -or $Segments -contains '..') {
    throw "remote HOME is unavailable or unsafe for $SshHost"
  }
  $HomePath
}
$AresHome = Get-RemoteHome $AresSshHost
$HomelabRemoteHome = Get-RemoteHome $HomelabSshHost
$AresRemoteRoot = "$AresHome/.local/share/clio-relay/release-acceptance/$RunId"
$HomelabRemoteRoot = "$HomelabRemoteHome/.local/share/clio-relay/release-acceptance/$RunId"
$AresFixtureRoot = "$AresRemoteRoot/fixtures"
$HomelabFixtureRoot = "$HomelabRemoteRoot/fixtures"
$AresStateRoot = "$AresRemoteRoot/runtime-state"
$HomelabStateRoot = "$HomelabRemoteRoot/runtime-state"

foreach ($Target in @(
  @{ Host = $AresSshHost; Root = $AresRemoteRoot },
  @{ Host = $HomelabSshHost; Root = $HomelabRemoteRoot }
)) {
  & $OpenSsh $Target.Host "install -d -m 700 '$($Target.Root)'"
  if ($LASTEXITCODE -ne 0) { throw "remote acceptance root creation failed" }
}

$Producer = gh api --hostname github.com user | ConvertFrom-Json
if (-not $Producer.login -or [int64]$Producer.id -le 0) { throw "GitHub producer identity unavailable" }
$env:CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN = $Producer.login
$env:CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID = [string]$Producer.id
$env:CLIO_RELAY_VALIDATION_LAUNCHER = "uv-tool"
$env:UV = (Get-Command uv -ErrorAction Stop).Source
if ((uv --version) -notmatch '^uv 0\.11\.28(?:\s|$)') { throw "release policy requires uv 0.11.28" }
```

Resolve the candidate wheel from the draft assets and independently compare it
with the staged manifest before installing it:

```powershell
$AllowedUvLocationEnvironment = @("UV_CACHE_DIR", "UV_TOOL_DIR", "UV_TOOL_BIN_DIR")
foreach ($Variable in @(Get-ChildItem Env: | Where-Object {
  $_.Name -like "UV_*" -and $_.Name -notin $AllowedUvLocationEnvironment
})) {
  Remove-Item "Env:$($Variable.Name)" -ErrorAction Stop
}
if ($Stage -eq "candidate") {
  $CandidateRoot = Join-Path $StageRoot "candidate-artifact"
  New-Item -ItemType Directory -Force $CandidateRoot | Out-Null
  gh release download $Tag --repo $GitHubRepo `
    --pattern "clio_relay-$Version-py3-none-any.whl" --pattern "SHA256SUMS" `
    --dir $CandidateRoot
  if ($LASTEXITCODE -ne 0) { throw "candidate artifact download failed" }
  $Wheels = @(Get-ChildItem -LiteralPath $CandidateRoot -Filter "*.whl" -File)
  $Manifests = @(Get-ChildItem -LiteralPath $CandidateRoot -Filter "SHA256SUMS" -File)
  if ($Wheels.Count -ne 1 -or $Manifests.Count -ne 1) {
    throw "candidate release must contain exactly one selected wheel and one manifest"
  }
  $Wheel = $Wheels[0].FullName
  $WheelName = $Wheels[0].Name
  $ManifestLines = @(
    Get-Content -LiteralPath $Manifests[0].FullName |
      Where-Object { $_ -match "^[0-9A-Fa-f]{64} [ *]$([Regex]::Escape($WheelName))$" }
  )
  if ($ManifestLines.Count -ne 1) { throw "candidate manifest entry is missing or ambiguous" }
  if ($ManifestLines[0] -notmatch '^([0-9A-Fa-f]{64}) [ *](.+)$' -or $Matches[2] -ne $WheelName) {
    throw "candidate manifest entry is malformed"
  }
  $ExpectedWheelSha256 = $Matches[1].ToLowerInvariant()
  $Observed = (Get-FileHash -Algorithm SHA256 $Wheel).Hash.ToLowerInvariant()
  if ($Observed -ne $ExpectedWheelSha256) { throw "candidate wheel digest mismatch" }
  gh attestation verify $Wheel --hostname github.com --repo iowarp/clio-relay `
    --signer-workflow iowarp/clio-relay/.github/workflows/stage-candidate.yml `
    --source-ref refs/heads/main --source-digest $TagCommit --deny-self-hosted-runners
  if ($LASTEXITCODE -ne 0) { throw "candidate build attestation verification failed" }
  uv tool install --force --python 3.12 --no-config `
    --default-index https://pypi.org/simple $Wheel
  if ($LASTEXITCODE -ne 0) { throw "candidate uv tool installation failed" }
  $InstallSource = "wheel:$([Uri]::new($Wheel).AbsoluteUri)"
  $ArtifactEvidence = @("--validation-artifact", $Wheel)
  $BootstrapArtifact = @("--relay-wheel", $Wheel)
  $env:CLIO_RELAY_VALIDATION_ARTIFACT_SHA256 = $Observed
} else {
  $PublishedRoot = Join-Path $StageRoot "published-artifact"
  New-Item -ItemType Directory -Force $PublishedRoot | Out-Null
  gh release download $Tag --repo $GitHubRepo --pattern "PYPI-PROMOTION.json" `
    --dir $PublishedRoot
  if ($LASTEXITCODE -ne 0) { throw "promotion record download failed" }
  $PromotionFiles = @(Get-ChildItem -LiteralPath $PublishedRoot -Filter "PYPI-PROMOTION.json" -File)
  if ($PromotionFiles.Count -ne 1) { throw "release must contain exactly one promotion record" }
  $PromotionPath = $PromotionFiles[0].FullName
  $Promotion = Get-Content -Raw $PromotionPath | ConvertFrom-Json
  if ([string]$Promotion.version -cne $Version) { throw "promotion version mismatch" }
  if ([string]$Promotion.wheel_sha256 -cnotmatch '^[0-9a-f]{64}$') {
    throw "promotion wheel digest is missing or malformed"
  }
  uv tool install --force --refresh --no-config `
    --default-index https://pypi.org/simple `
    --python 3.12 "clio-relay==$Version"
  if ($LASTEXITCODE -ne 0) { throw "released uv tool installation failed" }
  $InstallSource = "pypi:clio-relay==$Version"
  $ArtifactEvidence = @()
  $BootstrapArtifact = @()
  $env:CLIO_RELAY_VALIDATION_ARTIFACT_SHA256 = $Promotion.wheel_sha256
}
$Relay = (Get-Command clio-relay -ErrorAction Stop).Source
$env:CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE = $Relay
$Evidence = @("--validation-launcher", "uv-tool", "--validation-install-source", $InstallSource)
```

The producer login and numeric GitHub id are resolved afresh for the stage but
remain the real operator identity. The invocation id, report id, report path,
pipeline id, relay-session id, and gateway-session id must be fresh for every
operation. This helper guarantees a new invocation id, refuses report-path
replacement, and tracks only the 17 policy reports:

```powershell
$PolicyReports = [System.Collections.Generic.List[string]]::new()
function Invoke-RelayReport {
  param(
    [Parameter(Mandatory)] [string] $Id,
    [Parameter(Mandatory)] [string[]] $Command,
    [Parameter(Mandatory)] [string] $ReportOption,
    [switch] $Diagnostic,
    [switch] $NoArtifactOption
  )
  $Directory = if ($Diagnostic) { $DiagnosticRoot } else { $ReportRoot }
  $Path = Join-Path $Directory "$PolicyReportPrefix-$Id.json"
  if (Test-Path -LiteralPath $Path) { throw "refusing to replace $Path" }
  $env:CLIO_RELAY_VALIDATION_INVOCATION_ID = "$RunId-$Id-$([guid]::NewGuid().ToString('N'))"
  $FullCommand = @($Command) + @($ReportOption, $Path) + $Evidence
  if (-not $NoArtifactOption) { $FullCommand += $ArtifactEvidence }
  $Output = & $Relay @FullCommand
  $ExitCode = $LASTEXITCODE
  $Output | ForEach-Object { Write-Host $_ }
  if ($ExitCode -ne 0) { throw "clio-relay failed for $Id with exit code $ExitCode" }
  if (-not (Test-Path -LiteralPath $Path)) { throw "missing report $Path" }
  $Report = Get-Content -Raw $Path | ConvertFrom-Json
  if ($Report.status -ne "passed") { throw "report did not pass: $Path" }
  if (-not $Diagnostic) { $PolicyReports.Add((Resolve-Path $Path).Path) }
  [pscustomobject]@{ Path = (Resolve-Path $Path).Path; Output = ($Output -join "`n"); Report = $Report }
}

function ConvertTo-LfText {
  param(
    [Parameter(Mandatory)] [AllowEmptyString()] [string] $Text,
    [Parameter(Mandatory)] [string] $Description
  )
  if ($Text.Length -gt 0 -and [int]$Text[0] -eq 0xFEFF) {
    $Text = $Text.Substring(1)
  }
  if ($Text.IndexOf([char]0) -ge 0) {
    throw "$Description contains a NUL byte"
  }
  $Text = $Text.Replace("`r`n", "`n").Replace("`r", "`n")
  if (-not $Text.EndsWith("`n", [StringComparison]::Ordinal)) {
    $Text += "`n"
  }
  $Text
}

function Read-LfUtf8TextFile {
  param([Parameter(Mandatory)] [string] $Source)
  $ResolvedSource = (Resolve-Path -LiteralPath $Source -ErrorAction Stop).Path
  if (-not (Test-Path -LiteralPath $ResolvedSource -PathType Leaf)) {
    throw "text staging source is not a file: $Source"
  }
  $StrictUtf8 = [Text.UTF8Encoding]::new($false, $true)
  try {
    $Text = $StrictUtf8.GetString([IO.File]::ReadAllBytes($ResolvedSource))
  } catch {
    throw "text staging source is not valid UTF-8: $Source"
  }
  ConvertTo-LfText $Text "text staging source $Source"
}

function Render-Template {
  param(
    [Parameter(Mandatory)] [string] $Source,
    [Parameter(Mandatory)] [string] $Destination,
    [Parameter(Mandatory)] [hashtable] $Values
  )
  $Text = Read-LfUtf8TextFile $Source
  foreach ($Key in $Values.Keys) { $Text = $Text.Replace("__${Key}__", [string]$Values[$Key]) }
  if ($Text -match '__[A-Z_]+__') { throw "unresolved template token in $Destination" }
  $Text = ConvertTo-LfText $Text "rendered template $Destination"
  $Parent = Split-Path -Parent $Destination
  New-Item -ItemType Directory -Force $Parent | Out-Null
  [IO.File]::WriteAllText((Join-Path (Resolve-Path $Parent) (Split-Path -Leaf $Destination)), $Text, [Text.UTF8Encoding]::new($false))
}

function New-LfTextStagingCopy {
  param(
    [Parameter(Mandatory)] [string] $Source,
    [Parameter(Mandatory)] [string] $StagingName
  )
  if ($StagingName -cnotmatch '^[A-Za-z0-9._-]+$') {
    throw "text staging name is unsafe: $StagingName"
  }
  $Text = Read-LfUtf8TextFile $Source
  $StagingDirectory = Join-Path $RenderedRoot "remote-text"
  New-Item -ItemType Directory -Force $StagingDirectory | Out-Null
  $StagingPath = Join-Path $StagingDirectory $StagingName
  if (Test-Path -LiteralPath $StagingPath) {
    throw "refusing to replace normalized text staging copy: $StagingPath"
  }
  [IO.File]::WriteAllText($StagingPath, $Text, [Text.UTF8Encoding]::new($false))
  $StagedBytes = [IO.File]::ReadAllBytes($StagingPath)
  $HasUtf8Bom = (
    $StagedBytes.Length -ge 3 -and
    $StagedBytes[0] -eq 0xEF -and
    $StagedBytes[1] -eq 0xBB -and
    $StagedBytes[2] -eq 0xBF
  )
  if ($HasUtf8Bom -or
      $StagedBytes -contains [byte]0x0D -or
      $StagedBytes -contains [byte]0x00) {
    throw "normalized text staging copy is not LF-only, NUL-free UTF-8: $StagingPath"
  }
  $StagingPath
}

function Copy-RemoteTextFile {
  param(
    [Parameter(Mandatory)] [string] $Source,
    [Parameter(Mandatory)] [string] $StagingName,
    [Parameter(Mandatory)] [string] $SshHost,
    [Parameter(Mandatory)] [string] $RemotePath,
    [Parameter(Mandatory)] [string] $Description
  )
  $StagingPath = New-LfTextStagingCopy $Source $StagingName
  & $OpenScp $StagingPath "${SshHost}:$RemotePath" | Out-Host
  if ($LASTEXITCODE -ne 0) { throw "$Description staging failed" }
}
```

Candidate and released runs must use different `$RunId` values and different
stage roots. Never copy a candidate path, invocation id, report id, pipeline id,
relay-session id, or gateway-session id into the released pass.

## Verify target configuration

The local registry remains operator-owned state and is not a release asset. Its
1.0 evidence entries must explicitly select `slurm` for Ares and `external` for
homelab. Ares must also declare the site Spack executable. Do not recreate an
existing entry with a partial `cluster add` command because that command replaces
the whole definition.

```powershell
$AresSpack = [string]$AresDefinition.spack_executable
$AresFreshBaseSpack = "$AresHome/spack/bin/spack"
if ($AresDefinition.scheduler_provider -ne "slurm") { throw "Ares must explicitly use slurm" }
if ($AresSpack -notmatch '^/[A-Za-z0-9._+/-]+$' -or
    $AresSpack.StartsWith("//") -or
    $AresSpack.EndsWith("/") -or
    $AresSpack -match '(^|/)\.\.(/|$)') {
  throw "Ares spack_executable must be one canonical absolute path"
}
foreach ($Executable in @($AresSpack, $AresFreshBaseSpack)) {
  & $OpenSsh $AresSshHost "test -x '$Executable'"
  if ($LASTEXITCODE -ne 0) { throw "required Ares Spack executable is absent: $Executable" }
}
if ($HomelabDefinition.scheduler_provider -ne "external") { throw "homelab must explicitly use external" }
foreach ($Name in @(
  $AresDefinition.frp_transport.token_env,
  $AresDefinition.frp_transport.stcp_secret_env,
  $HomelabDefinition.frp_transport.token_env,
  $HomelabDefinition.frp_transport.stcp_secret_env,
  "CLIO_RELAY_API_TOKEN"
)) {
  if ([string]::IsNullOrWhiteSpace([string]$Name)) {
    throw "cluster registry contains a blank secret environment-variable name"
  }
  if (-not (Test-Path "Env:$Name")) { throw "required secret environment variable is absent: $Name" }
}
```

The selected Gray-Scott executable is built inside this acceptance run from the
latest reviewed `clio-core` development commit that contains the direct
`external/iowarp-gray-scott` application. It is not the Coeus adapter and it
must not resolve Hermes or `adios2-coeus`. The source commit, embedded
Gray-Scott tree, exact plain-ADIOS2 DAG hash, build directory, and install
directory are all bound below. The build helper resolves the selected hash's
own install prefix, requires exactly one ADIOS2 CMake package configuration
under that prefix, and passes its directory explicitly as `ADIOS2_DIR`;
`spack build-env` alone exposes dependencies but not the selected package's own
installation prefix.

Before invoking this block, set `CLIO_RELAY_ACCEPTANCE_ADIOS_HASH` to the
reviewed 32-character DAG hash of the plain `adios2` installation selected for
this acceptance run. A site may retain multiple valid ADIOS2 variants; the
gate binds one explicit installation and never selects a variant by inventory
order or by assuming the site has only one.

```powershell
$ExpectedCoreCommit = "e2fedd8847f8deb71f041f692e405023a712ca44"
$ExpectedGrayTree = "072d6eab3df3bde92e48ae2f4823305af831535e"
$AresCoreRoot = "$AresRemoteRoot/clio-core-$($ExpectedCoreCommit.Substring(0, 12))"
$GrayBuildRoot = "$AresRemoteRoot/build-gray-scott"
$GrayInstallRoot = "$AresRemoteRoot/install-gray-scott"
$GrayExecutable = "$GrayInstallRoot/bin/gray-scott"
$ExpectedLammpsHash = "p5gjmq4rseitqanua7mdd2zdnag4v3u2"
$ExpectedAdiosHash = [string]$env:CLIO_RELAY_ACCEPTANCE_ADIOS_HASH
if ($ExpectedAdiosHash -cnotmatch '^[a-z0-9]{32}$') {
  throw "CLIO_RELAY_ACCEPTANCE_ADIOS_HASH must be one exact lowercase DAG hash"
}

function Get-SpackRecordHash {
  param([Parameter(Mandatory)] $Record)
  foreach ($Property in @("hash", "full_hash", "dag_hash")) {
    $Candidate = $Record.PSObject.Properties[$Property]
    if ($null -eq $Candidate) { continue }
    $Value = [string]$Candidate.Value
    if (-not [string]::IsNullOrWhiteSpace($Value)) { return $Value }
  }
  throw "Spack package record omitted its DAG hash"
}

$AdiosJson = (
  & $OpenSsh $AresSshHost "'$AresSpack' find --json '/$ExpectedAdiosHash'" | Out-String
)
if ($LASTEXITCODE -ne 0) { throw "selected plain ADIOS2 lookup failed" }
$AdiosDecoded = $AdiosJson | ConvertFrom-Json
$AdiosSpecs = $AdiosDecoded.PSObject.Properties["specs"]
$AdiosRecords = @(
  if ($null -ne $AdiosSpecs) { $AdiosSpecs.Value } else { $AdiosDecoded }
)
$PlainAdios = @($AdiosRecords | Where-Object { [string]$_.name -ceq "adios2" })
if ($PlainAdios.Count -ne 1 -or
    (Get-SpackRecordHash $PlainAdios[0]) -cne $ExpectedAdiosHash) {
  throw "selected exact plain ADIOS2 installation is absent or ambiguous"
}
$AdiosPrefix = (
  & $OpenSsh $AresSshHost "'$AresSpack' location -i '/$ExpectedAdiosHash'" | Out-String
).Trim()
if ($LASTEXITCODE -ne 0 -or $AdiosPrefix -notmatch '^/[A-Za-z0-9._+/-]+$') {
  throw "selected plain ADIOS2 prefix is unavailable"
}

Copy-RemoteTextFile `
  -Source "examples/release-gate/gray-scott-direct-build.sh" `
  -StagingName "gray-scott-direct-build.sh" `
  -SshHost $AresSshHost `
  -RemotePath "$AresRemoteRoot/gray-scott-direct-build.sh" `
  -Description "Gray-Scott build script"
& $OpenSsh $AresSshHost "chmod 700 '$AresRemoteRoot/gray-scott-direct-build.sh' && '$AresRemoteRoot/gray-scott-direct-build.sh' '$AresSpack' '$AresCoreRoot' '$GrayBuildRoot' '$GrayInstallRoot' '$ExpectedCoreCommit' '$ExpectedGrayTree' '$ExpectedAdiosHash'"
if ($LASTEXITCODE -ne 0) { throw "direct IOWarp Gray-Scott build failed" }
if ((& $OpenSsh $AresSshHost "git -C '$AresCoreRoot' rev-parse HEAD").Trim() -ne $ExpectedCoreCommit) {
  throw "unexpected clio-core commit"
}
if ((& $OpenSsh $AresSshHost "git -C '$AresCoreRoot' rev-parse HEAD:external/iowarp-gray-scott").Trim() -ne $ExpectedGrayTree) {
  throw "unexpected embedded Gray-Scott tree"
}
& $OpenSsh $AresSshHost "test -x '$GrayExecutable'"
if ($LASTEXITCODE -ne 0) { throw "acceptance-built Gray-Scott executable is absent" }
$LammpsHashes = @(
  & $OpenSsh $AresSshHost "'$AresSpack' find --format '{hash}' lammps" |
    ForEach-Object { $_.Trim() } | Where-Object { $_ }
)
if ($LammpsHashes.Count -ne 1 -or $LammpsHashes[0] -ne $ExpectedLammpsHash) {
  throw "expected unique LAMMPS installation is absent or ambiguous"
}
$ExpectedLammpsPrefix = (
  & $OpenSsh $AresSshHost "'$AresSpack' location -i '/$ExpectedLammpsHash'" | Out-String
).Trim()
if ($LASTEXITCODE -ne 0 -or
    $ExpectedLammpsPrefix -notmatch '^/[A-Za-z0-9._+/-]+$' -or
    $ExpectedLammpsPrefix.StartsWith("//") -or
    $ExpectedLammpsPrefix -match '(^|/)\.\.(/|$)') {
  throw "expected exact LAMMPS prefix is unavailable or non-canonical"
}
$Linked = & $OpenSsh $AresSshHost "ldd '$GrayExecutable'"
if ($LASTEXITCODE -ne 0 -or $Linked -match 'not found') { throw "Gray-Scott runtime linkage failed" }
$AdiosLibraries = @(
  $Linked | ForEach-Object {
    if ($_ -match 'libadios2\S*\s+=>\s+(\S+)') { $Matches[1] }
  }
)
if ($AdiosLibraries.Count -eq 0 -or
    @($AdiosLibraries | Where-Object { -not $_.StartsWith("$AdiosPrefix/") }).Count -ne 0) {
  throw "Gray-Scott does not resolve every ADIOS2 library from the selected Spack prefix"
}
$MpiLibraries = @(
  $Linked | ForEach-Object {
    if ($_ -match 'libmpi(?:_\S+)?\.so\S*\s+=>\s+(\S+)') { $Matches[1] }
  } | Sort-Object -Unique
)
if ($MpiLibraries.Count -ne 1) {
  throw "Gray-Scott must resolve exactly one MPI runtime"
}
if (($Linked | Out-String) -match '(?i)coeus|hermes') {
  throw "direct Gray-Scott acceptance binary unexpectedly links Coeus or Hermes"
}
```

The Ares LAMMPS Spack hash is verified above. JARVIS receives canonical
`/$ExpectedLammpsHash` and `/$ExpectedAdiosHash` load specs and owns environment
materialization, persistence, and scheduler reload semantics. The relay does not
expose a `spack_load` agent tool.

## Deploy the exact artifact and run the queue proof

Report 1 is the Ares bootstrap report. Homelab bootstrap is deployment
preparation and its report is diagnostic. Both persistent worker services must
then be reinstalled and restarted with at least three slots and a JARVIS cap of
two. Before this run, `loginctl show-user "$USER" -p Linger --value` must return
`yes` on each target. When site policy permits, configure that once with
`loginctl enable-linger "$USER"`; otherwise ask the site administrator. The
login-scoped installer opt-out is diagnostic and cannot satisfy this gate.

```powershell
$Bootstrap = Invoke-RelayReport -Id "ares-bootstrap" -NoArtifactOption `
  -ReportOption "--report" `
  -Command (@("cluster", "bootstrap", "--cluster", $AresCluster) + $BootstrapArtifact)

$HomelabBootstrap = Invoke-RelayReport -Id "homelab-bootstrap-preparation" -Diagnostic -NoArtifactOption `
  -ReportOption "--report" `
  -Command (@("cluster", "bootstrap", "--cluster", $HomelabCluster) + $BootstrapArtifact)

foreach ($Cluster in @($AresCluster, $HomelabCluster)) {
  & $Relay cluster install-endpoint-service --cluster $Cluster --concurrency 3 `
    --kind-concurrency jarvis=2 --kind-concurrency remote_agent=2 `
    --kind-concurrency mcp_call=1 --start --enable --require-persistent
  if ($LASTEXITCODE -ne 0) { throw "worker service installation failed: $Cluster" }
}

$BootstrapYaml = Join-Path $RenderedRoot "ares-bootstrap.yaml"
Render-Template "examples/release-gate/ares-bootstrap-echo.yaml.tmpl" $BootstrapYaml @{
  RUN_ID = $RunId
  REMOTE_ROOT = $AresRemoteRoot
}
Invoke-RelayReport -Id "ares-cluster-bootstrap-live-test" -ReportOption "--report" -Command @(
  "live-test", "--cluster", $AresCluster,
  "--validation-scenario", "cluster-bootstrap",
  "--jarvis-yaml", $BootstrapYaml,
  "--verify-cluster-deployment",
  "--require-structured-runtime-metadata",
  "--timeout-seconds", "600", "--poll-seconds", "1"
)

Invoke-RelayReport -Id "ares-queue-management" -ReportOption "--report" -Command @(
  "queue", "validate", "--cluster", $AresCluster, "--kind", "jarvis",
  "--older-than", "1m", "--scheduler-provider", "slurm",
  "--scheduler-run-seconds", "30", "--scheduler-timeout-seconds", "180",
  "--scheduler-poll-seconds", "1"
)
```

Do not stage cleanup, gateway, or transport fixtures before the queue report has
passed. This ensures the queue validator measures the configured worker rather
than state left by later acceptance fixtures.

## Stage non-secret fixtures

Render only into the ignored stage directory. Stage code and input files after
the queue report; never stage a token, shared secret, local registry, cached MCP
schema, or prior report.

```powershell
$PortSeed = Get-Random -Minimum 1000 -Maximum 15000
$AresServicePort = 19000 + $PortSeed
$AresDesktopPort = 29000 + $PortSeed
$AresDedicatedServicePort = 19500 + $PortSeed
$AresDedicatedDesktopPort = 29500 + $PortSeed
$HomelabServicePort = 20000 + $PortSeed
$HomelabDesktopPort = 30000 + $PortSeed
$AresDefaultApiPort = 18000 + $PortSeed
$AresDefaultLocalPort = 28000 + $PortSeed
$CancelApiPort = 18750 + $PortSeed
$CancelLocalPort = 28750 + $PortSeed
$HomelabDefaultApiPort = 18500 + $PortSeed
$HomelabDefaultLocalPort = 28500 + $PortSeed
$HomelabTransportLocalPort = 31000 + $PortSeed
$HomelabTransportRemotePort = 21000 + $PortSeed
$HomelabSshTransportLocalPort = 32000 + $PortSeed
$HomelabSshTransportRemotePort = 22000 + $PortSeed
$AresDefaultHealthNonce = "$([guid]::NewGuid().ToString('N'))$([guid]::NewGuid().ToString('N'))"
$AresDedicatedHealthNonce = "$([guid]::NewGuid().ToString('N'))$([guid]::NewGuid().ToString('N'))"
$HomelabHealthNonce = "$([guid]::NewGuid().ToString('N'))$([guid]::NewGuid().ToString('N'))"
function Assert-LocalPortAvailable {
  param([Parameter(Mandatory)] [int] $Port)
  $Listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $Port)
  try { $Listener.Start() }
  catch { throw "desktop acceptance port is already occupied: $Port" }
  finally { $Listener.Stop() }
}
foreach ($Port in @(
  $AresDesktopPort, $AresDedicatedDesktopPort, $HomelabDesktopPort,
  $AresDefaultLocalPort, $CancelLocalPort, $HomelabDefaultLocalPort,
  $HomelabTransportLocalPort, $HomelabSshTransportLocalPort
)) {
  Assert-LocalPortAvailable $Port
}

foreach ($Target in @(
  @{ Name = $AresCluster; Host = $AresSshHost; Root = $AresFixtureRoot; State = $AresStateRoot },
  @{ Name = $HomelabCluster; Host = $HomelabSshHost; Root = $HomelabFixtureRoot; State = $HomelabStateRoot }
)) {
  & $OpenSsh $Target.Host "install -d -m 700 '$($Target.Root)/gateway' '$($Target.State)'"
  if ($LASTEXITCODE -ne 0) { throw "remote fixture directory creation failed" }
  Copy-RemoteTextFile `
    -Source "examples/release-gate/gateway/http_service.py" `
    -StagingName "$($Target.Name)-gateway-http-service.py" `
    -SshHost $Target.Host `
    -RemotePath "$($Target.Root)/gateway/http_service.py" `
    -Description "gateway HTTP fixture"
  Copy-RemoteTextFile `
    -Source "examples/release-gate/gateway/external_runtime.py" `
    -StagingName "$($Target.Name)-gateway-external-runtime.py" `
    -SshHost $Target.Host `
    -RemotePath "$($Target.Root)/gateway/external_runtime.py" `
    -Description "gateway external-runtime fixture"
}
Copy-RemoteTextFile `
  -Source "examples/release-gate/gateway/slurm_status.py" `
  -StagingName "gateway-slurm-status.py" `
  -SshHost $AresSshHost `
  -RemotePath "$AresFixtureRoot/gateway/slurm_status.py" `
  -Description "SLURM gateway status fixture"
Copy-RemoteTextFile `
  -Source "examples/release-gate/gateway/slurm_cancel.py" `
  -StagingName "gateway-slurm-cancel.py" `
  -SshHost $AresSshHost `
  -RemotePath "$AresFixtureRoot/gateway/slurm_cancel.py" `
  -Description "SLURM gateway cancel fixture"
Copy-RemoteTextFile `
  -Source "examples/release-gate/gateway/slurm_submit.sh" `
  -StagingName "gateway-slurm-submit.sh" `
  -SshHost $AresSshHost `
  -RemotePath "$AresFixtureRoot/gateway/slurm_submit.sh" `
  -Description "SLURM gateway shell fixture"

$AresRuntime = Join-Path $RenderedRoot "ares-runtime.json"
Render-Template "examples/release-gate/gateway/ares-runtime.json.tmpl" $AresRuntime @{
  RUN_ID = "$RunId-ares-default"
  REMOTE_FIXTURE_ROOT = $AresFixtureRoot
  REMOTE_STATE_ROOT = $AresStateRoot
  SERVICE_PORT = $AresServicePort
  DESKTOP_PORT = $AresDesktopPort
  HEALTH_NONCE = $AresDefaultHealthNonce
}
$AresDedicatedRuntime = Join-Path $RenderedRoot "ares-dedicated-runtime.json"
Render-Template "examples/release-gate/gateway/ares-runtime.json.tmpl" $AresDedicatedRuntime @{
  RUN_ID = "$RunId-ares-dedicated"
  REMOTE_FIXTURE_ROOT = $AresFixtureRoot
  REMOTE_STATE_ROOT = $AresStateRoot
  SERVICE_PORT = $AresDedicatedServicePort
  DESKTOP_PORT = $AresDedicatedDesktopPort
  HEALTH_NONCE = $AresDedicatedHealthNonce
}
$HomelabRuntime = Join-Path $RenderedRoot "homelab-runtime.json"
Render-Template "examples/release-gate/gateway/homelab-runtime.json.tmpl" $HomelabRuntime @{
  RUN_ID = "$RunId-homelab-default"
  REMOTE_FIXTURE_ROOT = $HomelabFixtureRoot
  REMOTE_STATE_ROOT = $HomelabStateRoot
  SERVICE_PORT = $HomelabServicePort
  DESKTOP_PORT = $HomelabDesktopPort
  HEALTH_NONCE = $HomelabHealthNonce
}
```

## Run the two JARVIS application reports

Set up each fresh pipeline through the same durable JARVIS MCP path, wait for
the setup jobs, and then let `jarvis-mcp-validate` run and query it. The two
applications produce two independent reports.

```powershell
function Write-JsonFile {
  param([string] $Name, [object] $Value)
  $Path = Join-Path $RenderedRoot $Name
  [IO.File]::WriteAllText(
    $Path,
    ($Value | ConvertTo-Json -Depth 20),
    [Text.UTF8Encoding]::new($false)
  )
  $Path
}

function Invoke-JarvisSetupTool {
  param([string] $Tool, [string] $Name, [object] $Arguments)
  $Path = Write-JsonFile "$Name.json" $Arguments
  $JobId = (& $Relay jarvis-mcp-call --cluster $AresCluster --tool $Tool `
    --arguments-json-file $Path --idempotency-key "$RunId-$Name-$([guid]::NewGuid().ToString('N'))" `
    --timeout-seconds 900 | Out-String).Trim()
  if ($LASTEXITCODE -ne 0 -or -not $JobId) { throw "JARVIS setup submission failed: $Name" }
  & $Relay job wait $JobId --cluster $AresCluster --timeout-seconds 900 --poll-seconds 1 | Out-Host
  if ($LASTEXITCODE -ne 0) { throw "JARVIS setup job failed: $Name" }
}

$GrayPipeline = "$RunId-gray-scott"
$GrayOutput = "$AresRemoteRoot/gray-scott/output.bp"
$GrayCheckpoint = "$AresRemoteRoot/gray-scott/checkpoint.bp"
Invoke-JarvisSetupTool "jarvis_create_pipeline" "gray-create" @{
  pipeline_id = $GrayPipeline
}
Invoke-JarvisSetupTool "jarvis_add_step" "gray-add" @{
  pipeline_id = $GrayPipeline
  package_name = "builtin.gray_scott"
  step_id = "gray_scott_bp5"
  do_configure = $true
  config = @{
    deploy_mode = "default"; nprocs = 1; ppn = 1
    executable = $GrayExecutable; width = 32; height = 32; steps = 20; out_every = 10
    outdir = $GrayOutput; checkpoint = $true; checkpoint_freq = 1
    checkpoint_output = $GrayCheckpoint; adios_span = $false
    adios_memory_selection = $false; mesh_type = "image"
  }
}
$GrayRun = Write-JsonFile "gray-run.json" @{
  pipeline_id = $GrayPipeline; submit = $true; wait = $true
  spack_specs = @("/$ExpectedAdiosHash")
  execution = @{
    mode = "scheduler"; partition = "compute"; nodes = 1; tasks = 1; tasks_per_node = 1
    walltime = "00:10:00"; job_name = "$RunId-gray"
    output = "$AresRemoteRoot/gray-%j.out"; error = "$AresRemoteRoot/gray-%j.err"
  }
}
Invoke-RelayReport -Id "ares-jarvis-gray-scott" -ReportOption "--report" -Command @(
  "jarvis-mcp-validate", "--cluster", $AresCluster, "--arguments-json-file", $GrayRun,
  "--package-search-query", "gray scott",
  "--wait-timeout-seconds", "900", "--poll-seconds", "0.05"
)

$RemoteLammpsInput = "$AresRemoteRoot/lammps/in.lj"
& $OpenSsh $AresSshHost "install -d -m 700 '$AresRemoteRoot/lammps'"
Copy-RemoteTextFile `
  -Source "examples/release-gate/lammps-bounded.in" `
  -StagingName "lammps-bounded.in" `
  -SshHost $AresSshHost `
  -RemotePath $RemoteLammpsInput `
  -Description "LAMMPS input"
$LammpsPipeline = "$RunId-lammps"
Invoke-JarvisSetupTool "jarvis_create_pipeline" "lammps-create" @{
  pipeline_id = $LammpsPipeline
}
Invoke-JarvisSetupTool "jarvis_add_step" "lammps-add" @{
  pipeline_id = $LammpsPipeline
  package_name = "builtin.lammps"
  step_id = "lammps"
  do_configure = $true
  config = @{
    deploy_mode = "default"; nprocs = 1; ppn = 1; script = $RemoteLammpsInput
    lmp_bin = "lmp -nonbuf"; out = "$AresRemoteRoot/lammps/out"; kokkos_gpu = $false
    progress = @{ adapter = "lammps"; log_visibility = "shared"; total_steps = 10000 }
  }
}
$LammpsRun = Write-JsonFile "lammps-run.json" @{
  pipeline_id = $LammpsPipeline; submit = $true; wait = $true
  spack_specs = @("/$ExpectedLammpsHash")
  execution = @{
    mode = "scheduler"; partition = "compute"; nodes = 1; tasks = 1; tasks_per_node = 1
    walltime = "00:10:00"; job_name = "$RunId-lammps"
    output = "$AresRemoteRoot/lammps-%j.out"; error = "$AresRemoteRoot/lammps-%j.err"
  }
}
Invoke-RelayReport -Id "ares-jarvis-lammps" -ReportOption "--report" -Command @(
  "jarvis-mcp-validate", "--cluster", $AresCluster, "--arguments-json-file", $LammpsRun,
  "--package-search-query", "lammps",
  "--wait-timeout-seconds", "900", "--poll-seconds", "0.05"
)
```

The Gray report must contain JARVIS-native progress for package id
`gray_scott_bp5` and a finalized `gray-scott-timesteps` ADIOS2 BP5 collection
with latest timestep 20 and two observed members. The LAMMPS report must contain
determinate JARVIS-native progress for package id `lammps`. Stdout parsing alone
cannot satisfy either requirement.

## Run the three-tool Spack contract

Register exactly the three agent-facing user operations. The `--arg=...` form is
required for child arguments beginning with `--`.

```powershell
$AresClioKit = "$AresHome/.local/bin/clio-kit"
& $Relay remote-mcp register --cluster $AresCluster --name spack `
  --command $AresClioKit `
  --arg=mcp-server --arg=spack --arg=-- --arg=--spack-command --arg=$AresSpack `
  --contract clio-kit-spack-user-v2 `
  --allow-tool spack_find --allow-tool spack_locate --allow-tool spack_install `
  --profile user --call-timeout-seconds 14400 --replace
if ($LASTEXITCODE -ne 0) { throw "Spack MCP registration failed" }
& $Relay remote-mcp refresh --cluster $AresCluster --name spack
if ($LASTEXITCODE -ne 0) { throw "Spack MCP discovery failed" }

$SpackCalls = @(
  @{
    Id = "ares-spack-find"; Tool = "spack_find"; Arguments = @{ query = "lammps" }
    Expectation = @{
      contract = "clio-kit-spack-user-v2"; tool = "spack_find"
      package_name = "lammps"; dag_hash = $ExpectedLammpsHash
    }
  },
  @{
    Id = "ares-spack-locate"; Tool = "spack_locate"; Arguments = @{ spec = "lammps" }
    Expectation = @{
      contract = "clio-kit-spack-user-v2"; tool = "spack_locate"
      package_name = "lammps"; dag_hash = $ExpectedLammpsHash
      requested_spec = "lammps"; prefix = $ExpectedLammpsPrefix
    }
  }
)
foreach ($Call in $SpackCalls) {
  $Arguments = Write-JsonFile "$($Call.Id).json" $Call.Arguments
  $Expectation = Write-JsonFile "$($Call.Id)-expectation.json" $Call.Expectation
  Invoke-RelayReport -Id $Call.Id -ReportOption "--validation-report" -Command @(
    "remote-mcp", "validate", "--cluster", $AresCluster, "--name", "spack",
    "--tool", $Call.Tool, "--arguments-json-file", $Arguments,
    "--result-expectation-json-file", $Expectation,
    "--wait-timeout-seconds", "14400", "--poll-seconds", "1"
  )
}

# Prove installation rather than merely asking Spack to reuse LAMMPS. This
# second registration points at a run-specific private config, cache, build
# stage, and install tree. The transition report performs spack_find,
# spack_install(reuse=false), and spack_locate through this same registration.
$FreshSpackRoot = "$AresRemoteRoot/spack-fresh"
$FreshSpackStore = "$FreshSpackRoot/store"
$FreshSpackWrapper = "$FreshSpackRoot/bin/spack"
$FreshSpackConfigurationManifest = "$FreshSpackRoot/acceptance-manifest.sha256"
$FreshSpackSpec = "libsigsegv@2.14"
Copy-RemoteTextFile `
  -Source "examples/release-gate/spack-fresh-store.sh" `
  -StagingName "spack-fresh-store.sh" `
  -SshHost $AresSshHost `
  -RemotePath "$AresRemoteRoot/spack-fresh-store.sh" `
  -Description "fresh Spack setup script"
& $OpenSsh $AresSshHost "chmod 700 '$AresRemoteRoot/spack-fresh-store.sh' && '$AresRemoteRoot/spack-fresh-store.sh' '$FreshSpackRoot' '$AresFreshBaseSpack'"
if ($LASTEXITCODE -ne 0) { throw "disposable Spack store setup failed" }
$ExpectedFreshSpackConfigurationSha256 = (
  & $OpenSsh $AresSshHost "sha256sum '$FreshSpackConfigurationManifest' | cut -d' ' -f1" |
    Out-String
).Trim()
if ($LASTEXITCODE -ne 0 -or
    $ExpectedFreshSpackConfigurationSha256 -cnotmatch '^[0-9a-f]{64}$') {
  throw "fresh Spack configuration manifest digest is unavailable"
}
$ExpectedFreshSpackHash = (
  & $OpenSsh $AresSshHost "'$FreshSpackWrapper' spec --format '{hash}' '$FreshSpackSpec'" |
    Out-String
).Trim()
if ($LASTEXITCODE -ne 0 -or $ExpectedFreshSpackHash -cnotmatch '^[a-z0-9]{32}$') {
  throw "fresh Spack acceptance spec did not concretize to one canonical hash"
}

& $Relay remote-mcp register --cluster $AresCluster --name spack-fresh `
  --command $AresClioKit `
  --arg=mcp-server --arg=spack --arg=-- `
  --arg=--spack-command --arg=$FreshSpackWrapper `
  --namespace spack-fresh `
  --contract clio-kit-spack-user-v2 `
  --allow-tool spack_find --allow-tool spack_locate --allow-tool spack_install `
  --profile user --call-timeout-seconds 14400 --replace
if ($LASTEXITCODE -ne 0) { throw "fresh Spack MCP registration failed" }
& $Relay remote-mcp refresh --cluster $AresCluster --name spack-fresh
if ($LASTEXITCODE -ne 0) { throw "fresh Spack MCP discovery failed" }
& $Relay remote-mcp reload --profile user | Out-Host

$FreshArguments = Write-JsonFile "ares-spack-install.json" @{
  spec = $FreshSpackSpec; reuse = $false; timeout_seconds = 14400
}
$FreshExpectation = Write-JsonFile "ares-spack-install-expectation.json" @{
  contract = "clio-kit-spack-user-v2"; tool = "spack_install"
  package_name = "libsigsegv"; dag_hash = $ExpectedFreshSpackHash
  requested_spec = $FreshSpackSpec; reuse = $false
  fresh_install_store_root = $FreshSpackStore
  fresh_install_configuration_manifest_path = $FreshSpackConfigurationManifest
  fresh_install_configuration_sha256 = $ExpectedFreshSpackConfigurationSha256
}
Invoke-RelayReport -Id "ares-spack-install" -ReportOption "--validation-report" -Command @(
  "remote-mcp", "validate", "--cluster", $AresCluster, "--name", "spack-fresh",
  "--tool", "spack_install", "--arguments-json-file", $FreshArguments,
  "--result-expectation-json-file", $FreshExpectation,
  "--wait-timeout-seconds", "14400", "--poll-seconds", "1"
)
```

The discovered and allowlisted tool sets must both be exactly
`spack_find`, `spack_install`, and `spack_locate`. Load is intentionally absent:
JARVIS owns the environment used by a run and persists it before scheduler
execution. A successful protocol response is not sufficient release evidence:
the first two reports prove the exact LAMMPS DAG hash and independently resolved
prefix/canonical `/hash` load spec. The third is one ordered transition report
whose three durable relay jobs prove pre-install count zero, explicit
`reuse=false`/`spack install --fresh`, exact post-install hash, and a canonical
prefix contained by the run-specific disposable store. Its seven transition
checks also bind all phases to the `spack-fresh` user-profile registration,
preserve one wrapper/configuration manifest SHA and path before and after the
install, and retain distinct packaged stdio and result artifacts for each job.
The release policy pins `libsigsegv@2.14`, `reuse=false`, and the package name;
the run-specific DAG hash, store, prefix, and configuration path are validated
for canonical shape and internal equality rather than hardcoded in policy.

## Run explicit scheduler lifecycle

```powershell
Invoke-RelayReport -Id "ares-slurm-lifecycle" -ReportOption "--report" -Command @(
  "scheduler", "validate-lifecycle", "--cluster", $AresCluster,
  "--provider", "slurm", "--run-seconds", "30",
  "--timeout-seconds", "180", "--poll-seconds", "1"
)
```

## Build owned cleanup fixtures

Cleanup ownership is established by submitting through the API process created
for the exact relay-session generation. A manually submitted queue job is not a
valid substitute.

```powershell
function Start-OwnedSession {
  param([string] $Cluster, [string] $SessionId, [int] $RemotePort)
  & $Relay session start --cluster $Cluster --session-id $SessionId `
    --remote-api-port $RemotePort --require-token | Out-Host
  if ($LASTEXITCODE -ne 0) { throw "session start failed: $SessionId" }
  $Status = (& $Relay session status --cluster $Cluster --session-id $SessionId | Out-String) | ConvertFrom-Json
  if (-not $Status.session_generation_id) { throw "session generation is absent" }
  $Status.session_generation_id
}

function Submit-OwnedPipeline {
  param(
    [string] $Cluster, [string] $SessionId, [string] $Generation,
    [string] $PipelineYaml
  )
  $IdempotencyKey = "$RunId-$SessionId-$([guid]::NewGuid().ToString('N'))"
  $JobJson = (& $Relay session submit-jarvis --cluster $Cluster `
    --session-id $SessionId --session-generation-id $Generation `
    --pipeline-yaml-file $PipelineYaml --idempotency-key $IdempotencyKey | Out-String)
  if ($LASTEXITCODE -ne 0) { throw "owned submission failed: $SessionId" }
  $Job = $JobJson | ConvertFrom-Json
  if (-not $Job.job_id) { throw "owned submission returned no job id" }
  $Job.job_id
}

function Wait-OwnedSchedulerIdentity {
  param([string] $Cluster, [string] $JobId)
  $Deadline = (Get-Date).AddSeconds(120)
  do {
    $Status = (& $Relay job status $JobId --cluster $Cluster | Out-String) | ConvertFrom-Json
    if ($Status.terminal) { throw "owned job became terminal before scheduler identity was observed" }
    $ActiveSchedulers = @(
      $Status.scheduler | Where-Object {
        [string]$_.status.scheduler_job_id -and
          $_.status.phase -in @("pending", "allocated", "running")
      }
    )
    if ($ActiveSchedulers.Count -gt 0) { return $Status }
    Start-Sleep -Seconds 1
  } while ((Get-Date) -lt $Deadline)
  throw "scheduler identity timeout for $JobId"
}

function Start-OwnedGatewayFixture {
  param(
    [string] $Cluster, [string] $SessionId, [string] $Generation,
    [string] $RuntimeFile, [string] $FixtureId
  )
  $Result = Invoke-RelayReport -Id "$FixtureId-gateway-fixture" -Diagnostic `
    -ReportOption "--validation-report" -Command @(
      "gateway", "start-runtime", "--cluster", $Cluster, "--name", "$RunId-$FixtureId",
      "--runtime-json-file", $RuntimeFile,
      "--owner-session-id", $SessionId, "--owner-session-generation-id", $Generation
    )
  $Gateway = $Result.Output | ConvertFrom-Json
  if (-not $Gateway.session_id) { throw "owned gateway returned no session id" }
  $Gateway.session_id
}

function Invoke-EmergencySessionCleanup {
  param(
    [Parameter(Mandatory)] [string] $Cluster,
    [Parameter(Mandatory)] [string] $SessionId,
    [switch] $CancelSchedulerJobs,
    [string[]] $PreserveSchedulerJobIds = @()
  )
  $Command = @("session", "teardown", "--cluster", $Cluster, "--session-id", $SessionId)
  if ($CancelSchedulerJobs) {
    $Command += @("--cancel-jobs", "--cancel-scheduler-jobs")
  } else {
    $Command += @("--keep-jobs", "--keep-scheduler-jobs")
  }
  foreach ($SchedulerJobId in $PreserveSchedulerJobIds) {
    if ($SchedulerJobId) {
      $Command += @("--preserve-scheduler-job-id", $SchedulerJobId)
    }
  }
  $Command += "--no-stop-worker"
  Invoke-RelayReport -Id "emergency-session-$([guid]::NewGuid().ToString('N'))" `
    -Diagnostic -ReportOption "--validation-report" -Command $Command | Out-Null
}

function Find-ExactGatewayByName {
  param(
    [Parameter(Mandatory)] [string] $Cluster,
    [Parameter(Mandatory)] [string] $Name
  )
  $DesktopCursor = 1
  $ClusterCursor = 1
  $MatchesById = @{}
  for ($PageNumber = 1; $PageNumber -le 1000; $PageNumber++) {
    $Page = (& $Relay gateway list --cluster $Cluster --limit 100 `
      --desktop-cursor $DesktopCursor --cluster-cursor $ClusterCursor | Out-String) |
      ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw "gateway recovery listing failed" }
    foreach ($Gateway in @($Page.gateway_sessions)) {
      if ($Gateway.name -eq $Name -and $Gateway.state -ne "closed") {
        $MatchesById[[string]$Gateway.session_id] = $Gateway
      }
    }
    $NextDesktop = $Page.source_next_cursors.desktop
    $NextCluster = $Page.source_next_cursors.cluster
    if ($null -eq $NextDesktop -and $null -eq $NextCluster) { break }
    if ($null -ne $NextDesktop) {
      if ([int]$NextDesktop -le $DesktopCursor) { throw "desktop gateway cursor did not advance" }
      $DesktopCursor = [int]$NextDesktop
    }
    if ($null -ne $NextCluster) {
      if ([int]$NextCluster -le $ClusterCursor) { throw "cluster gateway cursor did not advance" }
      $ClusterCursor = [int]$NextCluster
    }
    if ($PageNumber -eq 1000) { throw "gateway recovery listing exceeded its page bound" }
  }
  $Matches = @($MatchesById.Values)
  if ($Matches.Count -gt 1) { throw "gateway recovery name is not unique: $Name" }
  if ($Matches.Count -eq 1) { return [string]$Matches[0].session_id }
  $null
}

function Invoke-EmergencyGatewayCleanup {
  param(
    [Parameter(Mandatory)] [string] $Cluster,
    [Parameter(Mandatory)] [string] $Name,
    [string] $SessionId
  )
  $ResolvedSessionId = $SessionId
  if (-not $ResolvedSessionId) {
    $ResolvedSessionId = Find-ExactGatewayByName -Cluster $Cluster -Name $Name
  }
  if (-not $ResolvedSessionId) { return }
  Invoke-RelayReport `
    -Id "emergency-gateway-$([guid]::NewGuid().ToString('N'))" `
    -Diagnostic -ReportOption "--validation-report" -Command @(
      "gateway", "stop-runtime", $ResolvedSessionId, "--cluster", $Cluster,
      "--keep-scheduler-job"
    ) | Out-Null
}
```

## Prove default detach and teardown

The default is to preserve relay and scheduler jobs. Spell both choices out in
the command so the report is unambiguous. Detach and teardown use the same
relay-session id and generation; the evidence evaluator groups their resource
records by that id.

```powershell
$AresCleanupYaml = Join-Path $RenderedRoot "ares-owned-cleanup.yaml"
Render-Template "examples/release-gate/owned-cleanup-ares.yaml.tmpl" $AresCleanupYaml @{
  RUN_ID = $RunId; REMOTE_ROOT = $AresRemoteRoot
}
$AresDefaultSession = "$RunId-ares-default-cleanup"
$AresDefaultMayExist = $true
$AresDefaultPrimaryError = $null
$AresDefaultCleanupError = $null
try {
  $AresDefaultGeneration = Start-OwnedSession $AresCluster $AresDefaultSession `
    $AresDefaultApiPort
  $AresDefaultJob = Submit-OwnedPipeline $AresCluster $AresDefaultSession `
    $AresDefaultGeneration $AresCleanupYaml
  Wait-OwnedSchedulerIdentity $AresCluster $AresDefaultJob | Out-Null
  $AresOwnedGateway = Start-OwnedGatewayFixture $AresCluster $AresDefaultSession `
    $AresDefaultGeneration $AresRuntime "ares-default"
  Invoke-RelayReport -Id "ares-cleanup-detach" -ReportOption "--validation-report" -Command @(
    "session", "detach", "--cluster", $AresCluster, "--session-id", $AresDefaultSession
  )
  Invoke-RelayReport -Id "ares-cleanup-teardown" -ReportOption "--validation-report" -Command @(
    "session", "teardown", "--cluster", $AresCluster, "--session-id", $AresDefaultSession,
    "--keep-jobs", "--keep-scheduler-jobs", "--no-stop-worker"
  )
  $AresDefaultMayExist = $false
} catch {
  $AresDefaultPrimaryError = $_
} finally {
  if ($AresDefaultMayExist) {
    try {
      Invoke-EmergencySessionCleanup -Cluster $AresCluster -SessionId $AresDefaultSession
    } catch {
      $AresDefaultCleanupError = $_
    }
  }
}
if ($AresDefaultPrimaryError) {
  if ($AresDefaultCleanupError) { Write-Warning "Ares emergency cleanup failed: $AresDefaultCleanupError" }
  throw $AresDefaultPrimaryError
}
if ($AresDefaultCleanupError) { throw $AresDefaultCleanupError }

```

Do not replace either teardown with implicit defaults. The reports must show
that the operator requested `keep_jobs` and `keep_scheduler_jobs`, stopped the
owned API and connector processes, closed the exact owned gateway records, and
left the persistent worker running.

## Prove explicit cancellation and sentinel preservation

Use a second Ares session and a second long but bounded scheduled job. Wait for
its structured scheduler identity before teardown. The separately submitted
held job is deliberately unowned by that relay session and must remain active.

```powershell
$AresCancelYaml = Join-Path $RenderedRoot "ares-owned-cancel.yaml"
Render-Template "examples/release-gate/owned-cancel-ares.yaml.tmpl" $AresCancelYaml @{
  RUN_ID = $RunId; REMOTE_ROOT = $AresRemoteRoot
}
$CancelSession = "$RunId-ares-explicit-cancel"
$SentinelName = "$RunId-sentinel"
$SentinelJobId = $null
$CancelSessionMayExist = $true
$CancelPrimaryError = $null
$CancelCleanupErrors = [System.Collections.Generic.List[string]]::new()
try {
  $CancelGeneration = Start-OwnedSession $AresCluster $CancelSession $CancelApiPort
  $CancelJob = Submit-OwnedPipeline $AresCluster $CancelSession `
    $CancelGeneration $AresCancelYaml
  Wait-OwnedSchedulerIdentity $AresCluster $CancelJob | Out-Null
  $SentinelRaw = (& $Relay scheduler submit-held-validation --cluster $AresCluster `
    --provider slurm --job-name $SentinelName --run-seconds 30 | Out-String).Trim()
  if ($LASTEXITCODE -ne 0) { throw "sentinel submission command failed" }
  $Sentinel = $SentinelRaw | ConvertFrom-Json
  $SentinelJobId = [string]$Sentinel.scheduler_job_id
  if ($SentinelJobId -notmatch '^[0-9]+$') { throw "sentinel submission returned no exact job id" }
  Invoke-RelayReport -Id "ares-explicit-cancel-teardown" `
    -ReportOption "--validation-report" -Command @(
      "session", "teardown", "--cluster", $AresCluster, "--session-id", $CancelSession,
      "--cancel-jobs", "--cancel-scheduler-jobs",
      "--preserve-scheduler-job-id", $SentinelJobId,
      "--no-stop-worker", "--relay-cancel-timeout-seconds", "120",
      "--relay-cancel-poll-seconds", "0.25"
    )
  $CancelSessionMayExist = $false
} catch {
  $CancelPrimaryError = $_
} finally {
  if (-not $SentinelJobId) {
    try {
      $SentinelRows = @(
        & $OpenSsh $AresSshHost `
          "squeue -h --user `"`$(id -un)`" --name '$SentinelName' --format='%A|%j'" |
          Where-Object { $_.Trim() }
      )
      if ($LASTEXITCODE -ne 0) { throw "exact sentinel recovery query failed" }
      $SentinelCandidates = @(
        foreach ($Row in $SentinelRows) {
          if ($Row -notmatch '^([0-9]+)\|(.+)$' -or $Matches[2] -ne $SentinelName) {
            throw "sentinel recovery returned an unexpected row"
          }
          $Matches[1]
        }
      ) | Sort-Object -Unique
      if ($SentinelCandidates.Count -gt 1) {
        throw "sentinel recovery found more than one exact-name job"
      }
      if ($SentinelCandidates.Count -eq 1) { $SentinelJobId = $SentinelCandidates[0] }
    } catch {
      $CancelCleanupErrors.Add($_.Exception.Message)
    }
  }
  if ($CancelSessionMayExist) {
    try {
      Invoke-EmergencySessionCleanup -Cluster $AresCluster -SessionId $CancelSession `
        -CancelSchedulerJobs -PreserveSchedulerJobIds @($SentinelJobId)
      $CancelSessionMayExist = $false
    } catch {
      $CancelCleanupErrors.Add($_.Exception.Message)
    }
  }
  if ($SentinelJobId) {
    try {
      & $Relay scheduler release-validation $SentinelJobId `
        --cluster $AresCluster --provider slurm | Out-Host
      if ($LASTEXITCODE -ne 0) {
        & $OpenSsh $AresSshHost "scancel '$SentinelJobId'"
        if ($LASTEXITCODE -ne 0) {
          throw "sentinel release and exact-job fallback cancellation both failed"
        }
      }
      $SentinelDeadline = (Get-Date).AddSeconds(120)
      do {
        $SentinelStatus = (& $Relay scheduler status $SentinelJobId `
          --cluster $AresCluster --provider slurm | Out-String) | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "exact sentinel status query failed" }
        if ($SentinelStatus.phase -in @("completed", "failed", "canceled")) { break }
        Start-Sleep -Seconds 1
      } while ((Get-Date) -lt $SentinelDeadline)
      if ($SentinelStatus.phase -notin @("completed", "failed", "canceled")) {
        throw "sentinel did not reach an exact terminal scheduler state"
      }
    } catch {
      $CancelCleanupErrors.Add($_.Exception.Message)
    }
  }
}
if ($CancelPrimaryError) {
  if ($CancelCleanupErrors.Count -gt 0) {
    Write-Warning "explicit-cancel emergency cleanup failed: $($CancelCleanupErrors -join '; ')"
  }
  throw $CancelPrimaryError
}
if ($CancelCleanupErrors.Count -gt 0) {
  throw "explicit-cancel cleanup failed: $($CancelCleanupErrors -join '; ')"
}
```

The report must contain a canceled owned relay job, its canceled SLURM job, and
the retained unowned sentinel with `preservation_verified=true`. Never cancel
all jobs by user, name, partition, or cluster.

## Prove homelab default detach and teardown

Run the homelab default-cleanup pair after the explicit Ares cancellation report
so the report creation order remains identical to the policy matrix.

```powershell
$HomelabCleanupYaml = Join-Path $RenderedRoot "homelab-owned-cleanup.yaml"
Render-Template "examples/release-gate/owned-cleanup-homelab.yaml.tmpl" $HomelabCleanupYaml @{
  RUN_ID = $RunId
}
$HomelabDefaultSession = "$RunId-homelab-default-cleanup"
$HomelabDefaultMayExist = $true
$HomelabDefaultPrimaryError = $null
$HomelabDefaultCleanupError = $null
try {
  $HomelabDefaultGeneration = Start-OwnedSession $HomelabCluster $HomelabDefaultSession `
    $HomelabDefaultApiPort
  $HomelabDefaultJob = Submit-OwnedPipeline $HomelabCluster `
    $HomelabDefaultSession $HomelabDefaultGeneration $HomelabCleanupYaml
  $HomelabOwnedGateway = Start-OwnedGatewayFixture $HomelabCluster `
    $HomelabDefaultSession $HomelabDefaultGeneration $HomelabRuntime "homelab-default"
  Invoke-RelayReport -Id "homelab-cleanup-detach" `
    -ReportOption "--validation-report" -Command @(
      "session", "detach", "--cluster", $HomelabCluster,
      "--session-id", $HomelabDefaultSession
    )
  Invoke-RelayReport -Id "homelab-cleanup-teardown" `
    -ReportOption "--validation-report" -Command @(
      "session", "teardown", "--cluster", $HomelabCluster,
      "--session-id", $HomelabDefaultSession,
      "--keep-jobs", "--keep-scheduler-jobs", "--no-stop-worker"
    )
  $HomelabDefaultMayExist = $false
} catch {
  $HomelabDefaultPrimaryError = $_
} finally {
  if ($HomelabDefaultMayExist) {
    try {
      Invoke-EmergencySessionCleanup -Cluster $HomelabCluster `
        -SessionId $HomelabDefaultSession
    } catch {
      $HomelabDefaultCleanupError = $_
    }
  }
}
if ($HomelabDefaultPrimaryError) {
  if ($HomelabDefaultCleanupError) {
    Write-Warning "homelab emergency cleanup failed: $HomelabDefaultCleanupError"
  }
  throw $HomelabDefaultPrimaryError
}
if ($HomelabDefaultCleanupError) { throw $HomelabDefaultCleanupError }
```

## Prove homelab transport

Use unique proxy, API, and local ports. Credentials are read only from the
environment keys referenced by the cluster registry.

```powershell
$HomelabTransportYaml = Join-Path $RenderedRoot "homelab-transport.yaml"
Render-Template "examples/release-gate/homelab-transport-echo.yaml.tmpl" $HomelabTransportYaml @{
  RUN_ID = $RunId
}
Invoke-RelayReport -Id "homelab-transport" -ReportOption "--report" -Command @(
  "live-test", "--cluster", $HomelabCluster,
  "--validation-scenario", "transport", "--jarvis-yaml", $HomelabTransportYaml,
  "--verify-cluster-deployment", "--verify-transport", "--verify-ssh-transport",
  "--no-verify-direct-transport", "--no-allow-direct-transport-fallback",
  "--require-structured-runtime-metadata",
  "--transport-local-bind-port", [string]$HomelabTransportLocalPort,
  "--transport-remote-api-port", [string]$HomelabTransportRemotePort,
  "--transport-proxy-name", "$RunId-homelab-transport",
  "--ssh-transport-local-bind-port", [string]$HomelabSshTransportLocalPort,
  "--ssh-transport-remote-api-port", [string]$HomelabSshTransportRemotePort,
  "--ssh-transport-session-id", "$RunId-homelab-ssh",
  "--timeout-seconds", "600", "--poll-seconds", "1"
)
```

## Prove the dedicated gateway pair

Reports 16 and 17 must refer to the exact same gateway session. Stop closes the
connectors and record while explicitly retaining the scheduler job; the bounded
fixture exits by itself.

```powershell
$DedicatedGatewayName = "$RunId-dedicated-gateway"
$GatewaySession = $null
$DedicatedGatewayMayExist = $true
$DedicatedGatewayPrimaryError = $null
$DedicatedGatewayCleanupError = $null
try {
  $GatewayStart = Invoke-RelayReport -Id "ares-gateway-start" `
    -ReportOption "--validation-report" -Command @(
      "gateway", "start-runtime", "--cluster", $AresCluster,
      "--name", $DedicatedGatewayName, "--runtime-json-file", $AresDedicatedRuntime
    )
  $GatewaySession = ($GatewayStart.Output | ConvertFrom-Json).session_id
  if (-not $GatewaySession) { throw "gateway start returned no session id" }
  Invoke-RelayReport -Id "ares-gateway-stop" `
    -ReportOption "--validation-report" -Command @(
      "gateway", "stop-runtime", $GatewaySession, "--cluster", $AresCluster,
      "--keep-scheduler-job"
    )
  $DedicatedGatewayMayExist = $false
} catch {
  $DedicatedGatewayPrimaryError = $_
} finally {
  if ($DedicatedGatewayMayExist) {
    try {
      Invoke-EmergencyGatewayCleanup -Cluster $AresCluster `
        -Name $DedicatedGatewayName -SessionId $GatewaySession
    } catch {
      $DedicatedGatewayCleanupError = $_
    }
  }
}
if ($DedicatedGatewayPrimaryError) {
  if ($DedicatedGatewayCleanupError) {
    Write-Warning "dedicated gateway emergency cleanup failed: $DedicatedGatewayCleanupError"
  }
  throw $DedicatedGatewayPrimaryError
}
if ($DedicatedGatewayCleanupError) { throw $DedicatedGatewayCleanupError }
```

## Verify and upload the exact report set

Do not use a wildcard over a long-lived validation directory. Verify the
ordered stage-local list and upload those exact files. Candidate and released
invocation ids must be globally disjoint.

```powershell
if ($PolicyReports.Count -ne $Matrix.report_count_per_stage) {
  throw "expected $($Matrix.report_count_per_stage) policy reports, found $($PolicyReports.Count)"
}
$ExpectedNames = @($OrderedMatrix | ForEach-Object { "$PolicyReportPrefix-$($_.id).json" })
$ActualNames = @($PolicyReports | ForEach-Object { Split-Path -Leaf $_ })
if (($ActualNames -join "`n") -ne ($ExpectedNames -join "`n")) {
  throw "policy report file set or order does not match the release matrix"
}
$Documents = $PolicyReports | ForEach-Object { Get-Content -Raw $_ | ConvertFrom-Json }
for ($Index = 0; $Index -lt $OrderedMatrix.Count; $Index++) {
  $Expected = $OrderedMatrix[$Index]
  $Observed = $Documents[$Index]
  if ($Observed.cluster -ne $Expected.cluster -or $Observed.scenario -ne $Expected.scenario) {
    throw "policy report cluster/scenario does not match matrix entry $($Expected.id)"
  }
}
if (($Documents.report_id | Sort-Object -Unique).Count -ne $Matrix.report_count_per_stage) {
  throw "duplicate report id"
}
$InvocationIds = $Documents | ForEach-Object { $_.evidence_trust.invocation_id }
if (($InvocationIds | Sort-Object -Unique).Count -ne $Matrix.report_count_per_stage) {
  throw "duplicate or missing invocation id"
}
if (($Documents | Where-Object status -ne "passed").Count -ne 0) { throw "non-passing report" }
$Manifest = @{
  schema_version = "clio-relay.operator-report-list.v1"
  stage = $Stage
  run_id = $RunId
  reports = $PolicyReports | ForEach-Object {
    @{ path = $_; sha256 = (Get-FileHash -Algorithm SHA256 $_).Hash.ToLowerInvariant() }
  }
}
$ManifestPath = Write-JsonFile "$Stage-report-list.json" $Manifest
foreach ($Path in $PolicyReports) {
  gh release upload $Tag $Path --repo $GitHubRepo
  if ($LASTEXITCODE -ne 0) { throw "report upload failed without replacement: $Path" }
}
```

### Out-of-band interrupted-upload recovery

Do not run this section during a normal acceptance pass. If an upload is
interrupted, do not resume at the failed file and do not use `--clobber`.
Define the guarded recovery function below, then deliberately invoke it with
`-ConfirmDiscardEntireIncompleteStage` to discard every exact asset name for
that one incomplete stage. Rerun the complete stage with a fresh run id.

```powershell
function Remove-IncompleteStageReports {
  param([switch] $ConfirmDiscardEntireIncompleteStage)
  if (-not $ConfirmDiscardEntireIncompleteStage) {
    throw "incomplete-stage recovery requires explicit whole-stage confirmation"
  }
  $ExistingReleaseAssets = @(
    gh release view $Tag --repo $GitHubRepo --json assets --jq '.assets[].name'
  )
  if ($LASTEXITCODE -ne 0) { throw "release asset recovery inventory failed" }
  $ExistingAssetSet = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::Ordinal
  )
  foreach ($AssetName in $ExistingReleaseAssets) {
    [void]$ExistingAssetSet.Add([string]$AssetName)
  }
  $IncompleteStageAssets = @(
    $ExpectedReleaseAssetNames | Where-Object { $ExistingAssetSet.Contains([string]$_) }
  )
  $ObservedStageSeals = @(
    $StageSealNames | Where-Object { $ExistingAssetSet.Contains([string]$_) }
  )
  if ($ObservedStageSeals.Count -ne 0) {
    throw "refusing to modify a sealed evidence stage: $($ObservedStageSeals -join ', ')"
  }
  foreach ($AssetName in $IncompleteStageAssets) {
    gh release delete-asset $Tag $AssetName --repo $GitHubRepo --yes
    if ($LASTEXITCODE -ne 0) { throw "exact stage-asset deletion failed: $AssetName" }
  }
}

# Recovery only; deliberately uncomment after reviewing the exact stage above:
# Remove-IncompleteStageReports -ConfirmDiscardEntireIncompleteStage
```

This recovery is intentionally all-or-nothing for one stage. It never selects
assets by wildcard and it never modifies candidate assets while recovering the
released stage, or vice versa. Never run it concurrently with an attestation,
gate, promotion, or finalization workflow. Any stage binding, downstream gate
decision, promotion record, or final claims record makes recovery fail closed.

The release workflow order is fixed:

1. `stage-candidate.yml`
2. `live-validation-attest.yml`
3. `release-gate.yml`
4. `released-validation-attest.yml`
5. `finalize-release.yml`

After the candidate upload, dispatch `live-validation-attest.yml` and then
`release-gate.yml` from protected `main`. After public PyPI publication, create
a new released stage, install from the public index, rerun all 17 reports, upload
the distinct `released-validation-*.json` files, and dispatch
`released-validation-attest.yml`. Only then may `finalize-release.yml` publish
the final GitHub release claims.
