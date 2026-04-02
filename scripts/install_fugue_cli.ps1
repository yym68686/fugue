$ErrorActionPreference = 'Stop'

$DefaultRepo = 'yym68686/fugue'

function Write-Info {
  param([string]$Message)
  Write-Host "[fugue-install] $Message"
}

function Fail {
  param([string]$Message)
  throw "[fugue-install] $Message"
}

function Get-ReleaseBaseUrl {
  if ($env:FUGUE_INSTALL_BASE_URL) {
    return $env:FUGUE_INSTALL_BASE_URL.TrimEnd('/')
  }

  $repo = if ($env:FUGUE_INSTALL_REPO) { $env:FUGUE_INSTALL_REPO } else { $DefaultRepo }
  $version = if ($env:FUGUE_VERSION) { $env:FUGUE_VERSION } else { 'latest' }

  if ($version -eq 'latest') {
    return "https://github.com/$repo/releases/latest/download"
  }

  if ($version.StartsWith('v')) {
    return "https://github.com/$repo/releases/download/$version"
  }

  return "https://github.com/$repo/releases/download/v$version"
}

function Get-Arch {
  $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
  switch ($arch) {
    'x64' { return 'amd64' }
    'arm64' { return 'arm64' }
    default { Fail "unsupported architecture: $arch" }
  }
}

function Get-InstallDir {
  if ($env:FUGUE_INSTALL_DIR) {
    return $env:FUGUE_INSTALL_DIR.TrimEnd('\')
  }

  return (Join-Path $env:LOCALAPPDATA 'Programs\Fugue\bin')
}

function Ensure-UserPath {
  param([string]$InstallDir)

  $normalizedInstallDir = $InstallDir.TrimEnd('\')
  $currentSessionPaths = @()
  if ($env:Path) {
    $currentSessionPaths = $env:Path.Split(';') | Where-Object { $_ -ne '' } | ForEach-Object { $_.TrimEnd('\') }
  }

  if ($currentSessionPaths -notcontains $normalizedInstallDir) {
    $env:Path = "$InstallDir;$env:Path"
  }

  $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
  $userPathEntries = @()
  if ($userPath) {
    $userPathEntries = $userPath.Split(';') | Where-Object { $_ -ne '' } | ForEach-Object { $_.TrimEnd('\') }
  }

  if ($userPathEntries -contains $normalizedInstallDir) {
    return $false
  }

  $updatedEntries = @($userPathEntries + $normalizedInstallDir) | Where-Object { $_ -ne '' } | Select-Object -Unique
  [Environment]::SetEnvironmentVariable('Path', ($updatedEntries -join ';'), 'User')
  return $true
}

$assetName = "fugue_windows_$(Get-Arch).zip"
$checksumsName = 'fugue_checksums.txt'
$installDir = Get-InstallDir
$baseUrl = Get-ReleaseBaseUrl

if ($env:FUGUE_INSTALL_DRY_RUN -eq '1') {
  Write-Output "asset=$assetName"
  Write-Output "install_dir=$installDir"
  Write-Output "asset_url=$baseUrl/$assetName"
  exit 0
}

$tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("fugue-install-" + [System.Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempDir | Out-Null

try {
  $archivePath = Join-Path $tempDir $assetName
  $checksumsPath = Join-Path $tempDir $checksumsName

  Write-Info "downloading $assetName"
  Invoke-WebRequest -Uri "$baseUrl/$assetName" -OutFile $archivePath

  try {
    Invoke-WebRequest -Uri "$baseUrl/$checksumsName" -OutFile $checksumsPath
    $checksumLine = Get-Content $checksumsPath | Where-Object { $_ -match "\s+$([regex]::Escape($assetName))$" } | Select-Object -First 1
    if ($checksumLine) {
      $expected = (($checksumLine -split '\s+') | Where-Object { $_ -ne '' })[0].ToLowerInvariant()
      $actual = (Get-FileHash -Path $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
      if ($expected -ne $actual) {
        Fail "checksum mismatch for $assetName"
      }
    } else {
      Write-Info "checksum entry for $assetName not found; continuing without verification"
    }
  } catch {
    Write-Info "could not verify checksums: $($_.Exception.Message)"
  }

  Write-Info "extracting $assetName"
  Expand-Archive -LiteralPath $archivePath -DestinationPath $tempDir -Force

  $binaryPath = Join-Path $tempDir 'fugue.exe'
  if (-not (Test-Path $binaryPath)) {
    Fail 'archive did not contain fugue.exe'
  }

  New-Item -ItemType Directory -Path $installDir -Force | Out-Null
  Copy-Item -LiteralPath $binaryPath -Destination (Join-Path $installDir 'fugue.exe') -Force

  $pathUpdated = Ensure-UserPath -InstallDir $installDir
  Write-Info "installed $(Join-Path $installDir 'fugue.exe')"
  if ($pathUpdated) {
    Write-Info "updated the user PATH for future shells"
  }
  Write-Info "run 'fugue --help' to get started"
} finally {
  Remove-Item -LiteralPath $tempDir -Recurse -Force -ErrorAction SilentlyContinue
}
