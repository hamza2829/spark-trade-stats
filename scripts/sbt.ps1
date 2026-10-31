# Optional Windows launcher when sbt is not installed. Java 17 must be on PATH or in JAVA_HOME.
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$SbtArgs = @('test'))
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectRoot
try {
    New-Item -ItemType Directory -Path '.tools' -Force | Out-Null
    $launcher = Join-Path $projectRoot '.tools\sbt-launch-1.10.7.jar'
    if (-not (Test-Path -LiteralPath $launcher)) {
        Invoke-WebRequest -Uri 'https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/1.10.7/sbt-launch-1.10.7.jar' -OutFile $launcher
    }
    $expectedHash = '3CCA02818047327A83EFDE776103A1EF92F76F72C062BADBBB062499A3270C07'
    if ((Get-FileHash -LiteralPath $launcher -Algorithm SHA256).Hash -ne $expectedHash) {
        throw 'sbt launcher checksum mismatch'
    }
    $java = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin\java.exe' } else { 'java' }
    $env:COURSIER_CACHE = Join-Path $projectRoot '.cache\coursier'
    & $java '-Dsbt.global.base=.cache/sbt-global' '-Dsbt.boot.directory=.cache/sbt-boot' `
        '-Dsbt.ivy.home=.cache/ivy' '-Dsbt.repository.config=project/repositories' `
        '-Dsbt.override.build.repos=true' '-Dsbt.log.noformat=true' -jar $launcher @SbtArgs
    if ($LASTEXITCODE -ne 0) { throw "sbt failed with exit code $LASTEXITCODE" }
} finally {
    Pop-Location
}
