param(
    [Parameter(Mandatory = $false)][string] $Configuration = "Release",
    [Parameter(Mandatory = $false)][string] $VersionSuffix = "",
    [Parameter(Mandatory = $false)][string] $OutputPath = "",
    [Parameter(Mandatory = $false)][switch] $SkipTests,
    [Parameter(Mandatory = $false)][switch] $EnableCodeCoverage,
    [Parameter(Mandatory = $false)][switch] $EnableIntegrationTests
)

$ErrorActionPreference = "Stop"

$solutionPath = Split-Path $MyInvocation.MyCommand.Definition
$sdkFile = Join-Path $solutionPath "global.json"

$libraryProjects = @(
    (Join-Path $solutionPath "JustSaying\JustSaying.csproj"),
    (Join-Path $solutionPath "JustSaying.Models\JustSaying.Models.csproj")
)

$testProjects = @(
    (Join-Path $solutionPath "JustSaying.UnitTests\JustSaying.UnitTests.csproj")
)

if ($EnableIntegrationTests -eq $true) {
  $testProjects += (Join-Path $solutionPath "JustSaying.IntegrationTests\JustSaying.IntegrationTests.csproj");
}

$dotnetVersion = (Get-Content $sdkFile | Out-String | ConvertFrom-Json).sdk.version

if ($OutputPath -eq "") {
    $OutputPath = Join-Path "$(Convert-Path "$PSScriptRoot")" "artifacts"
}

if ($null -ne $env:CI) {
    if (($VersionSuffix -eq "" -and $env:APPVEYOR_REPO_TAG -eq "false" -and $env:APPVEYOR_BUILD_NUMBER -ne "") -eq $true) {
        $ThisVersion = $env:APPVEYOR_BUILD_NUMBER -as [int]
        $VersionSuffix = "beta" + $ThisVersion.ToString("0000")
    }
}

$installDotNetSdk = $false;

if (($null -eq (Get-Command "dotnet.exe" -ErrorAction SilentlyContinue)) -and ($null -eq (Get-Command "dotnet" -ErrorAction SilentlyContinue))) {
    Write-Host "The .NET Core SDK is not installed."
    $installDotNetSdk = $true
}
else {
    Try {
        $installedDotNetVersion = (dotnet --version 2>&1 | Out-String).Trim()
    }
    Catch {
        $installedDotNetVersion = "?"
    }

    if ($installedDotNetVersion -ne $dotnetVersion) {
        Write-Host "The required version of the .NET Core SDK is not installed. Expected $dotnetVersion."
        $installDotNetSdk = $true
    }
}

if ($installDotNetSdk -eq $true) {
    $env:DOTNET_INSTALL_DIR = Join-Path "$(Convert-Path "$PSScriptRoot")" ".dotnetcli"

    if (!(Test-Path $env:DOTNET_INSTALL_DIR)) {
        mkdir $env:DOTNET_INSTALL_DIR | Out-Null
        $installScript = Join-Path $env:DOTNET_INSTALL_DIR "install.ps1"
        Invoke-WebRequest "https://dot.net/v1/dotnet-install.ps1" -OutFile $installScript -UseBasicParsing
        & $installScript -Version "$dotnetVersion" -InstallDir "$env:DOTNET_INSTALL_DIR" -NoPath
    }

    $env:PATH = "$env:DOTNET_INSTALL_DIR;$env:PATH"
    $dotnet = Join-Path "$env:DOTNET_INSTALL_DIR" "dotnet.exe"
}
else {
    $dotnet = "dotnet"
}

function DotNetPack {
    param([string]$Project)

    if ($VersionSuffix) {
        & $dotnet pack $Project --output $OutputPath --configuration $Configuration --version-suffix "$VersionSuffix"
    }
    else {
        & $dotnet pack $Project --output $OutputPath --configuration $Configuration
    }
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet pack failed with exit code $LASTEXITCODE"
    }
}

function DotNetTest {
    param([string]$Project)

    if ($EnableCodeCoverage -eq $false) {
        & $dotnet test $Project --output $OutputPath
    }
    else {

        if ($installDotNetSdk -eq $true) {
            $dotnetPath = $dotnet
        }
        else {
            $dotnetPath = (Get-Command "dotnet.exe").Source
        }

        $nugetPath = Join-Path $env:USERPROFILE ".nuget\packages"

        $openCoverVersion = "4.6.519"
        $openCoverPath = Join-Path $nugetPath "OpenCover\$openCoverVersion\tools\OpenCover.Console.exe"

        $reportGeneratorVersion = "3.1.2"
        $reportGeneratorPath = Join-Path $nugetPath "ReportGenerator\$reportGeneratorVersion\tools\ReportGenerator.exe"

        $coverageOutput = Join-Path $OutputPath "code-coverage.xml"
        $reportOutput = Join-Path $OutputPath "coverage"

        & $openCoverPath `
            `"-target:$dotnetPath`" `
            `"-targetargs:test $Project --output $OutputPath`" `
            -output:$coverageOutput `
            -hideskipped:All `
            -mergebyhash `
            -mergeoutput `
            -oldstyle `
            -register:user `
            -skipautoprops `
            `"-filter:+[JustSaying]* -[JustSaying.Tests]*`"

        & $reportGeneratorPath `
            `"-reports:$coverageOutput`" `
            `"-targetdir:$reportOutput`" `
            -verbosity:Warning
    }

    if ($LASTEXITCODE -ne 0) {
        throw "dotnet test failed with exit code $LASTEXITCODE"
    }
}

Write-Host "Creating packages..." -ForegroundColor Green

ForEach ($libraryProject in $libraryProjects) {
    DotNetPack $libraryProject
}

if (($null -ne $env:CI) -And ($EnableIntegrationTests -eq $true)) {
    & docker pull pafortin/goaws
    & docker run -d --name goaws -p 4100:4100 pafortin/goaws
    $env:AWS_SERVICE_URL="http://localhost:4100"
}

if ($SkipTests -eq $false) {
    Write-Host "Running tests..." -ForegroundColor Green
    ForEach ($testProject in $testProjects) {
        DotNetTest $testProject
    }
}
