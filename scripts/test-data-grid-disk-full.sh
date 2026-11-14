#!/usr/bin/env bash
# Real ENOSPC stays inside three owned 32-MiB tmpfs mounts; source and NuGet cache are read-only.
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image='mcr.microsoft.com/dotnet/sdk@sha256:212fe4cb74627d5adf9817a4088906ae3edff1bd853bb27565d4b46731efe61e'
cache="$(dotnet nuget locals global-packages --list | sed 's/^global-packages: //')"
[[ -d "$cache" ]] || { printf '%s\n' 'NuGet cache missing; restore the solution first.' >&2; exit 1; }
owner="$(uuidgen | tr '[:upper:]' '[:lower:]' | tr -d '-')"
container_id=''
cleanup() {
    if [[ -n "$container_id" ]]; then docker rm --force "$container_id" >/dev/null; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
container_id="$(docker create --name "hostloom-grid-diskfull-$owner" --label "hostloom.diskfull.owner=$owner" \
    --cap-drop ALL --security-opt no-new-privileges --memory 2g --memory-swap 2g --cpus 2 --network none \
    --mount "type=bind,source=$repo,target=/source,readonly" \
    --mount "type=bind,source=$cache,target=/cache,readonly" \
    --tmpfs /hostloom-grid-enospc-0:rw,noexec,nosuid,size=32m,mode=700 \
    --tmpfs /hostloom-grid-enospc-1:rw,noexec,nosuid,size=32m,mode=700 \
    --tmpfs /hostloom-grid-enospc-2:rw,noexec,nosuid,size=32m,mode=700 \
    --env HOSTLOOM_DATA_GRID_DISK_FULL=1 --env "HOSTLOOM_DATA_GRID_DISK_FULL_OWNER=$owner" \
    --env "HOSTLOOM_DATA_GRID_FULL_TESTS=${HOSTLOOM_DATA_GRID_FULL_TESTS:-0}" \
    --env HOSTLOOM_DATA_GRID_PROCESSES=1 --env DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    "$image" sleep infinity)"
printf 'Owned container: %s; owner: %s; image: %s\n' "$container_id" "$owner" "$image"
docker start "$container_id" >/dev/null
# The outer timeout bounds restore/build/test and kills the owned process tree on expiry.
docker exec "$container_id" timeout --kill-after=15s 900s bash -euo pipefail -c '
    mkdir /work
    tar -C /source --exclude=bin --exclude=obj --exclude=TestResults --exclude=__pycache__ -cf - \
        src tests examples/HostLoom.Examples.CompositionDiagnostics/ApplicationCompositionLedger.cs \
        global.json Directory.Build.props Directory.Build.targets Directory.Packages.props .editorconfig HostLoom.slnx \
        | tar -C /work -xf -
    cd /work
    for member in 0 1 2; do printf %s "$HOSTLOOM_DATA_GRID_DISK_FULL_OWNER" > "/hostloom-grid-enospc-$member/OWNER"; done
    dotnet --info
    uname -a
    df -T /hostloom-grid-enospc-{0,1,2}
    dotnet restore tests/HostLoom.Tests/HostLoom.Tests.csproj --packages /work/.packages --source /cache -m:1
    dotnet build tests/HostLoom.Tests/HostLoom.Tests.csproj -c Release --no-restore -m:1
    filter="*DataGridDiskFull*"
    if [[ "$HOSTLOOM_DATA_GRID_FULL_TESTS" == 1 ]]; then filter="*DataGrid*"; fi
    dotnet tests/HostLoom.Tests/bin/Release/net10.0/HostLoom.Tests.dll -class "$filter" -showLiveOutput
    for member in 0 1 2; do
        [[ "$(cat "/hostloom-grid-enospc-$member/OWNER")" == "$HOSTLOOM_DATA_GRID_DISK_FULL_OWNER" ]]
        [[ "$(find "/hostloom-grid-enospc-$member" -mindepth 1 -maxdepth 1 | wc -l)" -eq 1 ]]
    done
'
