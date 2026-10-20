$k = "D:\数据开发环境\VMware\sshkeys\cluster_ed25519"
New-Item -ItemType Directory -Force -Path (Split-Path $k) | Out-Null
if (Test-Path $k) { Remove-Item $k, "$k.pub" -Force -ErrorAction SilentlyContinue }
# -P 指定旧口令(空)来新建，避免 -N "" 空参被吞
& ssh-keygen -q -t ed25519 -N '""' -C "trae-cluster" -f $k
if (Test-Path "$k.pub") { Write-Output "GEN_OK" } else { Write-Output "GEN_FAIL" }