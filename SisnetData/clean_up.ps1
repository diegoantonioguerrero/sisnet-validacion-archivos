# Elimina los archivos y carpetas no deseados
$releasePath = Join-Path -Path $PSScriptRoot -ChildPath "bin\Release"

# Elimina los recursos zh-CN y fr
Remove-Item -Path "$releasePath\zh-CN" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "$releasePath\fr" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "$releasePath\ja" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "$releasePath\fi" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "$releasePath\es" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "$releasePath\de" -Recurse -Force -ErrorAction SilentlyContinue

Write-Output "Archivos y carpetas no deseados eliminados."
