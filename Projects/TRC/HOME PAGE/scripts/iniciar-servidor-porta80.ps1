# Executar PowerShell COMO ADMINISTRADOR (porta 80).
# Pasta do projeto: ...\HOME PAGE
Set-Location $PSScriptRoot\..
$env:PORT = '80'
node server/lan-server.mjs
