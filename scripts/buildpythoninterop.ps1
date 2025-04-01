# Set PYTHON_HOME environment variable
$env:PYTHON_HOME = & python -c "import sys; print(sys.prefix)"

# Check if maturin is installed
if (-not (Get-Command maturin -ErrorAction SilentlyContinue)) {
    Write-Host "maturin not found, installing..."
    cargo install maturin
}

# Get the script directory
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Definition
Write-Host "Home directory: $HOME"
Write-Host "Python home: $env:PYTHON_HOME"
Write-Host "Script directory: $SCRIPT_DIR"

# List home directory contents
Get-ChildItem -Path $HOME

# List contents of /home/cloudtest/.local/bin
Get-ChildItem -Path "/home/cloudtest/.local/bin"

# Check if pipenv is on the PATH
if (-not (Get-Command pipenv -ErrorAction SilentlyContinue)) {
    Write-Host "pipenv not found on PATH, using fully qualified name..."
    $PIPENV_CMD = "/home/cloudtest/.local/bin/pipenv"
} else {
    $PIPENV_CMD = "pipenv"
}
Write-Host "Using pipenv command: $PIPENV_CMD"

# Run pipenv commands
& $PIPENV_CMD run pip install patchelf
& $PIPENV_CMD run maturin build --frozen --manifest-path "$SCRIPT_DIR/../tdsx-python/Cargo.toml"