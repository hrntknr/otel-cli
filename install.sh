#!/bin/sh
set -eu

REPO="hrntknr/otel-cli"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
BINARY="otel-cli"

main() {
    detect_platform
    fetch_latest_version
    download_binary
    install_binary
    print_success
}

detect_platform() {
    OS="$(uname -s)"
    ARCH="$(uname -m)"
    EXE=""

    case "$OS" in
        Linux)
            case "$ARCH" in
                x86_64) TARGET="x86_64-unknown-linux-gnu" ;;
                aarch64|arm64) TARGET="aarch64-unknown-linux-gnu" ;;
                *) abort "Unsupported architecture: $ARCH" ;;
            esac
            ;;
        Darwin)
            case "$ARCH" in
                arm64) TARGET="aarch64-apple-darwin" ;;
                *) abort "Unsupported architecture: $ARCH" ;;
            esac
            ;;
        MINGW*|MSYS*|CYGWIN*)
            EXE=".exe"
            case "$ARCH" in
                x86_64) TARGET="x86_64-pc-windows-msvc" ;;
                aarch64|arm64) TARGET="aarch64-pc-windows-msvc" ;;
                *) abort "Unsupported architecture: $ARCH" ;;
            esac
            ;;
        *) abort "Unsupported OS: $OS" ;;
    esac
}

fetch_latest_version() {
    VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
        | grep '"tag_name"' | head -1 | cut -d'"' -f4)"

    if [ -z "$VERSION" ]; then
        abort "Failed to fetch latest version"
    fi
}

download_binary() {
    URL="https://github.com/${REPO}/releases/download/${VERSION}/${BINARY}-${TARGET}${EXE}"
    TMPDIR="$(mktemp -d)"
    TMPFILE="${TMPDIR}/${BINARY}${EXE}"

    info "Downloading ${BINARY} ${VERSION} for ${TARGET}..."
    curl -fsSL -o "$TMPFILE" "$URL"
    chmod +x "$TMPFILE"
}

install_binary() {
    if [ -w "$INSTALL_DIR" ]; then
        mv "$TMPFILE" "${INSTALL_DIR}/${BINARY}${EXE}"
    else
        info "Installing to ${INSTALL_DIR} (requires sudo)..."
        sudo mv "$TMPFILE" "${INSTALL_DIR}/${BINARY}${EXE}"
    fi
    rm -rf "$TMPDIR"
}

print_success() {
    cat <<EOF

  otel-cli ${VERSION} installed to ${INSTALL_DIR}/${BINARY}${EXE}

  Get started:
    $ otel-cli server            Start server with interactive TUI
    $ otel-cli --help            Show all commands

EOF
}

info() {
    printf '%s\n' "$*"
}

abort() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

main
