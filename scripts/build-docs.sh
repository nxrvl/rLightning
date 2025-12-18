#!/bin/bash

# Script to build rLightning documentation
# Usage: ./scripts/build-docs.sh [options]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SITE_DIR="$PROJECT_ROOT/site"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print colored message
print_message() {
    local color=$1
    shift
    echo -e "${color}$@${NC}"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Build rLightning documentation site

OPTIONS:
    -h, --help          Show this help message
    -s, --serve         Serve documentation locally with live reload
    -b, --build         Build static documentation (default)
    -d, --docker        Build Docker image
    -r, --run           Run documentation in Docker container
    -c, --clean         Clean build artifacts
    --check             Check for broken links
    --install-deps      Install Python dependencies

EXAMPLES:
    $0 --serve          # Serve docs with live reload at http://localhost:8000
    $0 --build          # Build static site
    $0 --docker --run   # Build and run in Docker
EOF
}

# Install dependencies
install_deps() {
    print_message "$YELLOW" "Installing MkDocs dependencies..."

    if ! command_exists pip && ! command_exists pip3; then
        print_message "$RED" "Error: pip not found. Please install Python and pip first."
        exit 1
    fi

    local pip_cmd="pip"
    command_exists pip3 && pip_cmd="pip3"

    $pip_cmd install mkdocs-material mkdocs-minify-plugin mkdocs-git-revision-date-localized-plugin

    print_message "$GREEN" "Dependencies installed successfully!"
}

# Check dependencies
check_deps() {
    if ! command_exists mkdocs; then
        print_message "$RED" "Error: mkdocs not found."
        print_message "$YELLOW" "Install with: pip install mkdocs-material"
        print_message "$YELLOW" "Or run: $0 --install-deps"
        exit 1
    fi
}

# Build documentation
build_docs() {
    print_message "$YELLOW" "Building documentation..."

    check_deps

    cd "$SITE_DIR"
    mkdocs build --strict

    print_message "$GREEN" "Documentation built successfully!"
    print_message "$GREEN" "Output: $SITE_DIR/site/"
}

# Serve documentation
serve_docs() {
    print_message "$YELLOW" "Starting documentation server..."
    print_message "$YELLOW" "Access at: http://localhost:8000"
    print_message "$YELLOW" "Press Ctrl+C to stop"

    check_deps

    cd "$SITE_DIR"
    mkdocs serve
}

# Build Docker image
build_docker() {
    print_message "$YELLOW" "Building Docker image for documentation..."

    if ! command_exists docker; then
        print_message "$RED" "Error: docker not found. Please install Docker first."
        exit 1
    fi

    cd "$PROJECT_ROOT"
    docker build -f Dockerfile.site -t rlightning-docs:latest .

    print_message "$GREEN" "Docker image built successfully!"
    print_message "$GREEN" "Image: rlightning-docs:latest"
}

# Run Docker container
run_docker() {
    print_message "$YELLOW" "Starting documentation container..."

    # Stop existing container if running
    docker stop rlightning-docs 2>/dev/null || true
    docker rm rlightning-docs 2>/dev/null || true

    docker run -d \
        --name rlightning-docs \
        -p 8080:8080 \
        rlightning-docs:latest

    print_message "$GREEN" "Documentation container started!"
    print_message "$GREEN" "Access at: http://localhost:8080"
    print_message "$YELLOW" "Stop with: docker stop rlightning-docs"
}

# Clean build artifacts
clean_docs() {
    print_message "$YELLOW" "Cleaning build artifacts..."

    rm -rf "$SITE_DIR/site"

    print_message "$GREEN" "Build artifacts cleaned!"
}

# Check for broken links
check_links() {
    print_message "$YELLOW" "Checking for broken links..."

    if [ ! -d "$SITE_DIR/site" ]; then
        print_message "$RED" "Error: Built site not found. Run with --build first."
        exit 1
    fi

    # Simple check for common issues
    find "$SITE_DIR/site" -name "*.html" -type f -exec grep -l "404" {} \; || true

    print_message "$GREEN" "Link check complete!"
}

# Main script logic
main() {
    if [ $# -eq 0 ]; then
        build_docs
        exit 0
    fi

    while [ $# -gt 0 ]; do
        case "$1" in
            -h|--help)
                usage
                exit 0
                ;;
            -s|--serve)
                serve_docs
                exit 0
                ;;
            -b|--build)
                build_docs
                exit 0
                ;;
            -d|--docker)
                build_docker
                shift
                ;;
            -r|--run)
                run_docker
                shift
                ;;
            -c|--clean)
                clean_docs
                exit 0
                ;;
            --check)
                check_links
                exit 0
                ;;
            --install-deps)
                install_deps
                exit 0
                ;;
            *)
                print_message "$RED" "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
}

main "$@"
