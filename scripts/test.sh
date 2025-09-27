#!/usr/bin/env bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
MODE="all"
DATABASE="all"
VERBOSE=false
CI_MODE=false

# Project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"
BINARY_NAME="dungbeetle.test.bin"

# Determine docker compose command (prefer newer 'docker compose' over legacy 'docker-compose')
if docker compose version > /dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose > /dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    log_error "Neither docker compose nor docker-compose is available"
    exit 1
fi

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        setup|run|cleanup|all)
            MODE="$1"
            shift
            ;;
        --postgres|--mysql|--all)
            DATABASE="${1#--}"
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --ci)
            CI_MODE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [MODE] [OPTIONS]"
            echo ""
            echo "Modes:"
            echo "  setup     Setup test environment (start services, create tables)"
            echo "  run       Run tests (services must be running)"
            echo "  cleanup   Stop services and cleanup"
            echo "  all       Setup → Run tests → Cleanup (default)"
            echo ""
            echo "Database Options:"
            echo "  --postgres  Test PostgreSQL only"
            echo "  --mysql     Test MySQL only"
            echo "  --all       Test both databases (default)"
            echo ""
            echo "Other Options:"
            echo "  -v, --verbose  Verbose output"
            echo "  --ci          CI mode (skip setup/cleanup)"
            echo "  -h, --help     Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_verbose() {
    if [ "$VERBOSE" = true ]; then
        echo -e "${BLUE}[VERBOSE]${NC} $1"
    fi
}

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        log_error "Docker is not running. Please start Docker."
        exit 1
    fi
}

# Wait for service to be healthy
wait_for_service() {
    local service_name="$1"
    local health_check="$2"
    local max_attempts=30
    local attempt=1

    log_info "Waiting for $service_name to be healthy..."

    while [ $attempt -le $max_attempts ]; do
        if eval "$health_check" > /dev/null 2>&1; then
            log_success "$service_name is healthy"
            return 0
        fi

        log_verbose "Attempt $attempt/$max_attempts: $service_name not ready yet"
        sleep 2
        attempt=$((attempt + 1))
    done

    log_error "$service_name failed to become healthy"
    return 1
}

# Setup test environment
setup_environment() {
    log_info "Setting up test environment..."

    check_docker

    # Start services
    log_info "Starting test services..."
    $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" up -d

    # Wait for services to be healthy
    wait_for_service "Redis" "docker exec dungbeetle-test-redis redis-cli ping"
    wait_for_service "PostgreSQL" "docker exec dungbeetle-test-postgres pg_isready -U testUser -d testDB"
    wait_for_service "MySQL" "docker exec dungbeetle-test-mysql mysqladmin ping -h localhost -u root --password=rootpassword"

    # Create test tables
    log_info "Creating test tables..."

    # PostgreSQL table
    docker exec dungbeetle-test-postgres psql -U testUser -d testDB -c "
        CREATE TABLE IF NOT EXISTS entries (
            id BIGSERIAL PRIMARY KEY,
            amount REAL,
            user_id VARCHAR(6),
            entry_date DATE,
            timestamp TIMESTAMP
        );" > /dev/null 2>&1

    # MySQL table
    docker exec dungbeetle-test-mysql mysql -u root -prootpassword mydb -e "
        CREATE TABLE IF NOT EXISTS entries (
            id BIGINT PRIMARY KEY,
            amount REAL,
            user_id VARCHAR(6),
            entry_date DATE,
            timestamp TIMESTAMP
        );" > /dev/null 2>&1

    # Build binary
    log_info "Building dungbeetle binary..."
    cd "$PROJECT_ROOT"
    CGO_ENABLED=0 go build -o "$BINARY_NAME" -ldflags="-s -w" ./cmd/*.go

    log_success "Test environment setup complete"
}

# Run tests
run_tests() {
    log_info "Running tests..."

    cd "$PROJECT_ROOT"
    local test_result=0

    # Test PostgreSQL
    if [ "$DATABASE" = "all" ] || [ "$DATABASE" = "postgres" ]; then
        log_info "Running PostgreSQL tests..."

        # Start server with PostgreSQL config
        ./"$BINARY_NAME" --config config.test_pg.toml --sql-directory=sql/pg &
        local server_pid=$!

        # Wait for server to start
        sleep 5

        # Run tests
        if go test ./client -v -covermode=count; then
            log_success "PostgreSQL tests passed"
        else
            log_error "PostgreSQL tests failed"
            test_result=1
        fi

        # Stop server
        kill $server_pid 2>/dev/null || true
        wait $server_pid 2>/dev/null || true
    fi

    # Test MySQL
    if [ "$DATABASE" = "all" ] || [ "$DATABASE" = "mysql" ]; then
        log_info "Running MySQL tests..."

        # Start server with MySQL config
        ./"$BINARY_NAME" --config config.test_mysql.toml --sql-directory=sql/mysql &
        local server_pid=$!

        # Wait for server to start
        sleep 5

        # Run tests
        if go test ./client -v -covermode=count; then
            log_success "MySQL tests passed"
        else
            log_error "MySQL tests failed"
            test_result=1
        fi

        # Stop server
        kill $server_pid 2>/dev/null || true
        wait $server_pid 2>/dev/null || true
    fi

    if [ $test_result -eq 0 ]; then
        log_success "All tests passed"
    else
        log_error "Some tests failed"
    fi

    return $test_result
}

# Cleanup test environment
cleanup_environment() {
    log_info "Cleaning up test environment..."

    cd "$PROJECT_ROOT"

    # Stop and remove containers
    if [ -f "$DOCKER_COMPOSE_FILE" ]; then
        $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" down -v
    fi

    # Remove binary
    rm -f "$BINARY_NAME"

    log_success "Cleanup complete"
}

# Main execution
main() {
    log_info "DungBeetle Test Script"
    log_info "Mode: $MODE, Database: $DATABASE, CI Mode: $CI_MODE"

    # Skip setup/cleanup in CI mode
    if [ "$CI_MODE" = true ]; then
        log_info "CI mode detected, skipping setup/cleanup"
        run_tests
        exit $?
    fi

    case $MODE in
        setup)
            setup_environment
            ;;
        run)
            run_tests
            ;;
        cleanup)
            cleanup_environment
            ;;
        all)
            setup_environment
            run_tests
            cleanup_environment
            ;;
        *)
            log_error "Unknown mode: $MODE"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
