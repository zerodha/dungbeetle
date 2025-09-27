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
CI_MODE=${CI:-false}
SERVER_PID=""

# Project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"
BINARY_NAME="dungbeetle.test.bin"

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

# Determine docker compose command (prefer newer 'docker compose' over legacy 'docker-compose')
if [ "$CI_MODE" = true ]; then
    DOCKER_COMPOSE=""
elif docker compose version > /dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose > /dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    log_error "Neither docker compose nor docker-compose is available"
    exit 1
fi

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
    wait_for_service "PostgreSQL" "docker exec dungbeetle-test-postgres pg_isready -h 127.0.0.1 -U testUser -d testDB"
    wait_for_service "MySQL" "docker exec dungbeetle-test-mysql mysql --protocol=TCP -h 127.0.0.1 -u root --password=rootpassword -e 'SELECT 1'"

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
        );" > /dev/null

    # MySQL table
    docker exec dungbeetle-test-mysql mysql -u root -prootpassword mydb -e "
        CREATE TABLE IF NOT EXISTS entries (
            id BIGINT PRIMARY KEY,
            amount REAL,
            user_id VARCHAR(6),
            entry_date DATE,
            timestamp TIMESTAMP
        );" > /dev/null

    build_binary

    log_success "Test environment setup complete"
}

build_binary() {
    # Build binary
    log_info "Building dungbeetle binary..."
    cd "$PROJECT_ROOT"
    CGO_ENABLED=0 go build -o "$BINARY_NAME" -ldflags="-s -w" ./cmd/*.go
}

stop_server() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
    fi
}

wait_for_server() {
    local attempt
    for attempt in {1..30}; do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            log_error "Test server exited before becoming ready"
            return 1
        fi
        if curl --fail --silent --max-time 1 http://127.0.0.1:6060/tasks > /dev/null; then
            return 0
        fi
        sleep 1
    done
    log_error "Test server did not become ready"
    return 1
}

# Run tests
run_tests() {
    log_info "Running tests..."

    cd "$PROJECT_ROOT"
    local test_result=0

    if (echo > /dev/tcp/127.0.0.1/6060) 2>/dev/null; then
        log_error "Port 6060 is already in use; stop the existing server first"
        return 1
    fi

    # Test PostgreSQL
    if [ "$DATABASE" = "all" ] || [ "$DATABASE" = "postgres" ]; then
        log_info "Running PostgreSQL tests..."

        # Start server with PostgreSQL config
        ./"$BINARY_NAME" --config config.test_pg.toml --sql-directory=sql/pg &
        SERVER_PID=$!

        # Wait for server to start
        wait_for_server

        # Run tests
        if go test ./client -count=1 -v -covermode=count; then
            log_success "PostgreSQL tests passed"
        else
            log_error "PostgreSQL tests failed"
            test_result=1
        fi

        # Stop server
        stop_server
    fi

    # Test MySQL
    if [ "$DATABASE" = "all" ] || [ "$DATABASE" = "mysql" ]; then
        log_info "Running MySQL tests..."

        # Start server with MySQL config
        ./"$BINARY_NAME" --config config.test_mysql.toml --sql-directory=sql/mysql &
        SERVER_PID=$!

        # Wait for server to start
        wait_for_server

        # Run tests
        if go test ./client -count=1 -v -covermode=count; then
            log_success "MySQL tests passed"
        else
            log_error "MySQL tests failed"
            test_result=1
        fi

        # Stop server
        stop_server
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

    local result=0

    # Stop and remove containers
    if [ -f "$DOCKER_COMPOSE_FILE" ]; then
        $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" down -v || result=$?
    fi

    # Remove binary
    rm -f "$BINARY_NAME"

    if [ "$result" -eq 0 ]; then
        log_success "Cleanup complete"
    fi
    return "$result"
}

# Always stop the server; complete cycles also clean up on failure or interruption.
finish() {
    local result=$?
    trap - EXIT
    stop_server
    if [ "$MODE" = "all" ] && [ "$CI_MODE" != true ]; then
        cleanup_environment || result=1
    fi
    exit "$result"
}

# Main execution
main() {
    log_info "DungBeetle Test Script"
    log_info "Mode: $MODE, Database: $DATABASE, CI Mode: $CI_MODE"

    trap finish EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    # Skip setup/cleanup in CI mode
    if [ "$CI_MODE" = true ]; then
        log_info "CI mode detected, skipping setup/cleanup"
        build_binary
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
            ;;
        *)
            log_error "Unknown mode: $MODE"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
