{ pkgs ? import <nixpkgs> {
    config = {
      allowBroken = true;
      allowUnfree = true;
    };
  }
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # PostgreSQL with TimescaleDB
    (postgresql.withPackages (p: [ p.timescaledb ]))

    # Rust toolchain
    cargo
    rustc
    rust-analyzer
    rustfmt
    clippy
    screen

    # Build dependencies
    pkg-config
    openssl

    # Database utilities
    pgcli  # Better PostgreSQL CLI

    # Grafana
    grafana

    # Task runner
    just
  ];

  shellHook = ''
    # Environment variables for Rust builds
    export OPENSSL_DIR="${pkgs.openssl.dev}"
    export OPENSSL_LIB_DIR="${pkgs.openssl.out}/lib"
    export PKG_CONFIG_PATH="${pkgs.openssl.dev}/lib/pkgconfig"

    # PostgreSQL environment setup
    export PGDATA="$PWD/.postgres-data"
    export PGHOST="$PWD/.postgres-socket"
    export PGPORT="5432"
    export PGDATABASE="kimp_station"

    # Alias to stop the database manually
    alias stop_db="pg_ctl stop -D $PGDATA -m fast"

    echo "🚀 Kimp Station Development Environment"
    echo "======================================="
    
    # Run the universal setup script
    ./scripts/setup_timescaledb.sh

    # Grafana environment setup
    export GRAFANA_DATA="$PWD/.grafana-data"
    mkdir -p "$GRAFANA_DATA/dashboards"

    # Copy provisioning configs if not exists
    if [ ! -d "$GRAFANA_DATA/provisioning" ]; then
      cp -r "$PWD/grafana/provisioning" "$GRAFANA_DATA/"
      cp "$PWD/grafana/dashboards/"*.json "$GRAFANA_DATA/dashboards/"
    fi

    echo ""
    echo "📋 Available Commands:"
    echo "  just run            - Run the application"
    echo "  just grafana        - Start Grafana dashboard"
    echo "  just grafana-stop   - Stop Grafana"
    echo "  pgcli               - Connect to Database"
    echo "  stop_db             - Stop PostgreSQL"
    echo ""
  '';
}
