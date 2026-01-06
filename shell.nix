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

    # Build dependencies
    pkg-config
    openssl

    # Database utilities
    pgcli  # Better PostgreSQL CLI
  ];

  shellHook = ''
    # Environment variables for Rust builds
    export OPENSSL_DIR = "${pkgs.openssl.dev}";
    export OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
    export PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";

    # PostgreSQL environment setup
    export PGDATA="$PWD/.postgres-data"
    export PGHOST="$PWD/.postgres-socket"
    export PGPORT="5432"
    export PGDATABASE="kimp_station"

    # Cleanup on exit
    trap 'echo ""; if [ -f "$PGDATA/postmaster.pid" ]; then echo "👋 Stopping PostgreSQL..."; pg_ctl stop -D $PGDATA -m fast; fi' EXIT

    echo "🚀 Kimp Station Development Environment"
    echo "======================================="
    
    # Run the universal setup script
    ./scripts/setup_timescaledb.sh

    echo ""
    echo "📋 Available Commands:"
    echo "  cargo run           - Run the application"
    echo "  pgcli               - Connect to Database"
    echo ""
  '';
}
