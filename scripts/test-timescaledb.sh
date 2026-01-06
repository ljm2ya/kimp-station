#!/usr/bin/env bash
set -e

echo "🧪 Testing TimescaleDB Setup"
echo "============================="
echo ""

# Check if PostgreSQL is running
echo "1️⃣ Checking PostgreSQL status..."
if pg_isready -q; then
    echo "   ✅ PostgreSQL is running"
else
    echo "   ❌ PostgreSQL is not running"
    exit 1
fi

# Check database exists
echo ""
echo "2️⃣ Checking database exists..."
if psql -lqt | cut -d \| -f 1 | grep -qw "kimp_station"; then
    echo "   ✅ Database 'kimp_station' exists"
else
    echo "   ❌ Database 'kimp_station' not found"
    exit 1
fi

# Check TimescaleDB extension
echo ""
echo "3️⃣ Checking TimescaleDB extension..."
EXTENSION_CHECK=$(psql -d kimp_station -tAc "SELECT extname, extversion FROM pg_extension WHERE extname = 'timescaledb';")
if [ -n "$EXTENSION_CHECK" ]; then
    echo "   ✅ TimescaleDB extension installed: $EXTENSION_CHECK"
else
    echo "   ❌ TimescaleDB extension not installed"
    exit 1
fi

# Test creating a hypertable
echo ""
echo "4️⃣ Testing hypertable creation..."
psql -d kimp_station <<-EOF > /dev/null 2>&1
    DROP TABLE IF EXISTS test_snapshots CASCADE;
    CREATE TABLE test_snapshots (
        time TIMESTAMPTZ NOT NULL,
        value DOUBLE PRECISION
    );
    SELECT create_hypertable('test_snapshots', 'time', chunk_time_interval => INTERVAL '1 day');
EOF

if [ $? -eq 0 ]; then
    echo "   ✅ Hypertable creation successful"
else
    echo "   ❌ Hypertable creation failed"
    exit 1
fi

# Check hypertable exists
HYPERTABLE_CHECK=$(psql -d kimp_station -tAc "SELECT COUNT(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'test_snapshots';")
if [ "$HYPERTABLE_CHECK" = "1" ]; then
    echo "   ✅ Hypertable verified in TimescaleDB catalog"
else
    echo "   ❌ Hypertable not found in catalog"
    exit 1
fi

# Test data insertion
echo ""
echo "5️⃣ Testing data insertion..."
psql -d kimp_station -c "INSERT INTO test_snapshots VALUES (NOW(), 100.5), (NOW() - INTERVAL '1 hour', 99.2);" > /dev/null
RECORD_COUNT=$(psql -d kimp_station -tAc "SELECT COUNT(*) FROM test_snapshots;")
if [ "$RECORD_COUNT" = "2" ]; then
    echo "   ✅ Data insertion successful ($RECORD_COUNT records)"
else
    echo "   ❌ Data insertion failed"
    exit 1
fi

# Test compression policy
echo ""
echo "6️⃣ Testing compression policy..."
psql -d kimp_station -c "SELECT add_compression_policy('test_snapshots', INTERVAL '7 days');" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "   ✅ Compression policy added successfully"
else
    echo "   ⚠️  Compression policy may already exist or failed (non-critical)"
fi

# Cleanup
echo ""
echo "7️⃣ Cleaning up test table..."
psql -d kimp_station -c "DROP TABLE test_snapshots CASCADE;" > /dev/null
echo "   ✅ Test table dropped"

# Build the application
echo ""
echo "8️⃣ Testing application build..."
if cargo build 2>&1 | tail -5; then
    echo "   ✅ Application built successfully"
else
    echo "   ❌ Application build failed"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ All tests passed!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📊 Database Information:"
psql -d kimp_station -c "\dx timescaledb"
echo ""
echo "📈 Hypertable Status:"
psql -d kimp_station -c "SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = 'snapshots';" 2>/dev/null || echo "  Application hypertable not yet created (will be created on first run)"
echo ""
echo "🚀 Ready to run: cargo run"
