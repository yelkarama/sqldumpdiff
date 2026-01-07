#!/bin/bash

# Build native executable using GraalVM
# Requires GraalVM JDK to be installed and JAVA_HOME set

set -e

echo "=========================================="
echo "GraalVM Native Image Build"
echo "=========================================="
echo ""

# Check if GraalVM is installed
if ! command -v native-image &> /dev/null; then
    echo "Error: native-image not found!"
    echo ""
    echo "Install GraalVM:"
    echo "  brew install --cask graalvm-jdk"
    echo ""
    echo "Or download from: https://www.graalvm.org/downloads/"
    echo ""
    echo "Then install native-image:"
    echo "  \$JAVA_HOME/bin/gu install native-image"
    exit 1
fi

echo "GraalVM detected:"
java -version
echo ""

echo "Step 1: Clean and build JAR..."
mvn clean package -DskipTests
echo ""

echo "Step 2: Generate reflection configuration (if needed)..."
if [ ! -f src/main/resources/META-INF/native-image/reflect-config.json ]; then
    echo "Run with tracing agent first:"
    echo "  ./profile.sh --native-config 11.sql 22.sql"
    echo ""
fi

echo "Step 3: Building native image (this may take 2-5 minutes)..."
mvn -Pnative -DskipTests package
echo ""

echo "=========================================="
echo "Native image build complete!"
echo "=========================================="
echo ""
echo "Executable: target/sqldumpdiff"
echo ""
echo "Test it:"
echo "  ./target/sqldumpdiff old.sql new.sql > delta.sql"
echo ""

# Show file size comparison
if [ -f target/sqldumpdiff-1.0.0.jar ]; then
    JAR_SIZE=$(du -h target/sqldumpdiff-1.0.0.jar | cut -f1)
    echo "JAR size: $JAR_SIZE"
fi

if [ -f target/sqldumpdiff ]; then
    NATIVE_SIZE=$(du -h target/sqldumpdiff | cut -f1)
    echo "Native size: $NATIVE_SIZE"
    echo ""
    echo "Startup time comparison:"
    echo "  JAR:    ~100-200ms (JVM warmup)"
    echo "  Native: ~1-5ms (instant startup)"
fi
