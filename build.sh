#! /bin/bash

cd $(dirname $0)

# Ensure boost directory exists and download if needed
echo "Checking boost dependency..."
mkdir -p boost
if [ ! -f "boost/boost_1_77_0.tar.bz2" ]; then
    echo "Downloading boost 1.77.0..."
    cd boost
    wget -q https://sourceforge.net/projects/boost/files/boost/1.77.0/boost_1_77_0.tar.bz2
    tar xfj boost_1_77_0.tar.bz2
    cd ..
fi

# Apply LineairDB CMakeLists.txt fixes for compilation warnings
echo "Applying LineairDB compilation fixes..."
LINEAIRDB_CMAKE="third_party/LineairDB/CMakeLists.txt"
if ! grep -q "target_compile_options.*-Wno-error" "$LINEAIRDB_CMAKE"; then
    # Add include(GNUInstallDirs) if not present
    if ! grep -q "include(GNUInstallDirs)" "$LINEAIRDB_CMAKE"; then
        sed -i '/cmake_minimum_required/a include(GNUInstallDirs)' "$LINEAIRDB_CMAKE"
    fi
    
    # Add -Wno-error flag to LineairDB library
    sed -i '/add_library.*SOURCES/a target_compile_options(${PROJECT_NAME} PRIVATE -Wno-error)' "$LINEAIRDB_CMAKE"
    echo "Applied LineairDB compilation fixes"
fi

# Prepare build directory with clean structure
echo "Setting up build directory structure..."
mkdir -p build/data
mkdir -p build/lineairdb_proxy
mkdir -p build/lineairdb_service

# Build lineairdb_service
echo "Building lineairdb_service..."
cd build/lineairdb_service
cmake ../../lineairdb_service \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=1
make -j `nproc`
cd ../..

# Create lineairdb_proxy copy with necessary dependencies in build directory
echo "Creating lineairdb_proxy build structure..."
cp -r lineairdb_proxy build/
# Create symbolic links in build directory only
mkdir -p build/lineairdb_proxy/third_party
ln -sf $(pwd)/third_party/LineairDB build/lineairdb_proxy/third_party/LineairDB
ln -sf $(pwd)/proto build/lineairdb_proxy/proto

# Create MySQL storage engine link to build directory version
ln -sf $(pwd)/build/lineairdb_proxy third_party/mysql-server/storage/lineairdb

# Build MySQL with lineairdb_proxy storage engine  
echo "Building MySQL with lineairdb_proxy..."
cd build

cmake ../third_party/mysql-server \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
    -DWITH_BUILD_ID=0 \
    -DWITH_ASAN=0 \
    -DCMAKE_BUILD_TYPE=Debug \
    -DDOWNLOAD_BOOST=0 \
    -DWITH_BOOST=../boost/boost_1_77_0 \
    -DWITHOUT_EXAMPLE_STORAGE_ENGINE=1 \
    -DWITHOUT_FEDERATED_STORAGE_ENGINE=1 \
    -DWITHOUT_ARCHIVE_STORAGE_ENGINE=1 \
    -DWITHOUT_BLACKHOLE_STORAGE_ENGINE=0 \
    -DWITHOUT_NDB_STORAGE_ENGINE=1 \
    -DWITHOUT_NDBCLUSTER_STORAGE_ENGINE=1 \
    -DWITHOUT_PARTITION_STORAGE_ENGINE=1 \
    -G Ninja

ninja $1 -j `nproc`