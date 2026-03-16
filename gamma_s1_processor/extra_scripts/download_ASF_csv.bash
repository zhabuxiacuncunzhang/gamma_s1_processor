#!/bin/bash

# Configuration
USERNAME="Zhang_Xuesong"
PASSWORD="Snzzbyc123"
THREADS=5
RETRY=3

# Check arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 <csv_file> [options]"
    echo "Options:"
    echo "  -o <dir>      Output directory (default: ./downloads)"
    echo "  -u <user>     Username"
    echo "  -p <pass>     Password"
    echo "  -t <threads>  Parallel downloads (default: 3)"
    echo "  -r <retry>    Retry count (default: 3)"
    exit 1
fi

# Parse arguments
CSV_FILE="$1"
shift
OUTPUT_DIR="./"

while [[ $# -gt 0 ]]; do
    case $1 in
        -o) OUTPUT_DIR="$2"; shift 2 ;;
        -u) USERNAME="$2"; shift 2 ;;
        -p) PASSWORD="$2"; shift 2 ;;
        -t) THREADS="$2"; shift 2 ;;
        -r) RETRY="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Counters
TOTAL=0
SUCCESS=0
FAIL=0
SKIP=0
SIZE_MISMATCH=0

# Create temp files
URL_FILE="./urls.txt"
SIZE_MAP="./size_map.txt"
FAIL_FILE="./failed.txt"
LOG_FILE="./download.log"
MISMATCH_FILE="./size_mismatch.txt"

> "$URL_FILE"
> "$SIZE_MAP"
> "$FAIL_FILE"
> "$LOG_FILE"
> "$MISMATCH_FILE"

echo "=========================================="
echo "Starting Sentinel-1 download"
echo "=========================================="
echo "CSV: $CSV_FILE"
echo "Output: $OUTPUT_DIR"
echo "Threads: $THREADS"
echo "Start: $(date '+%H:%M:%S')"
echo "------------------------------------------"

# Extract URLs and sizes
echo "Extracting URLs and sizes from CSV..."
line_num=0

while IFS=, read -r -a cols; do
    ((line_num++))
    
    # Skip header
    if [ $line_num -eq 1 ]; then
        continue
    fi
    
    # Check if we have enough columns
    if [ ${#cols[@]} -lt 27 ]; then
        continue
    fi
    
    url=$(echo "${cols[26]}" | tr -d '"')
    expected_size=$(echo "${cols[27]}" | tr -d '"')
    
    if [ -n "$url" ] && [ "$url" != "" ]; then
        filename=$(basename "$url")
        filepath="$OUTPUT_DIR/$filename"
        
        # Check if file exists and size matches
        if [ -f "$filepath" ]; then
            if [ -n "$expected_size" ] && [ "$expected_size" != "" ]; then
                actual_size=$(du -m "$filepath" 2>/dev/null | cut -f1 || echo "0")
                expected_int=$(printf "%.0f" "$expected_size" 2>/dev/null || echo "0")
                
                # Check if size is within 10%
                if [ "$expected_int" -gt 0 ] && [ "$actual_size" -gt 0 ]; then
                    diff_percent=$(( (actual_size - expected_int) * 100 / expected_int ))
                    if [ ${diff_percent#-} -lt 10 ]; then  # Within ¡À10%
                        echo "SKIP: $filename (exists, size OK: ${actual_size}MB)"
                        ((SKIP++))
                        continue
                    else
                        echo "REDOWNLOAD: $filename (size mismatch: ${actual_size}MB vs ${expected_int}MB)"
                    fi
                fi
            fi
        fi
        
        echo "$url" >> "$URL_FILE"
        echo "$filename|$expected_size" >> "$SIZE_MAP"
        ((TOTAL++))
    fi
done < "$CSV_FILE"

echo "Found $TOTAL files to download"
echo "Skipped $SKIP files (already downloaded with correct size)"

if [ $TOTAL -eq 0 ]; then
    echo "No files to download"
    exit 0
fi

# Download function with size verification
download_and_verify() {
    local url="$1"
    local filename=$(basename "$url")
    local filepath="$OUTPUT_DIR/$filename"
    local attempts=0
    local success=0
    
    # Get expected size from map
    local expected_size=$(grep "^$filename|" "$SIZE_MAP" | cut -d'|' -f2)
    
    while [ $attempts -lt $RETRY ] && [ $success -eq 0 ]; do
        ((attempts++))
        
        echo "Downloading ($attempts/$RETRY): $filename"
        
        # Download file
        aria2c --http-user="$USERNAME" --http-passwd="$PASSWORD" \
               --max-connection-per-server=5 \
               --split=5 \
               --dir="$OUTPUT_DIR" \
               --out="$filename" \
               --summary-interval=0 \
               --quiet=true \
               "$url" > /dev/null 2>&1
        
        if [ $? -eq 0 ]; then
            # Verify file size
            if [ -n "$expected_size" ] && [ "$expected_size" != "" ]; then
                actual_size=$(du -m "$filepath" 2>/dev/null | cut -f1 || echo "0")
                expected_int=$(printf "%.0f" "$expected_size" 2>/dev/null || echo "0")
                
                if [ "$expected_int" -eq 0 ]; then
                    echo "OK: $filename (no expected size to verify)"
                    success=1
                elif [ "$actual_size" -ge $((expected_int * 9 / 10)) ]; then  # At least 90%
                    echo "OK: $filename (size OK: ${actual_size}MB)"
                    success=1
                else
                    echo "SIZE MISMATCH: $filename (got ${actual_size}MB, expected ${expected_int}MB)"
                    echo "$url|${actual_size}MB|${expected_int}MB" >> "$MISMATCH_FILE"
                    rm -f "$filepath"  # Remove incomplete file
                    sleep 2
                fi
            else
                echo "OK: $filename (downloaded, no size check)"
                success=1
            fi
        else
            echo "FAILED attempt $attempts: $filename"
            sleep 2
        fi
    done
    
    if [ $success -eq 1 ]; then
        return 0
    else
        echo "FAIL: $filename (all attempts failed)"
        echo "$url" >> "$FAIL_FILE"
        return 1
    fi
}

# Export function for parallel execution
export -f download_and_verify
export USERNAME PASSWORD OUTPUT_DIR RETRY FAIL_FILE SIZE_MAP MISMATCH_FILE

# Download files in parallel
echo "------------------------------------------"
echo "Downloading $TOTAL files with $THREADS threads..."

# Use xargs for parallel downloads
cat "$URL_FILE" | xargs -P "$THREADS" -I {} bash -c 'download_and_verify "{}"'

# Count results
SUCCESS=$((TOTAL - $(wc -l < "$FAIL_FILE" 2>/dev/null || echo 0)))
FAIL=$(wc -l < "$FAIL_FILE" 2>/dev/null || echo 0)
SIZE_MISMATCH=$(wc -l < "$MISMATCH_FILE" 2>/dev/null || echo 0)

# Adjust success count (subtract size mismatches)
SUCCESS=$((SUCCESS - SIZE_MISMATCH))

# Final report
echo "=========================================="
echo "Download completed with size verification"
echo "=========================================="
echo "Time: $(date '+%H:%M:%S')"
echo "------------------------------------------"
echo "Total in CSV:    $((TOTAL + SKIP))"
echo "Skipped (OK):    $SKIP"
echo "To download:     $TOTAL"
echo "Successful:      $SUCCESS"
echo "Size mismatch:   $SIZE_MISMATCH"
echo "Failed:          $FAIL"
echo "------------------------------------------"

if [ $TOTAL -gt 0 ]; then
    rate=$((SUCCESS * 100 / TOTAL))
    echo "Success rate:    $rate%"
fi

# List files with size mismatch
if [ $SIZE_MISMATCH -gt 0 ]; then
    echo "------------------------------------------"
    echo "Files with size mismatch:"
    cat "$MISMATCH_FILE" | while IFS='|' read url actual expected; do
        echo "$(basename "$url"): Got ${actual}, Expected ${expected}"
    done
fi

# List failed files
if [ $FAIL -gt 0 ]; then
    echo "------------------------------------------"
    echo "Failed files:"
    cat "$FAIL_FILE" | xargs -I {} basename {}
fi

echo "=========================================="

# Cleanup temp files
rm -f "$URL_FILE" "$SIZE_MAP"

# Keep important files
if [ $SIZE_MISMATCH -eq 0 ]; then
    rm -f "$MISMATCH_FILE"
else
    echo "Size mismatch list: $MISMATCH_FILE"
fi

if [ $FAIL -eq 0 ]; then
    rm -f "$FAIL_FILE"
else
    echo "Failed URLs: $FAIL_FILE"
fi

echo "Output directory: $OUTPUT_DIR"
du -sh "$OUTPUT_DIR"