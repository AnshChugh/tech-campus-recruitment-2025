#!/usr/bin/env python3
import os
import sys
import mmap
import datetime
from pathlib import Path
import multiprocessing as mp
from typing import List, Tuple
import time

def validate_date(date_str: str) -> bool:
    """Validate if the input string is a valid date in YYYY-MM-DD format."""
    try:
        datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def find_date_boundaries(mm: mmap.mmap, file_size: int) -> Tuple[str, str]:
    """Find the earliest and latest date in the file."""
    # Check first date (beginning of file)
    mm.seek(0)
    first_line = mm.readline().decode('utf-8', errors='ignore').strip()
    
    # Extract date from format like "2024-12-01T02:49:15.0000"
    first_date = extract_date_from_line(first_line)
    
    # Check last date (near end of file)
    pos = max(0, file_size - 100000)  # Go to near the end
    mm.seek(pos)
    mm.readline()  # Skip partial line
    
    last_date = "9999-99-99"
    for _ in range(100):  # Read a few lines to find a valid date
        line = mm.readline().decode('utf-8', errors='ignore').strip()
        if not line:
            break
        date = extract_date_from_line(line)
        if date:
            last_date = date
    
    return first_date, last_date

def extract_date_from_line(line: str) -> str:
    """Extract date part from log line."""
    if not line:
        return "0000-00-00"
        
    # Try to extract date from format like "2024-12-01T02:49:15.0000"
    if len(line) >= 10 and line[4] == '-' and line[7] == '-':
        if 'T' in line[:19]:
            return line.split('T')[0]
        else:
            return line[:10]
    return "0000-00-00"

def estimate_position(file_size: int, target_date: str, min_date: str, max_date: str) -> int:
    """Estimate position in file based on date range and file size."""
    try:
        target = datetime.datetime.strptime(target_date, '%Y-%m-%d')
        min_d = datetime.datetime.strptime(min_date, '%Y-%m-%d')
        max_d = datetime.datetime.strptime(max_date, '%Y-%m-%d')
        
        total_days = (max_d - min_d).days
        if total_days == 0:
            return 0
            
        target_days = (target - min_d).days
        ratio = target_days / total_days
        return int(file_size * ratio)
    except Exception as e:
        print(f"Error estimating position: {e}")
        return 0

def find_lines_for_date(chunk: Tuple[int, int], file_path: str, target_date: str) -> List[str]:
    """Process a chunk of the file to find lines matching the target date."""
    results = []
    
    with open(file_path, 'rb') as f:
        start, end = chunk
        f.seek(start)
        
        # If not at the beginning, read to the next line boundary
        if start > 0:
            f.readline()
        
        current_pos = f.tell()
        while current_pos < end:
            try:
                line = f.readline().decode('utf-8', errors='ignore').strip()
                current_pos = f.tell()
                
                # Check if line contains our target date
                if line and extract_date_from_line(line) == target_date:
                    # Format the line to match expected output
                    formatted_line = format_line_for_output(line)
                    results.append(formatted_line)
            except Exception:
                # Skip problematic lines
                current_pos = f.tell()
    
    return results

def format_line_for_output(line: str) -> str:
    """Format log line to match expected output format."""
    # Replace T with space and remove .0000 from timestamp
    if 'T' in line:
        parts = line.split(' - ', 2)
        if len(parts) >= 3:
            timestamp = parts[0].replace('T', ' ')
            if '.0000' in timestamp:
                timestamp = timestamp.replace('.0000', '')
            return f"{timestamp} {parts[1]} {parts[2]}"
    
    return line

def process_file(file_path: str, target_date: str, output_file: str):
    """Process the file and extract logs for the target date."""
    num_processes = max(1, mp.cpu_count() - 1)  # Leave one CPU free
    chunk_size = 100 * 1024 * 1024  # 100MB chunks
    
    try:
        file_size = os.path.getsize(file_path)
        chunks = []
        
        with open(file_path, 'rb') as f:
            # Memory map the file for efficient access
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            
            # Find date boundaries in file
            first_date, last_date = find_date_boundaries(mm, file_size)
            print(f"File contains logs from {first_date} to {last_date}")
            
            # Estimate position and create search chunks
            estimated_pos = estimate_position(file_size, target_date, first_date, last_date)
            
            # Create primary chunk centered around estimated position
            primary_chunk_size = min(chunk_size * 2, file_size // 2)
            primary_start = max(0, estimated_pos - primary_chunk_size // 2)
            primary_end = min(file_size, primary_start + primary_chunk_size)
            chunks.append((primary_start, primary_end))
            
            # Create additional chunks to cover the whole file
            # Before primary chunk
            start = 0
            while start < primary_start:
                end = min(start + chunk_size, primary_start)
                chunks.append((start, end))
                start = end
            
            # After primary chunk
            start = primary_end
            while start < file_size:
                end = min(start + chunk_size, file_size)
                chunks.append((start, end))
                start = end
                
            mm.close()
        
        # Process chunks in parallel
        print(f"Processing file in {len(chunks)} chunks using {num_processes} processes...")
        with mp.Pool(processes=num_processes) as pool:
            chunk_results = pool.starmap(
                find_lines_for_date,
                [(chunk, file_path, target_date) for chunk in chunks]
            )
        
        # Combine and write results
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            count = 0
            for result_set in chunk_results:
                for line in result_set:
                    f.write(line + '\n')
                    count += 1
                    
        print(f"Found {count} log entries for {target_date}")
        print(f"Results written to {output_file}")
        
    except Exception as e:
        print(f"Error processing file: {e}")
        return False
    
    return True

def main():
    """Main entry point for the log extraction script."""
    if len(sys.argv) != 2:
        print("Usage: python extract_logs.py YYYY-MM-DD")
        return 1
    
    target_date = sys.argv[1]
    
    if not validate_date(target_date):
        print(f"Error: '{target_date}' is not a valid date in YYYY-MM-DD format")
        return 1
    
    # Log file (not passed as argument)
    log_file = "logs_2024.log"
    if not os.path.exists(log_file):
        print(f"Error: Log file not found at '{log_file}'")
        return 1
    
    output_file = f"output/output_{target_date}.txt"
    
    # Measure time taken for processing
    start_time = time.time()
    
    # Process the file
    success = process_file(log_file, target_date, output_file)
    
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())