# Log Extraction Solution Discussion

## Solutions Considered

Using Python Instead of C++ as to support cross platform usage (primarily because use of mmap)

1. **Naive Line By Line Scan**
   - Reading the file line by line to find matching dates
   - Pros: Simple implementation
   - Cons: Extremely slow for a 1TB file, would scan the entire file
   - Single Threaded, takes about 20s for the example query on ssd

2. **Indexing Solution**
   - Building an index that maps dates to file positions
   - Pros: Very fast lookups after indexing
   - Cons: Initial indexing is slow and requires extra storage

3. **Binary Search with Memory Mapping**
   - Estimating log positions based on dates and file size
   - Pros: No preprocessing needed, efficient for chronological logs
   - Cons: Less accurate if logs aren't evenly distributed

4. **Chunk-based Parallel Processing**
   - Dividing the file into chunks and processing in parallel
   - Pros: Utilizes multiple cores, handles large files well
   - Cons: Adds complexity, requires chunk boundary handling

## Final Solution Summary

I implemented a hybrid approach combining memory mapping, binary search estimation, and parallel processing:

1. The solution uses memory mapping for efficient file access
2. It estimates where in the file the target date's logs might be found
3. It processes this high-probability region first, then covers the rest
4. All chunks are processed in parallel using Python's multiprocessing
5. Results are combined and written to the output file

This approach was chosen because it:
- Efficiently handles the 1TB file constraint 
- Minimizes memory usage by processing chunks
- Optimizes for speed through parallel processing (full cpu usage)
- Doesn't require preprocessing
- Works well with chronologically ordered logs

## Steps to Run

1. Ensure Python 3.6+ is installed
2. Place the log file in the same directory as the script (named `logs_2024.log` in this case)
3. Run: `python extract_logs.py YYYY-MM-DD`
4. Results will be saved to `output/output_YYYY-MM-DD.txt`
