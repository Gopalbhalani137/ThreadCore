# Multi-Threaded Print Job Manager

## Overview
The **Multi-Threaded Print Job Manager** is a Python-based simulation of a concurrent print spooling system. Utilizing the Producer-Consumer design pattern, it reads print jobs from a CSV file, categorizes them as "Normal" or "Heavy" based on user-defined duration thresholds, and delegates them to dedicated thread pools for simulated printing. 

## Features
* **Concurrency:** Implements multi-threading using Python's `threading` and `queue` modules.
* **Dynamic Routing:** Automatically categorizes tasks (Heavy vs. Normal) based on a custom threshold.
* **Thread-Safe Logging:** Utilizes `threading.Lock()` to ensure data integrity across multiple worker threads.
* **Poison Pill Termination:** Gracefully shuts down worker threads once the producer finishes reading jobs.
* **Analytics Generation:** Automatically generates CSV logs and TXT summaries detailing timestamps, durations, and total processing times for both printer types.

## Prerequisites
* Python 3.7 or higher (requires `dataclasses`)
* A valid input CSV file formatted with three columns: `customer_name`, `file_name`, `print_duration_seconds`.

## Usage
1. Ensure your input CSV file is in the same directory as the script. Example CSV format:
   ```csv
   customer_name,file_name,print_duration_seconds
   Alice,report.pdf,4
   Bob,thesis.docx,15
   Charlie,image.png,2



3. Follow the CLI prompts to configure the system:
   * Provide the name of the input CSV.
   * Set the threshold (in seconds) that separates Normal jobs from Heavy jobs.
   * Define the number of Normal and Heavy printer threads to spawn.

## Outputs
Upon completion, the script generates four files in the root directory:
* `Normal.csv` / `Heavy.csv`: Detailed logs of each printed file, including exact start/end timestamps and the specific thread used.
* `Normal_summary.txt` / `Heavy_summary.txt`: High-level metrics showing total tasks processed and total time taken.

## Future Enhancements
* **Priority Queuing:** Allow VIP customers or urgent files to bypass the standard queue using `queue.PriorityQueue`.
* **Database Integration:** Replace CSV logging with SQLite or PostgreSQL for persistent, queryable data storage.
* **Real-Time Visuals:** Implement CLI progress bars or a graphical user interface (GUI) to monitor queue sizes and printer status in real-time.
* **Pause/Resume Functionality:** Add the ability to pause specific printer threads for "maintenance" without losing queued jobs.
