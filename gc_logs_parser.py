import re

def analyze_gc_log(log_path):
    total_gc_time = 0
    gc_events = []
    
    with open(log_path, 'r') as f:
        for line in f:
            # Parse GC pause times
            match = re.search(r'Pause.*?(\d+\.\d+)ms', line)
            if match:
                pause_time = float(match.group(1))
                gc_events.append(pause_time)
                total_gc_time += pause_time
    
    if gc_events:
        print(f"Total GC Time: {total_gc_time:.2f}ms")
        print(f"GC Events: {len(gc_events)}")
        print(f"Avg GC Pause: {sum(gc_events)/len(gc_events):.2f}ms")
        print(f"Max GC Pause: {max(gc_events):.2f}ms")
    
    return gc_events


analyze_gc_log("./gc-logs/driver-gc.log.0")

