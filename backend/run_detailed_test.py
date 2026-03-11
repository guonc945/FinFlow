import subprocess
import os

def run_test():
    print("Running auto sync test...")
    # Use -u for unbuffered output
    process = subprocess.Popen(
        ['python', '-u', 'test_auto_sync.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=os.getcwd()
    )
    
    with open('detailed_test_output.log', 'w', encoding='utf-8') as f:
        while True:
            line = process.stdout.readline()
            if not line:
                break
            decoded_line = line.decode('utf-8', errors='ignore')
            print(decoded_line, end='')
            f.write(decoded_line)
    
    process.wait()
    print(f"\nTest finished with code {process.returncode}")

if __name__ == "__main__":
    run_test()
