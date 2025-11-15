import subprocess, sys, os, datetime

def save_snapshot(out_dir="logs"):
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"requirements_snapshot_{ts}.txt")
    result = subprocess.run([sys.executable,"-m","pip","freeze"], capture_output=True, text=True)
    with open(path,"w",encoding="utf-8") as f:
        f.write(result.stdout)
    print("Requirements snapshot saved:", path)

if __name__ == "__main__":
    save_snapshot()