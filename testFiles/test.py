import subprocess

process = subprocess.Popen(["python3", "test2.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
print("Hi mom!")
for line in process.stdout:
    print(line, end='') 
for line in process.stderr:
    print(line, end='') 
