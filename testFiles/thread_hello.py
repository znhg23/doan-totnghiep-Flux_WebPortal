import threading
import platform
import socket

# Get the current thread ID
current_thread_id = threading.get_ident()

# Get the processor name (hostname)
processor_name = socket.gethostname()

# Print the message with the current thread ID and processor name
print(f"Hello world, from thread {current_thread_id} on processor {processor_name}") 