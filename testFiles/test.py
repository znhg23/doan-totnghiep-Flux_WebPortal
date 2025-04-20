import flux
import flux.job
import os


handle = flux.Flux("local:///tmp/flux-0vHmCv/local-0")
jobspec = flux.job.JobspecV1.from_command(["python3", "./thread_hello.py"],4)
#%%
# This is how we set the "current working directory" (cwd) for the job
jobspec.cwd = os.getcwd()

#%%
# This is how we set the job environment
jobspec.environment = dict(os.environ)

jobID = flux.job.submit(handle, jobspec)
print(jobID)