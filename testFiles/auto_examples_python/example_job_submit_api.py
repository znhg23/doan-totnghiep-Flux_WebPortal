import flux
import flux.job


jobId = None
def submit_cb(fut, flux_handle):
        # when this callback fires, the jobid will be ready
        jobId = fut.get_id()
        # Create a future representing the result of the job
        result_fut = flux.job.result_async(flux_handle, jobId)
        # attach a callback to fire when the job finishes
        result_fut.then(result_cb)

def result_cb(fut):
        job = fut.get_info()
        result = job.result.lower()
        print(f"{job.id}: {result} with returncode {job.returncode}")

flux_handle = flux.Flux("local:///tmp/flux-zMx5pi/local-0")
jobspec = flux.job.JobspecV1.from_command(["/bin/true"])

submit_future = flux.job.submit_async(flux_handle, jobspec)
submit_future.then(submit_cb, flux_handle)
# enter the flux event loop (the 'reactor') to trigger the callbacks
# once the futures complete
flux_handle.reactor_run()

print(jobId)