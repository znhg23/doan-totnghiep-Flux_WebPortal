import flux

handle = flux.Flux("local:///tmp/flux-7BFcQR/local-0")

flux.job.submit(handle, flux.job.JobspecV1.from_command(["echo", "test2"]))
